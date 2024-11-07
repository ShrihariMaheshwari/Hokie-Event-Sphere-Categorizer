from apscheduler.schedulers.asyncio import AsyncIOScheduler
import pytz
import aiohttp
import asyncio
import os
from datetime import datetime, timedelta
import motor.motor_asyncio

class TicketmasterSync:
    def __init__(self):
        self.mongo_client = motor.motor_asyncio.AsyncIOMotorClient(os.getenv("MONGO_URI"))
        self.db = self.mongo_client.events_db
        self.ticketmaster_key = os.getenv("TICKETMASTER_API_KEY")

    async def fetch_events(self):
        """Fetch events from Ticketmaster API"""
        base_url = "https://app.ticketmaster.com/discovery/v2/events.json"
        
        start_date = datetime.now()
        end_date = start_date + timedelta(days=30)
        
        params = {
            'apikey': self.ticketmaster_key,
            'city': 'Blacksburg',
            'stateCode': 'VA',
            'startDateTime': start_date.strftime('%Y-%m-%dT%H:%M:%SZ'),
            'endDateTime': end_date.strftime('%Y-%m-%dT%H:%M:%SZ'),
            'size': 200
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(base_url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data.get('_embedded', {}).get('events', [])
                    return []
        except Exception as e:
            print(f"Error fetching events: {e}")
            return []

    async def process_event(self, event):
        """Process and categorize individual Ticketmaster event"""
        try:
            event_data = {
                'title': event['name'],
                'description': event.get('description', ''),
                'venue': event['_embedded']['venues'][0]['name'],
                'startDate': event['dates']['start']['localDate'],
                'startTime': event['dates']['start'].get('localTime', '19:00:00'),
                'endTime': event['dates']['start'].get('localTime', '22:00:00'),
                'source': 'ticketmaster',
                'imageUrl': event['images'][0]['url'] if event.get('images') else None,
                'registrationFee': event['priceRanges'][0]['min'] if event.get('priceRanges') else 0
            }

            # Make request to categorize endpoint
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{os.getenv('SELF_URL', 'http://localhost:8000')}/categorize",
                    json=event_data
                ) as response:
                    if response.status == 200:
                        categorized_event = await response.json()
                        return {
                            **categorized_event,
                            'ticketmaster_id': event['id']
                        }
            return None
        except Exception as e:
            print(f"Error processing event: {e}")
            return None

    async def sync(self):
        """Main sync function"""
        try:
            print(f"Starting Ticketmaster sync at {datetime.now()}")
            
            events = await self.fetch_events()
            print(f"Fetched {len(events)} events from Ticketmaster")

            for event in events:
                try:
                    processed_event = await self.process_event(event)
                    if processed_event:
                        # Upsert to MongoDB
                        await self.db.events.update_one(
                            {'ticketmaster_id': processed_event['ticketmaster_id']},
                            {'$set': processed_event},
                            upsert=True
                        )
                except Exception as e:
                    print(f"Error processing individual event: {e}")
                    continue

            print("Ticketmaster sync completed")
        except Exception as e:
            print(f"Error in sync: {e}")

async def start_scheduler():
    sync_service = TicketmasterSync()
    scheduler = AsyncIOScheduler()

    timezone = pytz.timezone('UTC')
    
    # Schedule job to run every 12 hours
    scheduler.add_job(sync_service.sync, 'interval', hours=12, timezone=timezone)
    # Also run immediately on startup
    scheduler.add_job(sync_service.sync, 'date', run_date=datetime.now())
    
    scheduler.start()
    print("Ticketmaster sync scheduler started...")
    
    try:
        while True:
            await asyncio.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()

if __name__ == "__main__":
    asyncio.run(start_scheduler())