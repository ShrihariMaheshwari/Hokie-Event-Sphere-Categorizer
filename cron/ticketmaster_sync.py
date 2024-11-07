from apscheduler.schedulers.asyncio import AsyncIOScheduler
import pytz
import aiohttp
import asyncio
import os
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import motor.motor_asyncio
from bson import ObjectId

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
            'city': 'Washinton',
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

    async def sync(self):
        """Main sync function"""
        try:
            print(f"Starting Ticketmaster sync at {datetime.now()}")
            
            events = await self.fetch_events()
            print(f"Fetched {len(events)} events from Ticketmaster")

            successful_syncs = 0
            failed_syncs = 0

            for event in events:
                try:
                    # Make request to categorize endpoint
                    async with aiohttp.ClientSession() as session:
                        async with session.post(
                            f"{os.getenv('SELF_URL')}/categorize/ticketmaster",
                            json=event
                        ) as response:
                            if response.status == 200:
                                successful_syncs += 1
                            else:
                                failed_syncs += 1
                                print(f"Failed to sync event {event.get('name')}: {await response.text()}")
                                
                except Exception as e:
                    failed_syncs += 1
                    print(f"Error processing individual event: {e}")
                    continue

            print(f"""
            Ticketmaster sync completed:
            - Total events processed: {len(events)}
            - Successfully synced: {successful_syncs}
            - Failed to sync: {failed_syncs}
            """)
        except Exception as e:
            print(f"Error in sync: {e}")

async def start_scheduler():
    sync_service = TicketmasterSync()
    
    # Run sync immediately on startup
    await sync_service.sync()

    # Set up the scheduler for repeated syncing
    scheduler = AsyncIOScheduler()
    timezone = pytz.timezone('UTC')
    
    # Schedule the job to run every 12 hours
    scheduler.add_job(sync_service.sync, 'interval', hours=12, timezone=timezone)
    
    scheduler.start()
    print("Ticketmaster sync scheduler started...")
    
    try:
        while True:
            await asyncio.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        await sync_service.mongo_client.close()  # Close MongoDB client on shutdown
