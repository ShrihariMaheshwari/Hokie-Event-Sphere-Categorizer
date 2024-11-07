from apscheduler.schedulers.asyncio import AsyncIOScheduler
import pytz
import aiohttp
import asyncio
import os
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import motor.motor_asyncio

class TicketmasterSync:
    def __init__(self):
        self.mongo_client = motor.motor_asyncio.AsyncIOMotorClient(os.getenv("MONGO_URI"))
        self.db = self.mongo_client.events_db
        self.ticketmaster_key = os.getenv("TICKETMASTER_API_KEY")
        self.self_url = os.getenv("SELF_URL")

    async def fetch_events(self):
        """Fetch events from Ticketmaster API"""
        if not self.ticketmaster_key:
            print("ERROR: Ticketmaster API key not found!")
            return []

        base_url = "https://app.ticketmaster.com/discovery/v2/events.json"
        start_date = datetime.now()
        end_date = start_date + timedelta(days=30)
        
        params = {
            'apikey': self.ticketmaster_key,
            'stateCode': 'VA',
            'startDateTime': start_date.strftime('%Y-%m-%dT%H:%M:%SZ'),
            'endDateTime': end_date.strftime('%Y-%m-%dT%H:%M:%SZ'),
            'size': 200,
            'sort': 'date,asc'
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(base_url, params=params) as response:
                    print(f"Ticketmaster API Response Status: {response.status}")
                    
                    if response.status == 200:
                        data = await response.json()
                        events = data.get('_embedded', {}).get('events', [])
                        print(f"Found {len(events)} events in Ticketmaster response")
                        return events
                    else:
                        error_text = await response.text()
                        print(f"Ticketmaster API error: {error_text}")
                        return []
                        
        except Exception as e:
            print(f"Error fetching Ticketmaster events: {e}")
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
                    async with aiohttp.ClientSession() as session:
                        async with session.post(
                            f"{self.self_url}/categorize/ticketmaster",
                            json=event
                        ) as response:
                            if response.status == 200:
                                successful_syncs += 1
                                print(f"Successfully processed event: {event.get('name')}")
                            else:
                                failed_syncs += 1
                                error_text = await response.text()
                                print(f"Failed to process event {event.get('name')}: {error_text}")
                                
                except Exception as e:
                    failed_syncs += 1
                    print(f"Error processing event: {e}")
                    continue

            print(f"""
            Ticketmaster sync completed:
            - Total events processed: {len(events)}
            - Successfully synced: {successful_syncs}
            - Failed to sync: {failed_syncs}
            """)
            
        except Exception as e:
            print(f"Error in Ticketmaster sync: {e}")

async def start_scheduler():
    """Initialize and start the Ticketmaster sync scheduler"""
    sync_service = TicketmasterSync()
    
    # Run initial sync
    await sync_service.sync()

    # Setup scheduler
    scheduler = AsyncIOScheduler()
    scheduler.add_job(sync_service.sync, 'interval', hours=12, timezone=pytz.UTC)
    scheduler.start()
    print("Ticketmaster sync scheduler started...")
    
    try:
        while True:
            await asyncio.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()