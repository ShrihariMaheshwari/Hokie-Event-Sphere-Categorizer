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
        self.mongo_client = motor.motor_asyncio.AsyncIOMotorClient(
            os.getenv("MONGO_URI"),
            serverSelectionTimeoutMS=5000  # 5 second timeout for MongoDB operations
        )
        self.db = self.mongo_client.events_db
        self.ticketmaster_key = os.getenv("TICKETMASTER_API_KEY")
        self.self_url = os.getenv("SELF_URL")
        self.timeout = aiohttp.ClientTimeout(total=30)  # 30 seconds timeout

    async def fetch_events(self) -> List[Dict[str, Any]]:
        """Fetch events from Ticketmaster API with improved error handling"""
        if not self.ticketmaster_key:
            print(f"[{datetime.now()}] ERROR: Ticketmaster API key not found!")
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
            async with aiohttp.ClientSession(timeout=self.timeout) as session:
                async with session.get(base_url, params=params) as response:
                    current_time = datetime.now()
                    print(f"[{current_time}] Ticketmaster API Response Status: {response.status}")
                    
                    if response.status == 200:
                        data = await response.json()
                        events = data.get('_embedded', {}).get('events', [])
                        print(f"[{current_time}] Found {len(events)} events in Ticketmaster response")
                        return events
                    else:
                        error_text = await response.text()
                        print(f"[{current_time}] Ticketmaster API error: {error_text}")
                        return []
                        
        except asyncio.TimeoutError:
            print(f"[{datetime.now()}] Timeout while fetching Ticketmaster events")
            return []
        except Exception as e:
            print(f"[{datetime.now()}] Error fetching Ticketmaster events: {str(e)}")
            return []

    async def process_single_event(self, event: Dict[str, Any]) -> bool:
        """Process a single event with timeout and error handling"""
        try:
            async with aiohttp.ClientSession(timeout=self.timeout) as session:
                async with session.post(
                    f"{self.self_url}/categorize/ticketmaster",
                    json=event
                ) as response:
                    current_time = datetime.now()
                    if response.status == 200:
                        result = await response.json()
                        if result is None:  # Event was a duplicate
                            print(f"[{current_time}] Skipped duplicate event: {event.get('name')}")
                            return True
                        print(f"[{current_time}] Successfully processed event: {event.get('name')}")
                        return True
                    else:
                        error_text = await response.text()
                        print(f"[{current_time}] Failed to process event {event.get('name')}: {error_text}")
                        return False
                        
        except asyncio.TimeoutError:
            print(f"[{datetime.now()}] Timeout processing event: {event.get('name')}")
            return False
        except Exception as e:
            print(f"[{datetime.now()}] Error processing event {event.get('name')}: {str(e)}")
            return False

    async def sync(self):
        """Main sync function with improved error handling and batch processing"""
        sync_start_time = datetime.now()
        print(f"[{sync_start_time}] Starting Ticketmaster sync")
        
        try:
            events = await self.fetch_events()
            if not events:
                print(f"[{datetime.now()}] No events to process")
                return

            print(f"[{datetime.now()}] Starting to process {len(events)} events")
            
            successful_syncs = 0
            failed_syncs = 0
            skipped_duplicates = 0

            # Process events in smaller batches
            batch_size = 10
            for i in range(0, len(events), batch_size):
                batch = events[i:i + batch_size]
                batch_start_time = datetime.now()
                print(f"[{batch_start_time}] Processing batch {i//batch_size + 1} of {(len(events) + batch_size - 1)//batch_size}")
                
                # Process batch of events concurrently
                tasks = [self.process_single_event(event) for event in batch]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                for result in results:
                    if isinstance(result, bool):
                        if result:
                            successful_syncs += 1
                        else:
                            failed_syncs += 1
                    else:  # Exception occurred
                        failed_syncs += 1
                        print(f"[{datetime.now()}] Error in batch processing: {str(result)}")
                
                # Add a small delay between batches to prevent overwhelming the system
                await asyncio.sleep(1)
                
                batch_end_time = datetime.now()
                batch_duration = (batch_end_time - batch_start_time).total_seconds()
                print(f"[{batch_end_time}] Batch completed in {batch_duration:.2f} seconds")

            sync_end_time = datetime.now()
            sync_duration = (sync_end_time - sync_start_time).total_seconds()
            
            print(f"""
            [{sync_end_time}] Ticketmaster sync completed:
            - Duration: {sync_duration:.2f} seconds
            - Total events processed: {len(events)}
            - Successfully synced: {successful_syncs}
            - Failed to sync: {failed_syncs}
            - Batches processed: {(len(events) + batch_size - 1)//batch_size}
            """)
            
        except Exception as e:
            print(f"[{datetime.now()}] Error in Ticketmaster sync: {str(e)}")

async def start_scheduler():
    """Initialize and start the Ticketmaster sync scheduler with error handling"""
    try:
        sync_service = TicketmasterSync()
        
        # Run initial sync
        print(f"[{datetime.now()}] Running initial sync")
        await sync_service.sync()

        # Setup scheduler
        scheduler = AsyncIOScheduler()
        scheduler.add_job(
            sync_service.sync,
            'interval',
            hours=12,
            timezone=pytz.UTC,
            max_instances=1,  # Prevent multiple instances running simultaneously
            coalesce=True     # Combine multiple missed runs
        )
        
        scheduler.start()
        print(f"[{datetime.now()}] Ticketmaster sync scheduler started successfully")
        
        try:
            while True:
                await asyncio.sleep(3600)  # Check every hour
        except (KeyboardInterrupt, SystemExit):
            print(f"[{datetime.now()}] Shutting down scheduler")
            scheduler.shutdown()
            
    except Exception as e:
        print(f"[{datetime.now()}] Scheduler error: {str(e)}")
        raise