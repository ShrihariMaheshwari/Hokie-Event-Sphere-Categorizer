from apscheduler.schedulers.asyncio import AsyncIOScheduler
import pytz
import aiohttp
import asyncio
import os
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import motor.motor_asyncio
import sys

class TicketmasterSync:
    def __init__(self):
        self.mongo_client = motor.motor_asyncio.AsyncIOMotorClient(
            os.getenv("MONGO_URI"),
            serverSelectionTimeoutMS=5000
        )
        self.db = self.mongo_client.events_db
        self.ticketmaster_key = os.getenv("TICKETMASTER_API_KEY")
        self.self_url = os.getenv("SELF_URL")
        self.timeout = aiohttp.ClientTimeout(total=30)
        self.is_syncing = False

    async def fetch_events(self) -> List[Dict[str, Any]]:
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
                    print(f"[{datetime.now()}] Ticketmaster API Response Status: {response.status}")
                    
                    if response.status == 200:
                        data = await response.json()
                        events = data.get('_embedded', {}).get('events', [])
                        print(f"[{datetime.now()}] Found {len(events)} events in Ticketmaster response")
                        return events
                    else:
                        error_text = await response.text()
                        print(f"[{datetime.now()}] Ticketmaster API error: {error_text}")
                        return []
                        
        except Exception as e:
            print(f"[{datetime.now()}] Error fetching Ticketmaster events: {str(e)}")
            return []

    async def is_event_exists(self, event: Dict[str, Any]) -> bool:
        try:
            event_id = event.get('id')
            if event_id:
                existing = await self.db.events.find_one({"ticketmaster_id": event_id})
                if existing:
                    return True

            name = event.get('name')
            start_date = event.get('dates', {}).get('start', {}).get('localDate')
            if name and start_date:
                existing = await self.db.events.find_one({
                    "title": name,
                    "startDate": datetime.fromisoformat(start_date)
                })
                if existing:
                    return True

            return False
        except Exception as e:
            print(f"[{datetime.now()}] Error checking event existence: {str(e)}")
            return False

    async def process_single_event(self, event: Dict[str, Any]) -> bool:
        try:
            if await self.is_event_exists(event):
                print(f"[{datetime.now()}] Event already exists, skipping: {event.get('name')}")
                return True

            async with aiohttp.ClientSession(timeout=self.timeout) as session:
                async with session.post(
                    f"{self.self_url}/categorize/ticketmaster",
                    json=event
                ) as response:
                    if response.status == 200:
                        print(f"[{datetime.now()}] Successfully processed new event: {event.get('name')}")
                        return True
                    else:
                        error_text = await response.text()
                        print(f"[{datetime.now()}] Failed to process event {event.get('name')}: {error_text}")
                        return False
                        
        except Exception as e:
            print(f"[{datetime.now()}] Error processing event {event.get('name')}: {str(e)}")
            return False

    async def sync(self):
        if self.is_syncing:
            print(f"[{datetime.now()}] Sync already in progress, skipping...")
            return

        try:
            self.is_syncing = True
            sync_start_time = datetime.now()
            print(f"[{sync_start_time}] Starting Ticketmaster sync")
            
            events = await self.fetch_events()
            if not events:
                print(f"[{datetime.now()}] No events to process")
                return

            print(f"[{datetime.now()}] Starting to process {len(events)} events")
            
            successful_syncs = 0
            failed_syncs = 0
            skipped_events = 0

            batch_size = 10
            for i in range(0, len(events), batch_size):
                batch = events[i:i + batch_size]
                tasks = [self.process_single_event(event) for event in batch]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                for result in results:
                    if isinstance(result, bool):
                        if result:
                            successful_syncs += 1
                        else:
                            failed_syncs += 1
                    else:
                        failed_syncs += 1
                
                await asyncio.sleep(1)

            sync_end_time = datetime.now()
            sync_duration = (sync_end_time - sync_start_time).total_seconds()
            
            print(f"""
[{sync_end_time}] Ticketmaster sync completed:
- Duration: {sync_duration:.2f} seconds
- Total events checked: {len(events)}
- Successfully processed: {successful_syncs}
- Failed to process: {failed_syncs}
- Batches processed: {(len(events) + batch_size - 1)//batch_size}
            """)
            
        except Exception as e:
            print(f"[{datetime.now()}] Error in Ticketmaster sync: {str(e)}")
        finally:
            self.is_syncing = False

class SchedulerService:
    def __init__(self):
        self.scheduler = AsyncIOScheduler()
        self.sync_service = TicketmasterSync()

    async def run_sync(self):
        """Run a single sync and exit"""
        try:
            await self.sync_service.sync()
            print(f"[{datetime.now()}] Initial sync completed. Service will restart in 12 hours.")
            # Exit the process after completion
            sys.exit(0)
        except Exception as e:
            print(f"[{datetime.now()}] Error in sync: {str(e)}")
            sys.exit(1)

    async def start_scheduled_sync(self):
        """Start the scheduler for periodic syncs"""
        try:
            # Schedule the job to run every 12 hours
            self.scheduler.add_job(
                self.sync_service.sync,
                'interval',
                hours=12,
                timezone=pytz.UTC,
                max_instances=1,
                coalesce=True
            )
            
            self.scheduler.start()
            print(f"[{datetime.now()}] Scheduler started. Will sync every 12 hours.")
            
            # Run the first sync immediately
            await self.run_sync()
            
        except Exception as e:
            print(f"[{datetime.now()}] Scheduler error: {str(e)}")
            sys.exit(1)

async def start_scheduler():
    """Initialize and start the scheduler"""
    scheduler_service = SchedulerService()
    await scheduler_service.start_scheduled_sync()

if __name__ == "__main__":
    try:
        asyncio.run(start_scheduler())
    except KeyboardInterrupt:
        print(f"[{datetime.now()}] Service stopped by user")
        sys.exit(0)