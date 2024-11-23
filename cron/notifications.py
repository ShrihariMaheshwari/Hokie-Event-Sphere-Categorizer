from apscheduler.schedulers.asyncio import AsyncIOScheduler
import pytz
import aiohttp
import asyncio
import os
from datetime import datetime, timedelta
from typing import Dict, Any, List
import motor.motor_asyncio
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib


class EmailReminderJob:
    def __init__(self):
        self.mongo_client = motor.motor_asyncio.AsyncIOMotorClient(
            os.getenv("MONGO_URI"),
            serverSelectionTimeoutMS=5000
        )
        self.db = self.mongo_client.events_db
        self.smtp_host = os.getenv("SMTP_HOST")
        self.smtp_port = int(os.getenv("SMTP_PORT", 587))
        self.smtp_user = os.getenv("SMTP_USER")
        self.smtp_password = os.getenv("SMTP_PASSWORD")
        self.scheduler = None
        self.is_running = False

    async def fetch_events_starting_soon(self) -> List[Dict[str, Any]]:
        """Fetch events starting in less than 2 days."""
        now = datetime.utcnow()
        two_days_later = now + timedelta(days=2)

        events = await self.db.events.find({
            "startDate": {
                "$gte": now,
                "$lt": two_days_later
            }
        }).to_list(length=1000)

        return events

    async def send_email(self, to_email: str, subject: str, content: str) -> bool:
        """Send email using SMTP."""
        try:
            msg = MIMEMultipart()
            msg["From"] = self.smtp_user
            msg["To"] = to_email
            msg["Subject"] = subject

            msg.attach(MIMEText(content, "html"))

            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                server.starttls()
                server.login(self.smtp_user, self.smtp_password)
                server.send_message(msg)

            print(f"[{datetime.now()}] Email sent to {to_email}")
            return True
        except Exception as e:
            print(f"[{datetime.now()}] Error sending email to {to_email}: {str(e)}")
            return False

    async def process_event(self, event: Dict[str, Any]) -> None:
        """Process a single event, sending reminders to RSVP'd users."""
        title = event.get("title")
        venue = event.get("venue")
        start_date = event.get("startDate")
        rsvps = event.get("rsvps", [])

        for rsvp in rsvps:
            email = rsvp.get("email")
            name = rsvp.get("name")

            if not email:
                print(f"[{datetime.now()}] Skipping RSVP without email for event {title}")
                continue

            email_content = f"""
                <p>Dear {name},</p>
                <p>This is a reminder about your RSVP for the event:</p>
                <p><strong>{title}</strong></p>
                <p>Venue: {venue}</p>
                <p>Start Date: {datetime.fromisoformat(start_date).strftime('%Y-%m-%d %H:%M:%S')}</p>
                <p>We look forward to seeing you there!</p>
                <p>Best Regards,<br>The Events Team</p>
            """

            await self.send_email(email, f"Reminder: Upcoming Event '{title}'", email_content)

    async def send_reminders(self) -> None:
        """Fetch events and send reminders."""
        if self.is_running:
            print(f"[{datetime.now()}] Reminder job is already running, skipping...")
            return

        try:
            self.is_running = True
            print(f"[{datetime.now()}] Starting reminder job...")

            events = await self.fetch_events_starting_soon()
            if not events:
                print(f"[{datetime.now()}] No events found starting in less than 2 days.")
                return

            print(f"[{datetime.now()}] Found {len(events)} event(s) starting soon.")

            for event in events:
                await self.process_event(event)

        except Exception as e:
            print(f"[{datetime.now()}] Error in reminder job: {str(e)}")
        finally:
            self.is_running = False

    def start_scheduler(self) -> None:
        """Start the scheduler."""
        if not self.scheduler:
            self.scheduler = AsyncIOScheduler()
            self.scheduler.add_job(
                self.send_reminders,
                "interval",
                hours=24,
                timezone=pytz.UTC,
                max_instances=1,
                coalesce=True
            )
            self.scheduler.start()
            print(f"[{datetime.now()}] Email reminder scheduler initialized")


async def start_reminder_scheduler():
    """Initialize and start the email reminder scheduler."""
    reminder_service = EmailReminderJob()

    # Start the scheduler
    reminder_service.start_scheduler()

    # Run the initial reminder job
    print(f"[{datetime.now()}] Running initial reminder job")
    await reminder_service.send_reminders()

    return reminder_service


# Run the script directly for testing
if __name__ == "__main__":
    asyncio.run(start_reminder_scheduler())
