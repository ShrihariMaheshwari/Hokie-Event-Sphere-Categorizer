from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import openai
import os
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import motor.motor_asyncio
import asyncio
from bson import ObjectId
from cron.ticketmaster_sync import start_scheduler
import json

app = FastAPI(title="Hokie Event Categorizer")

# CORS setup
app.add_middleware(
    CORSMiddleware,
    allow_origins=[os.getenv("EXPRESS_BACKEND_URL", "*")],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# MongoDB setup
mongo_client = motor.motor_asyncio.AsyncIOMotorClient(os.getenv("MONGO_URI"))
db = mongo_client.events_db

# OpenAI setup
openai.api_key = os.getenv("OPENAI_API_KEY")

def format_datetime(dt):
    """Format datetime object to string"""
    if isinstance(dt, datetime):
        return dt.isoformat()
    return str(dt)

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import openai
import os
from typing import Dict, Any
from datetime import datetime, timedelta
import motor.motor_asyncio
import asyncio
from bson import ObjectId
from cron.ticketmaster_sync import start_scheduler
import json

app = FastAPI(title="Hokie Event Categorizer")

# CORS and DB setup
app.add_middleware(
    CORSMiddleware,
    allow_origins=[os.getenv("EXPRESS_BACKEND_URL", "*")],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

mongo_client = motor.motor_asyncio.AsyncIOMotorClient(os.getenv("MONGO_URI"))
db = mongo_client.events_db

# OpenAI client setup (new way)
client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

async def categorize_with_gpt(event_data: Dict[str, Any]):
    """Categorize event using GPT-4"""
    try:
        event_info = (
            f"Event: {event_data['title']}, "
            f"Venue: {event_data['venue']}, "
            f"Description: {event_data.get('description', 'No description available')}"
        )

        prompt = f"""
        Categorize this event and improve its description if needed.
        
        Event Info: {event_info}
        
        Categories:
        - Sports: Live Sports Events, Amateur Sports Events, Sports Meetups
        - Movies: General Movies, Sports Movies, Documentaries
        - Tech Events: Tech Conferences, Hackathons, Tech Meetups
        - Social Events: Social Gatherings, Cultural Events
        - Others: Miscellaneous Events
        
        Return in this format:
        {{
            "main_category": "category name",
            "sub_category": "subcategory name",
            "description": "improved or existing description"
        }}
        """

        # Using the new OpenAI client format
        response = await asyncio.to_thread(
            client.chat.completions.create,
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are an event categorizer."},
                {"role": "user", "content": prompt}
            ]
        )

        # Extract the response
        if response.choices and response.choices[0].message:
            try:
                result = json.loads(response.choices[0].message.content)
                return {
                    "main_category": result.get("main_category", "Others"),
                    "sub_category": result.get("sub_category", "Miscellaneous Events"),
                    "description": result.get("description", event_data.get("description", ""))
                }
            except:
                return {
                    "main_category": "Others",
                    "sub_category": "Miscellaneous Events",
                    "description": event_data.get("description", "")
                }
                
    except Exception as e:
        print(f"Error in categorization: {e}")
        return {
            "main_category": "Others",
            "sub_category": "Miscellaneous Events",
            "description": event_data.get("description", "")
        }

async def process_ticketmaster_event(event: dict):
    """Process Ticketmaster event to match MongoDB schema"""
    try:
        # Basic event data
        event_data = {
            'title': event.get('name', ''),
            'venue': event.get('_embedded', {}).get('venues', [{}])[0].get('name', 'Venue Not Specified'),
            'organizerId': str(ObjectId()),  # Generate new ObjectId for organizerId
            'organizerEmail': 'events@ticketmaster.com',
            'description': event.get('description', ''),
            'imageUrl': event.get('images', [{'url': None}])[0].get('url', None),
            'rsvps': []
        }

        # Handle dates
        try:
            event_data['startDate'] = datetime.fromisoformat(event['dates']['start']['localDate'])
            event_data['endDate'] = datetime.fromisoformat(
                event['dates'].get('end', {}).get('localDate', event['dates']['start']['localDate'])
            )
            event_data['startTime'] = event['dates']['start'].get('localTime', '19:00:00')
            
            # Calculate end time
            if not event['dates'].get('end', {}).get('localTime'):
                end_datetime = datetime.strptime(event_data['startTime'], '%H:%M:%S') + timedelta(hours=3)
                event_data['endTime'] = end_datetime.strftime('%H:%M:%S')
            else:
                event_data['endTime'] = event['dates']['end']['localTime']
        except Exception as date_error:
            print(f"Date error for {event_data['title']}: {date_error}")
            current_time = datetime.now()
            event_data.update({
                'startDate': current_time,
                'endDate': current_time + timedelta(hours=3),
                'startTime': '19:00:00',
                'endTime': '22:00:00'
            })

        # Handle registration fee
        try:
            if event.get('priceRanges'):
                event_data['registrationFee'] = min(price['min'] for price in event['priceRanges'])
            else:
                event_data['registrationFee'] = 0
        except:
            event_data['registrationFee'] = 0

        # Get category and enhanced description
        enhanced_data = await categorize_with_gpt(event_data)
        event_data.update(enhanced_data)

        return event_data

    except Exception as e:
        print(f"Error processing event {event.get('name', 'Unknown')}: {e}")
        return None

@app.post("/categorize/ticketmaster")
async def categorize_ticketmaster_event(event_data: Dict[str, Any]):
    """Endpoint for categorizing Ticketmaster events"""
    try:
        processed_event = await process_ticketmaster_event(event_data)
        
        if not processed_event:
            return None

        # Add timestamps
        current_time = datetime.utcnow()
        processed_event['createdAt'] = current_time
        processed_event['updatedAt'] = current_time

        # Save to database
        try:
            result = await db.events.insert_one(processed_event)
            processed_event['_id'] = str(result.inserted_id)
            print(f"Successfully saved: {processed_event['title']}")
            return processed_event
        except Exception as db_error:
            print(f"Database error: {db_error}")
            return None

    except Exception as e:
        print(f"Processing error: {e}")
        return None


@app.post("/categorize/{event_id}")
async def categorize_manual_event(event_id: str):
    """Endpoint for categorizing manually created events"""
    try:
        obj_id = ObjectId(event_id)
        event = await db.events.find_one({"_id": obj_id})
        
        if not event:
            return {"error": "Event not found"}

        # Get categories from GPT
        enhanced_data = await categorize_with_gpt(event)
        
        if enhanced_data:
            # Update the event with new categories and description
            update_result = await db.events.update_one(
                {"_id": obj_id},
                {
                    "$set": {
                        "main_category": enhanced_data["main_category"],
                        "sub_category": enhanced_data["sub_category"],
                        "description": enhanced_data["description"],
                        "updatedAt": datetime.utcnow()
                    }
                }
            )
            
            if update_result.modified_count > 0:
                return {"success": True, "data": enhanced_data}
        
        return {"success": False, "error": "Failed to update event"}

    except Exception as e:
        print(f"Error: {str(e)}")
        return {"success": False, "error": str(e)}

@app.get("/")
async def root():
    return {
        "message": "Welcome to Hokie Event Categorizer API",
        "endpoints": {
            "/categorize/{event_id}": "Categorize existing events",
            "/categorize/ticketmaster": "Process and categorize Ticketmaster events",
            "/health": "Health check endpoint"
        }
    }

@app.get("/health")
async def health_check():
    try:
        await db.command('ping')
        return {
            "status": "healthy",
            "mongo": "connected",
            "timestamp": datetime.utcnow()
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.utcnow()
        }

@app.on_event("startup")
async def startup_event():
    """Startup event handler"""
    print("\nStarting FastAPI service...")
    
    # Check environment variables
    required_vars = {
        "MONGO_URI": os.getenv("MONGO_URI"),
        "OPENAI_API_KEY": os.getenv("OPENAI_API_KEY"),
        "TICKETMASTER_API_KEY": os.getenv("TICKETMASTER_API_KEY"),
    }

    missing_vars = [var for var, value in required_vars.items() if not value]
    if missing_vars:
        print(f"Missing required environment variables: {', '.join(missing_vars)}")
        return

    print("✓ Environment variables verified")

    try:
        await db.command('ping')
        print("✓ MongoDB connected")
    except Exception as e:
        print(f"× MongoDB connection failed: {e}")

    try:
        asyncio.create_task(start_scheduler())
        print("✓ Ticketmaster sync scheduler started")
    except Exception as e:
        print(f"× Scheduler start failed: {e}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))