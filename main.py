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

async def categorize_with_gpt(event_data: Dict[str, Any]):
    """Categorize event using GPT-3.5 with comprehensive prompt"""
    try:
        # Format the event info string
        event_info = (
            f"Event: {event_data['title']}, "
            f"Date: {event_data.get('startDate', 'N/A')}, "
            f"Time: {event_data.get('startTime', 'N/A')}, "
            f"Venue: {event_data['venue']}, "
            f"Price: {event_data.get('registrationFee', 'N/A')}, "
            f"Description: {event_data.get('description', 'N/A')}"
        )

        prompt = f"""
        You are given the following event information. Some fields may be missing or incorrect, and you need to complete them contextually.

        The 'price' should be categorized as the base price if available (in case of a range), otherwise 'Free' or 'TBA'. If the price is given as a range (e.g., "25-35 USD"), return only the lowest value.

        If the event name contains extra quotes or unnecessary characters, format it properly.

        If the 'description' is missing or incorrect, generate a catchy event description from the event name, address, and venue.

        Additionally, you need to categorize the event into one main category and one subcategory based on the event name and description.
        The subcategory must be strictly associated with the selected main category.

        Categories and their respective subcategories:

        - Sports: Live Sports Events, Amateur Sports Events, Sports Meetups, Sports-Themed Movies, Sports-Related Tech Events, Sports Social Gatherings
        - Movies: Sports-Themed Movies, General Action Movies, Drama Movies, Documentaries (including sports and tech documentaries)
        - Tech Events: Sports-Tech Conferences, General Tech Conferences, Hackathons and Workshops, Tech Meetups
        - Social Events: Sports-Related Social Gatherings, General Social Meetups, Cultural Events
        - Others: Miscellaneous Events

        The date should remain as provided, and the time should remain as provided (do not change the time).

        The information should be structured like this:
        {{
            "event_name": "formatted_event_name",
            "venue": "venue_name",
            "date": "original_date",
            "time": "original_time",
            "price": "single lowest value, Free, or TBA",
            "location": "full_address",
            "description": "correct_or_generated_event_description",
            "main_category": "one of the categories",
            "sub_category": "one of the associated subcategories to its main category"
        }}

        Event information: {event_info}
        """

        response = await openai.ChatCompletion.acreate(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are an event categorizer that specializes in organizing and describing events."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7
        )

        try:
            result = json.loads(response.choices[0].message.content.strip())
            return {
                'title': result['event_name'],
                'description': result['description'],
                'main_category': result['main_category'],
                'sub_category': result['sub_category'],
                'registrationFee': float(result['price']) if result['price'] not in ['Free', 'TBA'] else 0,
                'venue': result['venue']
            }
        except Exception as parse_error:
            print(f"Error parsing GPT response: {parse_error}")
            return {
                'main_category': "Others",
                'sub_category': "Miscellaneous Events",
                'description': event_data.get('description', f"Join us for {event_data['title']} at {event_data['venue']}!")
            }

    except Exception as e:
        print(f"Error in GPT categorization: {e}")
        return {
            'main_category': "Others",
            'sub_category': "Miscellaneous Events",
            'description': event_data.get('description', f"Join us for {event_data['title']} at {event_data['venue']}!")
        }

async def process_ticketmaster_event(event: dict):
    """Process Ticketmaster event to match MongoDB schema"""
    try:
        # Extract basic event details
        start_date = datetime.fromisoformat(event['dates']['start']['localDate'])
        start_time = event['dates']['start'].get('localTime', '19:00:00')
        
        # Calculate end time if not provided
        end_time = event['dates'].get('end', {}).get('localTime', '')
        if not end_time:
            end_datetime = datetime.strptime(start_time, '%H:%M:%S') + timedelta(hours=3)
            end_time = end_datetime.strftime('%H:%M:%S')

        # Get end date
        end_date = datetime.fromisoformat(
            event['dates'].get('end', {}).get('localDate', event['dates']['start']['localDate'])
        )

        # Process venue and price
        venue = event.get('_embedded', {}).get('venues', [{}])[0].get('name', 'Venue Not Specified')
        registration_fee = 0
        if event.get('priceRanges'):
            registration_fee = min(price['min'] for price in event['priceRanges'])

        # Create base event data
        event_data = {
            'title': event['name'],
            'venue': venue,
            'startTime': start_time,
            'endTime': end_time,
            'startDate': start_date,
            'endDate': end_date,
            'registrationFee': registration_fee,
            'organizerEmail': 'events@ticketmaster.com',
            'description': event.get('description', f"Join us for {event['name']} at {venue}!"),
            'imageUrl': event.get('images', [{'url': None}])[0].get('url'),
            'organizerId': 'ticketmaster',
            'rsvps': []
        }

        # Get enhanced data from GPT
        enhanced_data = await categorize_with_gpt(event_data)
        if enhanced_data:
            event_data.update(enhanced_data)

        return event_data

    except Exception as e:
        print(f"Error processing Ticketmaster event: {e}")
        return None

@app.post("/categorize/ticketmaster")
async def categorize_ticketmaster_event(event_data: Dict[str, Any]):
    """Endpoint for categorizing Ticketmaster events"""
    try:
        processed_event = await process_ticketmaster_event(event_data)
        
        if not processed_event:
            print(f"Failed to process event: {event_data.get('name', 'Unknown Event')}")
            return None

        # Add timestamps
        current_time = datetime.utcnow()
        processed_event['createdAt'] = current_time
        processed_event['updatedAt'] = current_time

        try:
            # Save to MongoDB
            result = await db.events.insert_one(processed_event)
            processed_event['_id'] = str(result.inserted_id)
            
            print(f"Successfully processed event: {processed_event['title']}")
            return processed_event

        except Exception as db_error:
            print(f"Database Error: {str(db_error)}")
            return None

    except Exception as e:
        print(f"Error in event processing: {str(e)}")
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