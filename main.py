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
    """Improved event categorization using GPT"""
    try:
        event_info = (
            f"Event: {event_data['title']}, "
            f"Venue: {event_data['venue']}, "
            f"Description: {event_data.get('description', 'No description available')}"
        )

        prompt = f"""
        Analyze this event carefully and categorize it based on its type and content. 
        
        Event Information: {event_info}

        Choose the most appropriate category and subcategory from these options:

        1. Sports Events:
           - Live Sports Events (for any professional or collegiate sports games)
           - Amateur Sports Events (for recreational or amateur competitions)
           - Sports Meetups (for sports-related gatherings)

        2. Entertainment Events:
           - Concerts & Music (for any musical performance)
           - Theater & Drama (for plays, musicals, dramatic performances)
           - Comedy Shows (for stand-up comedy, comedy performances)
           - Family Entertainment (for family-friendly shows, Disney on Ice, etc.)

        3. Cultural Events:
           - Art & Exhibition (for art shows, museum exhibitions)
           - Food & Drink (for food festivals, wine tastings)
           - Cultural Festivals (for cultural celebrations)

        4. Educational Events:
           - Conferences (for professional conferences)
           - Workshops (for learning sessions)
           - Tech Events (for technology-related events)

        5. Social Events:
           - Community Gatherings
           - Networking Events
           - Holiday Celebrations

        Also generate a compelling description if the current one is too brief.

        Return ONLY a JSON object in this exact format:
        {{
            "main_category": "category name",
            "sub_category": "specific subcategory name",
            "description": "detailed event description"
        }}

        The categorization should be very specific and accurate based on the event details provided.
        """

        # Using the new OpenAI client format
        response = await asyncio.to_thread(
            client.chat.completions.create,
            model="gpt-3.5-turbo",
            messages=[
                {
                    "role": "system", 
                    "content": "You are an expert event categorizer with deep knowledge of different types of events. Provide accurate and specific categorizations."
                },
                {"role": "user", "content": prompt}
            ],
            temperature=0.3  # Lower temperature for more consistent categorization
        )

        # Extract and validate the response
        if response.choices and response.choices[0].message:
            try:
                result = json.loads(response.choices[0].message.content)
                
                # Validate main category and provide fallback if needed
                main_categories = [
                    "Sports Events", "Entertainment Events", "Cultural Events",
                    "Educational Events", "Social Events"
                ]
                
                if result["main_category"] not in main_categories:
                    # Try to infer category from event title and description
                    event_text = f"{event_data['title']} {event_data.get('description', '')}".lower()
                    
                    if any(word in event_text for word in ['concert', 'music', 'band', 'singer', 'performance']):
                        result["main_category"] = "Entertainment Events"
                        result["sub_category"] = "Concerts & Music"
                    elif any(word in event_text for word in ['sports', 'game', 'match', 'tournament']):
                        result["main_category"] = "Sports Events"
                        result["sub_category"] = "Live Sports Events"
                    elif any(word in event_text for word in ['comedy', 'standup', 'laugh']):
                        result["main_category"] = "Entertainment Events"
                        result["sub_category"] = "Comedy Shows"
                    elif any(word in event_text for word in ['conference', 'tech', 'workshop']):
                        result["main_category"] = "Educational Events"
                        result["sub_category"] = "Conferences"
                    else:
                        result["main_category"] = "Entertainment Events"
                        result["sub_category"] = "General Entertainment"

                # Ensure we have a good description
                if not result.get("description") or len(result["description"]) < 50:
                    result["description"] = (
                        f"Join us for {event_data['title']} at {event_data['venue']}! "
                        f"This {result['sub_category']} event promises to be an unforgettable experience. "
                        f"Don't miss out on this exciting {result['main_category'].lower()} gathering!"
                    )

                print(f"Categorized '{event_data['title']}' as: {result['main_category']} - {result['sub_category']}")
                
                return result
                
            except json.JSONDecodeError as json_error:
                print(f"JSON parsing error: {json_error}")
                return infer_category_from_title(event_data)
                
        return infer_category_from_title(event_data)

    except Exception as e:
        print(f"Error in categorization: {e}")
        return infer_category_from_title(event_data)

def infer_category_from_title(event_data: Dict[str, Any]):
    """Infer category from event title and description when GPT fails"""
    title_lower = event_data['title'].lower()
    desc_lower = event_data.get('description', '').lower()
    combined_text = f"{title_lower} {desc_lower}"

    # Keywords for different categories
    category_keywords = {
        "Sports Events": ['game', 'sports', 'basketball', 'football', 'baseball', 'hockey', 'match', 'tournament'],
        "Entertainment Events": ['concert', 'music', 'show', 'performance', 'band', 'singer', 'live', 'tour'],
        "Cultural Events": ['festival', 'art', 'exhibition', 'museum', 'cultural', 'food', 'wine'],
        "Educational Events": ['conference', 'workshop', 'tech', 'learning', 'seminar', 'training'],
        "Social Events": ['party', 'gathering', 'meetup', 'social', 'networking', 'celebration']
    }

    # Subcategory mappings
    subcategory_mappings = {
        "Sports Events": "Live Sports Events",
        "Entertainment Events": "Concerts & Music",
        "Cultural Events": "Cultural Festivals",
        "Educational Events": "Conferences",
        "Social Events": "Community Gatherings"
    }

    # Find matching category
    for category, keywords in category_keywords.items():
        if any(keyword in combined_text for keyword in keywords):
            return {
                "main_category": category,
                "sub_category": subcategory_mappings[category],
                "description": event_data.get('description') or f"Join us for {event_data['title']} at {event_data['venue']}!"
            }

    # Default to Entertainment Events if no match found
    return {
        "main_category": "Entertainment Events",
        "sub_category": "General Entertainment",
        "description": event_data.get('description') or f"Join us for {event_data['title']} at {event_data['venue']}!"
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