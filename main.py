from fastapi import FastAPI
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

async def is_duplicate_event(db, event_data: Dict[str, Any]) -> bool:
    """
    Check if an event already exists in the database.
    Returns True if it's a duplicate, False otherwise.
    """

    try:
        #Get the ticketmaster id if available
        ticketmaster_id = event_data.get('id')

        # Check for existing event with same Ticketmaster ID
        if ticketmaster_id:
            existing_event = await db.events.find_one({
                "ticketmaster_id":   ticketmaster_id 
            })
            if existing_event:
                print(f"[{datetime.now()}] Duplicate found by ticketmaster_id: {ticketmaster_id}")
                return True
            
        # If no Ticketmaster ID or not found, check for similar events by title and date
        title = event_data.get('title')
        start_date = event_data.get('startDate')
        venue = event_data.get('venue')

        if all([title, start_date, venue]):
                existing_event = await db.events.find_one({
                    "title": title,
                    "startDate": start_date,
                    "venue": venue
                })
                if existing_event:
                    print(f"[{datetime.now()}] Duplicate found by title/date/venue: {title}")
                    return True
        return False
    except Exception as e:
        print(f"[{datetime.now()}] Error checking for duplicates: {e}")
        return False

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
            'rsvps': [],
            'ticketmaster_id': event.get('id') # Add Ticketmaster ID for deduplication
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
    processing_start = datetime.now()
    print(f"[{processing_start}] Starting to process event: {event_data.get('name', 'Unknown')}")

    try:
        processed_event = await process_ticketmaster_event(event_data)
        
        if not processed_event:
            print(f"[{datetime.now()}] Failed to process event: {event_data.get('name', 'Unknown')}")
            return None
        
        # Check for duplicates before saving
        is_duplicate = await is_duplicate_event(db, processed_event)
        if is_duplicate:
            print(f"[{datetime.now()}] Skipping duplicate event: {processed_event['title']}")
            return None

        # Add timestamps
        current_time = datetime.utcnow()
        processed_event['createdAt'] = current_time
        processed_event['updatedAt'] = current_time

        # Save to database
        try:
            result = await db.events.insert_one(processed_event)
            processed_event['_id'] = str(result.inserted_id)
            processing_end = datetime.now()
            processing_duration = (processing_end - processing_start).total_seconds()
            print(f"[{processing_end}] Successfully saved new event: {processed_event['title']} (Duration: {processing_duration}s)")
            return processed_event
        
        except Exception as db_error:
            print(f"[{datetime.now()}] Database error: {db_error}")
            return None

    except Exception as e:
        print(f"[{datetime.now()}] Processing error: {e}")
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

        # Create indexes for efficient duplicate checking
        await db.events.create_index("ticketmaster_id", sparse=True)
        await db.events.create_index([
            ("title", 1),
            ("startDate", 1),
            ("venue", 1)
        ])
        print("✓ Database indexes created")
    except Exception as e:
        print(f"× MongoDB connection failed: {e}")

    try:
        asyncio.create_task(start_scheduler())
        print("✓ Ticketmaster sync scheduler started")
    except Exception as e:
        print(f"× Scheduler start failed: {e}")
