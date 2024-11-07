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

async def categorize_with_gpt(event_data: Dict[str, Any]):
    """Categorize event using GPT-3.5"""
    try:
        prompt = f"""
        Categorize the following event into one main category and one subcategory.
        
        Categories and their respective subcategories:
        - Sports: Live Sports Events, Amateur Sports Events, Sports Meetups, Sports-Themed Movies, Sports-Related Tech Events, Sports Social Gatherings
        - Movies: Sports-Themed Movies, General Action Movies, Drama Movies, Documentaries
        - Tech Events: Sports-Tech Conferences, General Tech Conferences, Hackathons and Workshops, Tech Meetups
        - Social Events: Sports-Related Social Gatherings, General Social Meetups, Cultural Events
        - Others: Miscellaneous Events

        Event Details:
        Title: {event_data['title']}
        Description: {event_data['description']}
        Venue: {event_data['venue']}

        Return only the category and subcategory in JSON format:
        {{"main_category": "category", "sub_category": "subcategory"}}
        """

        response = await openai.ChatCompletion.acreate(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are an event categorization system."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3
        )

        categories = eval(response.choices[0].message.content.strip())
        print(f"GPT Categories: {categories}")
        return categories
    except Exception as e:
        print(f"Error in categorization: {e}")
        return {"main_category": "Others", "sub_category": "Miscellaneous Events"}

async def process_ticketmaster_event(event: dict):
    """Transform Ticketmaster event data to match our schema"""
    try:
        print(f"Processing event: {event.get('name', 'Unknown Event')}")

        # Extract base date and time
        try:
            start_date = datetime.fromisoformat(event['dates']['start']['localDate'])
            start_time = event['dates']['start'].get('localTime', '19:00:00')
            
            # Calculate end time (default to 3 hours after start if not provided)
            end_time = event['dates'].get('end', {}).get('localTime', '')
            if not end_time:
                start_datetime = datetime.strptime(start_time, '%H:%M:%S')
                end_datetime = start_datetime + timedelta(hours=3)
                end_time = end_datetime.strftime('%H:%M:%S')

            end_date = datetime.fromisoformat(
                event['dates'].get('end', {}).get('localDate', event['dates']['start']['localDate'])
            )
        except Exception as date_error:
            print(f"Error processing dates for event {event.get('name')}: {str(date_error)}")
            start_date = datetime.now()
            end_date = datetime.now() + timedelta(hours=3)
            start_time = "19:00:00"
            end_time = "22:00:00"

        # Calculate registration fee
        registration_fee = 0
        if event.get('priceRanges'):
            registration_fee = min(price['min'] for price in event['priceRanges'])

        # Get venue information
        try:
            venue = event['_embedded']['venues'][0]['name']
            venue_address = event['_embedded']['venues'][0].get('address', {}).get('line1', '')
            venue_city = event['_embedded']['venues'][0].get('city', {}).get('name', '')
            venue_state = event['_embedded']['venues'][0].get('state', {}).get('name', '')
            full_address = f"{venue_address}, {venue_city}, {venue_state}".strip()
        except (KeyError, IndexError):
            venue = "Venue Not Specified"
            full_address = "Address Not Available"

        # Generate unique identifier for Ticketmaster events
        event_identifier = f"TM-{event.get('id', '')}"

        # Process description
        description = event.get('description', event.get('info', ''))
        if not description:
            description = f"Join us for {event['name']} at {venue}. Don't miss this exciting event!"

        processed_event = {
            'title': event['name'],
            'description': description,
            'venue': venue,
            'startDate': start_date,
            'endDate': end_date,
            'startTime': start_time,
            'endTime': end_time,
            'registrationFee': registration_fee,
            'imageUrl': event.get('images', [{'url': None}])[0].get('url'),
            'organizerType': 'ticketmaster',
            'organizerEmail': 'events@ticketmaster.com',
            'source': 'ticketmaster',
            'ticketmaster_id': event_identifier,
            'address': full_address,
            'rsvps': []
        }

        return processed_event

    except Exception as e:
        print(f"Error processing Ticketmaster event {event.get('name', 'Unknown')}: {str(e)}")
        return None

@app.post("/categorize/{event_id}")
async def categorize_manual_event(event_id: str):
    """Endpoint for categorizing manually created events"""
    try:
        if not ObjectId.is_valid(event_id):
            raise HTTPException(status_code=400, detail="Invalid event ID format")
        
        obj_id = ObjectId(event_id)
        event = await db.events.find_one({"_id": obj_id})
        
        if not event:
            raise HTTPException(status_code=404, detail="Event not found")

        categories = await categorize_with_gpt(event)
        
        update_result = await db.events.update_one(
            {"_id": obj_id},
            {
                "$set": {
                    "main_category": categories["main_category"],
                    "sub_category": categories["sub_category"],
                    "updatedAt": datetime.utcnow()
                }
            }
        )
        
        if update_result.modified_count > 0:
            return {"success": True, "categories": categories}
        else:
            raise HTTPException(status_code=500, detail="Failed to update event categories")

    except Exception as e:
        print(f"Error in categorize_event: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/categorize/ticketmaster")
async def categorize_ticketmaster_event(event_data: Dict[str, Any]):
    """Endpoint for categorizing Ticketmaster events"""
    try:
        # Process and validate the Ticketmaster event
        processed_event = await process_ticketmaster_event(event_data)
        
        if not processed_event:
            error_msg = f"Failed to process event: {event_data.get('name', 'Unknown Event')}"
            print(error_msg)
            raise HTTPException(status_code=400, detail=error_msg)

        # Get categories from GPT
        try:
            categories = await categorize_with_gpt({
                'title': processed_event['title'],
                'description': processed_event['description'],
                'venue': processed_event['venue']
            })
            processed_event.update(categories)
        except Exception as gpt_error:
            print(f"GPT Categorization error: {str(gpt_error)}")
            processed_event['main_category'] = "Others"
            processed_event['sub_category'] = "Miscellaneous Events"

        # Add timestamps
        processed_event.update({
            'createdAt': datetime.utcnow(),
            'updatedAt': datetime.utcnow()
        })

        try:
            # Check if event already exists
            existing_event = await db.events.find_one({
                'ticketmaster_id': processed_event['ticketmaster_id']
            })
            
            if existing_event:
                # Update existing event
                processed_event['_id'] = existing_event['_id']
                await db.events.replace_one(
                    {'_id': existing_event['_id']},
                    processed_event
                )
            else:
                # Insert new event
                result = await db.events.insert_one(processed_event)
                processed_event['_id'] = result.inserted_id
            
            return processed_event

        except Exception as db_error:
            print(f"Database Error: {str(db_error)}")
            raise HTTPException(status_code=500, detail=f"Database error: {str(db_error)}")

    except HTTPException:
        raise
    except Exception as e:
        error_msg = f"Unexpected error in categorize_ticketmaster_event: {str(e)}"
        print(error_msg)
        raise HTTPException(status_code=500, detail=error_msg)

@app.get("/")
async def root():
    return {
        "message": "Welcome to Hokie Event Categorizer API",
        "endpoints": {
            "/categorize/{event_id}": "Add categories to manually created events",
            "/categorize/ticketmaster": "Categorize and save Ticketmaster events",
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
        raise HTTPException(status_code=500, detail=f"Health check failed: {str(e)}")

@app.on_event("startup")
async def startup_event():
    """Startup event handler with environment variable checks"""
    print("\nStarting up FastAPI service...")
    
    required_vars = {
        "MONGO_URI": os.getenv("MONGO_URI"),
        "OPENAI_API_KEY": os.getenv("OPENAI_API_KEY"),
        "TICKETMASTER_API_KEY": os.getenv("TICKETMASTER_API_KEY"),
        "SELF_URL": os.getenv("SELF_URL"),
        "EXPRESS_BACKEND_URL": os.getenv("EXPRESS_BACKEND_URL")
    }

    missing_vars = [var for var, value in required_vars.items() if not value]
    if missing_vars:
        print(f"ERROR: Missing required environment variables: {', '.join(missing_vars)}")
        return

    print("✓ All required environment variables are set")

    try:
        await db.command('ping')
        print("✓ MongoDB connection successful")
    except Exception as e:
        print(f"✗ MongoDB connection failed: {e}")

    try:
        asyncio.create_task(start_scheduler())
        print("✓ Started Ticketmaster sync scheduler")
    except Exception as e:
        print(f"✗ Error starting scheduler: {e}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))