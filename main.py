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

        return eval(response.choices[0].message.content.strip())
    except Exception as e:
        print(f"Error in categorization: {e}")
        raise HTTPException(status_code=500, detail=str(e))

async def process_ticketmaster_event(event: dict):
    """Transform Ticketmaster event data to match our schema"""
    try:
        # Print event data for debugging
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

            # Set end date
            end_date = datetime.fromisoformat(
                event['dates'].get('end', {}).get('localDate', event['dates']['start']['localDate'])
            )
        except Exception as date_error:
            print(f"Error processing dates for event {event.get('name')}: {str(date_error)}")
            raise

        # Calculate registration fee
        registration_fee = 0
        if event.get('priceRanges'):
            registration_fee = min(price['min'] for price in event['priceRanges'])

        # Get venue information safely
        try:
            venue = event['_embedded']['venues'][0]['name']
        except (KeyError, IndexError):
            print(f"Error getting venue for event {event.get('name')}")
            venue = "Venue Not Specified"

        event_identifier = f"TM-{event.get('id', '')}"

        # Construct event data
        processed_event = {
            'title': event['name'],
            'description': event.get('description', 'No description available'),
            'venue': venue,
            'startDate': start_date,
            'endDate': end_date,
            'startTime': start_time,
            'endTime': end_time,
            'registrationFee': registration_fee,
            'imageUrl': event.get('images', [{'url': None}])[0].get('url'),
            'organizerId': 'ticketmaster',
            'organizerEmail': 'events@ticketmaster.com',
            'source': 'ticketmaster',
            'ticketmaster_id': event_identifier,
            'rsvps': []
        }

        # Validate required fields
        required_fields = [
            'title', 'venue', 'startTime', 'endTime', 
            'startDate', 'endDate', 'organizerEmail', 
            'description', 'organizerId'
        ]
        
        missing_fields = [field for field in required_fields if not processed_event.get(field)]
        
        if missing_fields:
            print(f"Event {event['name']} missing required fields: {missing_fields}")
            return None

        return processed_event

    except Exception as e:
        print(f"Error processing Ticketmaster event {event.get('name', 'Unknown')}: {str(e)}")
        print(f"Event data: {event}")
        return None

@app.post("/categorize/{event_id}")
async def categorize_manual_event(event_id: str):
    """Endpoint for categorizing manually created events"""
    try:
        # Convert string ID to ObjectId
        obj_id = ObjectId(event_id)
        # Find the existing event

        event = await db.events.find_one({"_id": obj_id})
        if not event:
            raise HTTPException(status_code=404, detail="Event not found")

        # Get categories from GPT
        categories = await categorize_with_gpt(event)
        
        # Update only the category fields
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
            print(f"Successfully added categories to event: {event['title']}")
            return {
                "success": True,
                "categories": categories
            }
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
            print(f"Failed to process event: {event_data.get('name', 'Unknown Event')}")
            raise HTTPException(
                status_code=400,
                detail="Event could not be processed due to missing required data"
            )

        # Get categories from GPT
        try:
            categories = await categorize_with_gpt({
                'title': processed_event['title'],
                'description': processed_event['description'],
                'venue': processed_event['venue']
            })
        except Exception as gpt_error:
            print(f"GPT Categorization error: {str(gpt_error)}")
            raise HTTPException(status_code=500, detail=f"Categorization error: {str(gpt_error)}")

        # Add categories and timestamps
        processed_event.update({
            **categories,
            'createdAt': datetime.utcnow(),
            'updatedAt': datetime.utcnow()
        })

        # Generate a unique identifier for the event
        event_identifier = f"TM-{event_data.get('id', '')}"
        processed_event['ticketmaster_id'] = event_identifier
        
        # Save to MongoDB
        try:
            result = await db.events.update_one(
                {
                    'ticketmaster_id': event_identifier  # Use ticketmaster_id instead
                },
                {'$set': processed_event},
                upsert=True
            )
            
            print(f"Successfully categorized and saved Ticketmaster event: {processed_event['title']}")
            
            if result.upserted_id:
                processed_event['_id'] = str(result.upserted_id)
            
            return processed_event

        except Exception as db_error:
            print(f"Database Error: {str(db_error)}")
            print(f"Event that failed to save: {processed_event['title']}")
            raise HTTPException(status_code=500, detail=f"Database error: {str(db_error)}")

    except HTTPException as http_error:
        # Re-raise HTTP exceptions
        raise http_error
    except Exception as e:
        print(f"Unexpected error in categorize_ticketmaster_event: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

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
    
    # Check required environment variables
    required_vars = {
        "MONGO_URI": os.getenv("MONGO_URI"),
        "OPENAI_API_KEY": os.getenv("OPENAI_API_KEY"),
        "TICKETMASTER_API_KEY": os.getenv("TICKETMASTER_API_KEY"),
        "SELF_URL": os.getenv("SELF_URL"),
        "EXPRESS_BACKEND_URL": os.getenv("EXPRESS_BACKEND_URL")
    }

    # Verify all required variables are set
    missing_vars = [var for var, value in required_vars.items() if not value]
    if missing_vars:
        print(f"ERROR: Missing required environment variables: {', '.join(missing_vars)}")
        return

    print("✓ All required environment variables are set")

    # Test MongoDB connection
    try:
        await db.command('ping')
        print("✓ MongoDB connection successful")
    except Exception as e:
        print(f"✗ MongoDB connection failed: {e}")

    # Start Ticketmaster sync
    try:
        asyncio.create_task(start_scheduler())
        print("✓ Started Ticketmaster sync scheduler")
    except Exception as e:
        print(f"✗ Error starting scheduler: {e}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))