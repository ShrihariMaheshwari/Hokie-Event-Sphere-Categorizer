from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import openai
import os
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
import motor.motor_asyncio
import asyncio
from datetime import datetime, timedelta
from cron.ticketmaster_sync import start_scheduler

class RSVP(BaseModel):
    name: str
    email: str
    phone: Optional[str] = None
    createdAt: datetime = Field(default_factory=datetime.utcnow)

class Event(BaseModel):
    title: str
    venue: str
    startTime: str
    endTime: str
    startDate: datetime
    endDate: datetime
    registrationFee: Optional[float] = None
    organizerEmail: str
    description: str
    imageUrl: Optional[str] = None
    organizerId: str
    rsvps: List[RSVP] = []
    createdAt: Optional[datetime] = None
    updatedAt: Optional[datetime] = None
    source: Optional[str] = None  # 'manual' or 'ticketmaster'

app = FastAPI(title="Hokie Event Categorizer")

# CORS setup
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://hokieeventspherebackend.onrender.com"],  # Configure this with specific origins in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# MongoDB setup
mongo_client = motor.motor_asyncio.AsyncIOMotorClient(os.getenv("MONGO_URI"))
db = mongo_client.events_db

# OpenAI setup
openai.api_key = os.getenv("OPENAI_API_KEY")


async def categorize_with_gpt(event_data: dict):
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
        # Extract base date and time
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

        # Calculate registration fee
        registration_fee = 0
        if event.get('priceRanges'):
            registration_fee = min(price['min'] for price in event['priceRanges'])

        # Construct event data
        processed_event = {
            'title': event['name'],
            'description': event.get('description', 'No description available'),
            'venue': event['_embedded']['venues'][0]['name'],
            'startDate': start_date,
            'endDate': end_date,
            'startTime': start_time,
            'endTime': end_time,
            'registrationFee': registration_fee,
            'imageUrl': event['images'][0]['url'] if event.get('images') else None,
            'organizerId': 'ticketmaster',
            'organizerEmail': 'events@ticketmaster.com',
            'source': 'ticketmaster',
            'rsvps': [],
            'createdAt': datetime.utcnow(),
            'updatedAt': datetime.utcnow()
        }

        # Validate required fields
        required_fields = [
            'title', 'venue', 'startTime', 'endTime', 
            'startDate', 'endDate', 'organizerEmail', 
            'description', 'organizerId'
        ]
            
        missing_fields = [field for field in required_fields if not processed_event.get(field)]
            
        if missing_fields:
            print(f"Warning: Event {event['name']} missing required fields: {missing_fields}")
            return None

        return processed_event

    except Exception as e:
        print(f"Error processing Ticketmaster event {event.get('name', 'Unknown')}: {e}")
        return None

@app.post("/categorize/{event_id}")
async def categorize_event(event_id: str):
    """
    Endpoint for categorizing events from Express backend
    This is called after event creation in Express
    """
    try:
        # Find the existing event
        event = await db.Event.find_one({"_id": event_id})
        if not event:
            raise HTTPException(status_code=404, detail="Event not found")
        
        # Get categories from GPT
        categories = await categorize_with_gpt(event.dict())
        
        # Update only the category fields
        update_result = await db.Event.update_one(
            {"_id": event_id},
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
            raise HTTPException(
                status_code=400, 
                detail="Event could not be processed due to missing required data"
            )

        # Get categories from GPT
        categories = await categorize_with_gpt({
            'title': processed_event['title'],
            'description': processed_event['description'],
            'venue': processed_event['venue']
        })
        
        # Add categories
        processed_event.update(categories)
        
        # Save to MongoDB with upsert
        result = await db.Event.update_one(
            {
                'title': processed_event['title'],
                'startDate': processed_event['startDate'],
                'source': 'ticketmaster'
            },
            {'$set': processed_event},
            upsert=True
        )
        
        print(f"Successfully categorized and saved Ticketmaster event: {processed_event['title']}")
        
        if result.upserted_id:
            processed_event['_id'] = str(result.upserted_id)
        
        return processed_event

    except Exception as e:
        print(f"Error in categorize_ticketmaster_event: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    
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
    try:
        asyncio.create_task(start_scheduler())
        print("Started Ticketmaster sync scheduler")
    except Exception as e:
        print(f"Error starting scheduler: {e}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
