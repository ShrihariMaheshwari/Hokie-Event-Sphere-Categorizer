from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import openai
import os
from pydantic import BaseModel
from typing import Optional
import motor.motor_asyncio
import asyncio
from datetime import datetime
from cron.ticketmaster_sync import start_scheduler

app = FastAPI(title="Hokie Event Categorizer")

# CORS setup
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure this with specific origins in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# MongoDB setup
mongo_client = motor.motor_asyncio.AsyncIOMotorClient(os.getenv("MONGO_URI"))
db = mongo_client.events_db

# OpenAI setup
openai.api_key = os.getenv("OPENAI_API_KEY")

class Event(BaseModel):
    title: str
    description: str
    venue: str
    startDate: str
    startTime: str
    endTime: str
    source: str
    imageUrl: Optional[str]
    registrationFee: Optional[float]
    organizerEmail: Optional[str]

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

@app.post("/categorize")
async def categorize_event(event: Event):
    try:
        # Get categories from GPT
        categories = await categorize_with_gpt(event.dict())
        
        # Combine event data with categories
        categorized_event = {
            **event.dict(),
            **categories,
            'categorized_at': datetime.utcnow()
        }
        
        print(f"Successfully categorized event: {event.title}")
        return categorized_event

    except Exception as e:
        print(f"Error in categorize_event: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    try:
        # Test MongoDB connection
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
        # Start the Ticketmaster sync scheduler
        asyncio.create_task(start_scheduler())
        print("Started Ticketmaster sync scheduler")
    except Exception as e:
        print(f"Error starting scheduler: {e}")

# if __name__ == "__main__":
#     import uvicorn
#     port = int(os.getenv("PORT", "8000"))
#     uvicorn.run(app, host="0.0.0.0", port=port)