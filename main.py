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
    """Categorize event using GPT-3.5"""
    try:
        # Format dates for the prompt
        start_date = format_datetime(event_data.get('startDate', 'N/A'))
        start_time = event_data.get('startTime', 'N/A')
        
        # Create event info string with proper formatting
        event_info = (
            f"Event: {event_data['title']}, "
            f"Date: {start_date}, "
            f"Time: {start_time}, "
            f"Venue: {event_data['venue']}, "
            f"Address: {event_data.get('address', 'N/A')}, "
            f"Price: {event_data.get('registrationFee', 'N/A')}, "
            f"Description: {event_data['description']}"
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
                {"role": "system", "content": "You are an event planner AI that specializes in categorizing and formatting event information."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
            max_tokens=500
        )

        # Parse the response
        try:
            result_text = response.choices[0].message.content.strip()
            # Try to parse the response as JSON first
            try:
                result = json.loads(result_text)
            except json.JSONDecodeError:
                # If JSON parsing fails, try using eval as fallback
                result = eval(result_text)

            print(f"GPT Response for {event_data['title']}: {result}")
            
            # Update the event data with the processed information
            return {
                'title': result['event_name'],
                'venue': result['venue'],
                'description': result['description'],
                'main_category': result['main_category'],
                'sub_category': result['sub_category'],
                'address': result['location'],
                # Keep original dates and times if they exist
                'startDate': event_data.get('startDate', result.get('date')),
                'startTime': event_data.get('startTime', result.get('time')),
                'registrationFee': result['price'] if result['price'] not in ['Free', 'TBA'] else 0
            }
        except Exception as parse_error:
            print(f"Error parsing GPT response: {parse_error}")
            print(f"Raw response: {response.choices[0].message.content}")
            # Return default values if parsing fails
            return {
                'main_category': "Others",
                'sub_category': "Miscellaneous Events",
                'description': event_data['description']
            }

    except Exception as e:
        print(f"Error in categorization: {e}")
        return {
            'main_category': "Others",
            'sub_category': "Miscellaneous Events",
            'description': event_data['description']
        }

async def process_ticketmaster_event(event: dict):
    """Transform Ticketmaster event data to match our schema"""
    try:
        print(f"Processing event: {event.get('name', 'Unknown Event')}")

        # Extract base date and time with error handling
        try:
            start_date = datetime.fromisoformat(event['dates']['start']['localDate'])
            start_time = event['dates']['start'].get('localTime', '19:00:00')
            
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
            current_time = datetime.now()
            start_date = current_time
            end_date = current_time + timedelta(hours=3)
            start_time = "19:00:00"
            end_time = "22:00:00"

        # Get venue information with comprehensive error handling
        try:
            venue_info = event.get('_embedded', {}).get('venues', [{}])[0]
            venue = venue_info.get('name', 'Venue Not Specified')
            venue_address = venue_info.get('address', {}).get('line1', '')
            venue_city = venue_info.get('city', {}).get('name', '')
            venue_state = venue_info.get('state', {}).get('name', '')
            
            # Build address with proper formatting
            address_parts = [p for p in [venue_address, venue_city, venue_state] if p]
            full_address = ", ".join(address_parts) if address_parts else "Address Not Available"
            
        except Exception as venue_error:
            print(f"Error processing venue for {event.get('name')}: {str(venue_error)}")
            venue = "Venue Not Specified"
            full_address = "Address Not Available"

        # Process price information with error handling
        try:
            registration_fee = 0
            if event.get('priceRanges'):
                prices = [price.get('min', 0) for price in event['priceRanges'] if price.get('min') is not None]
                registration_fee = min(prices) if prices else 0
        except Exception as price_error:
            print(f"Error processing price: {str(price_error)}")
            registration_fee = 0

        # Process description with multiple fallbacks
        base_description = (
            event.get('description') or 
            event.get('info') or 
            event.get('pleaseNote') or 
            event.get('additionalInfo')
        )
        
        if not base_description:
            ticket_info = "Tickets available now!" if event.get('url') else "Contact venue for more details."
            base_description = f"Join us for {event['name']} at {venue}! Experience this exciting event live. {ticket_info}"

        # Generate unique identifier
        event_identifier = f"TM-{event.get('id', str(ObjectId()))}"

        # Create initial event data
        event_data = {
            'title': event['name'],
            'description': base_description,
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

        # Get enhanced data from GPT
        enhanced_data = await categorize_with_gpt(event_data)
        if enhanced_data:
            event_data.update(enhanced_data)
        
        return event_data

    except Exception as e:
        print(f"Detailed error processing Ticketmaster event: {str(e)}")
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

        enhanced_data = await categorize_with_gpt(event)
        
        if not enhanced_data:
            raise HTTPException(status_code=500, detail="Failed to categorize event")

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
            return {"success": True, "categories": enhanced_data}
        else:
            raise HTTPException(status_code=500, detail="Failed to update event categories")

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error in categorize_event: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/categorize/ticketmaster")
async def categorize_ticketmaster_event(event_data: Dict[str, Any]):
    """Endpoint for categorizing Ticketmaster events"""
    try:
        processed_event = await process_ticketmaster_event(event_data)
        
        if not processed_event:
            error_msg = f"Failed to process event: {event_data.get('name', 'Unknown Event')}"
            print(error_msg)
            raise HTTPException(status_code=400, detail=error_msg)

        # Add timestamps
        current_time = datetime.utcnow()
        processed_event['createdAt'] = current_time
        processed_event['updatedAt'] = current_time

        try:
            # Check if event already exists
            existing_event = await db.events.find_one({
                'ticketmaster_id': processed_event['ticketmaster_id']
            })
            
            if existing_event:
                # Update existing event
                result = await db.events.replace_one(
                    {'_id': existing_event['_id']},
                    {**processed_event, '_id': existing_event['_id']}
                )
                processed_event['_id'] = str(existing_event['_id'])
            else:
                # Insert new event
                result = await db.events.insert_one(processed_event)
                processed_event['_id'] = str(result.inserted_id)
            
            print(f"Successfully processed event: {processed_event['title']}")
            return processed_event

        except Exception as db_error:
            print(f"Database Error for {processed_event['title']}: {str(db_error)}")
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