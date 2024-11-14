from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import openai
import os
from typing import Dict, Any, List
from datetime import datetime, timedelta
import motor.motor_asyncio
import asyncio
from bson import ObjectId
from cron.ticketmaster_sync import start_scheduler
import json
import math
from collections import defaultdict
from transformers import AutoTokenizer, AutoModel
import torch
import numpy as np
from annoy import AnnoyIndex
import asyncio
from functools import lru_cache

app = FastAPI(title="Hokie Event Categorizer")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[os.getenv("EXPRESS_BACKEND_URL", "*")],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

mongo_client = motor.motor_asyncio.AsyncIOMotorClient(os.getenv("MONGO_URI"))
db = mongo_client.events_db
client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# BERT and Annoy initialization
tokenizer = AutoTokenizer.from_pretrained('bert-base-uncased')
model = AutoModel.from_pretrained('bert-base-uncased')
EMBEDDING_DIM = 768
event_embeddings_cache = {}
annoy_index = None

def create_event_embedding(event: Dict[str, Any]) -> np.ndarray:
    event_text = f"{event['title']} {event['description']} {event['main_category']} {event['sub_category']}"
    inputs = tokenizer(event_text, return_tensors='pt', padding=True, truncation=True, max_length=512)
    with torch.no_grad():
        outputs = model(**inputs)
    embedding = outputs.last_hidden_state[0, 0, :].numpy()
    return embedding

async def build_annoy_index(events: List[Dict[str, Any]]) -> AnnoyIndex:
    index = AnnoyIndex(EMBEDDING_DIM, 'angular')
    for i, event in enumerate(events):
        if event['_id'] not in event_embeddings_cache:
            embedding = create_event_embedding(event)
            event_embeddings_cache[str(event['_id'])] = {
                'embedding': embedding,
                'event': event,
                'index': i
            }
        index.add_item(i, event_embeddings_cache[str(event['_id'])]['embedding'])
    index.build(10)
    return index

@lru_cache(maxsize=1)
def get_annoy_index():
    return annoy_index

async def update_recommendations_index():
    global annoy_index
    current_date = datetime.utcnow()
    events = await db.events.find({"startDate": {"$gte": current_date}}).to_list(None)
    annoy_index = await build_annoy_index(events)
    return annoy_index

async def get_similar_events(event_embedding: np.ndarray, n_neighbors: int = 10) -> List[Dict[str, Any]]:
    index = get_annoy_index()
    if not index:
        await update_recommendations_index()
        index = get_annoy_index()
    
    similar_indices = index.get_nns_by_vector(event_embedding, n_neighbors)
    similar_events = []
    for idx in similar_indices:
        for cache_item in event_embeddings_cache.values():
            if cache_item['index'] == idx:
                similar_events.append(cache_item['event'])
                break
    return similar_events

async def categorize_with_gpt(event_data: Dict[str, Any]):
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
           - Live Sports Events
           - Amateur Sports Events
           - Sports Meetups
        2. Entertainment Events:
           - Concerts & Music
           - Theater & Drama
           - Comedy Shows
           - Family Entertainment
        3. Cultural Events:
           - Art & Exhibition
           - Food & Drink
           - Cultural Festivals
        4. Educational Events:
           - Conferences
           - Workshops
           - Tech Events
        5. Social Events:
           - Community Gatherings
           - Networking Events
           - Holiday Celebrations
        Return ONLY a JSON object in this exact format:
        {{
            "main_category": "category name",
            "sub_category": "specific subcategory name",
            "description": "detailed event description"
        }}
        """

        response = await asyncio.to_thread(
            client.chat.completions.create,
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are an expert event categorizer."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3
        )

        if response.choices and response.choices[0].message:
            try:
                result = json.loads(response.choices[0].message.content)
                main_categories = [
                    "Sports Events", "Entertainment Events", "Cultural Events",
                    "Educational Events", "Social Events"
                ]
                
                if result["main_category"] not in main_categories:
                    return infer_category_from_title(event_data)
                    
                if not result.get("description") or len(result["description"]) < 50:
                    result["description"] = (
                        f"Join us for {event_data['title']} at {event_data['venue']}! "
                        f"This {result['sub_category']} event promises to be an unforgettable experience."
                    )
                
                return result
                
            except json.JSONDecodeError:
                return infer_category_from_title(event_data)
                
        return infer_category_from_title(event_data)

    except Exception as e:
        print(f"Error in categorization: {e}")
        return infer_category_from_title(event_data)

def infer_category_from_title(event_data: Dict[str, Any]):
    title_lower = event_data['title'].lower()
    desc_lower = event_data.get('description', '').lower()
    combined_text = f"{title_lower} {desc_lower}"

    category_keywords = {
        "Sports Events": ['game', 'sports', 'basketball', 'football', 'baseball', 'tournament'],
        "Entertainment Events": ['concert', 'music', 'show', 'performance', 'band', 'singer'],
        "Cultural Events": ['festival', 'art', 'exhibition', 'museum', 'cultural', 'food'],
        "Educational Events": ['conference', 'workshop', 'tech', 'learning', 'seminar'],
        "Social Events": ['party', 'gathering', 'meetup', 'social', 'networking']
    }

    subcategory_mappings = {
        "Sports Events": "Live Sports Events",
        "Entertainment Events": "Concerts & Music",
        "Cultural Events": "Cultural Festivals",
        "Educational Events": "Conferences",
        "Social Events": "Community Gatherings"
    }

    for category, keywords in category_keywords.items():
        if any(keyword in combined_text for keyword in keywords):
            return {
                "main_category": category,
                "sub_category": subcategory_mappings[category],
                "description": event_data.get('description') or f"Join us for {event_data['title']} at {event_data['venue']}!"
            }

    return {
        "main_category": "Entertainment Events",
        "sub_category": "General Entertainment",
        "description": event_data.get('description') or f"Join us for {event_data['title']} at {event_data['venue']}!"
    }

async def calculate_event_recommendations(
    db,
    user_id: str,
    user_email: str,
    user_location: Dict[str, Any],
    limit: int = 10
) -> List[Dict[str, Any]]:
    try:
        weighted_recs = await calculate_weighted_recommendations(
            db, user_id, user_email, user_location, limit
        )
        
        semantic_recs = await calculate_semantic_recommendations(
            db, user_id, user_email, limit
        )
        
        final_recommendations = combine_recommendations(
            weighted_recs,
            semantic_recs,
            weight_collaborative=0.6,
            weight_semantic=0.4
        )
        
        return final_recommendations[:limit]
        
    except Exception as e:
        print(f"Error calculating recommendations: {e}")
        raise HTTPException(status_code=500, detail=str(e))

async def calculate_weighted_recommendations(
    db,
    user_id: str,
    user_email: str,
    user_location: Dict[str, Any],
    limit: int = 10
) -> List[Dict[str, Any]]:
    try:
        weights = {
            "category": 0.30,
            "rsvp": 0.25,
            "location": 0.20,
            "interests": 0.15,
            "price": 0.10
        }

        user_profile = await db.userprofiles.find_one({"_id": ObjectId(user_id)})
        if not user_profile:
            raise HTTPException(status_code=404, detail="User profile not found")

        click_history = await db.clickcounts.find({
            "userId": user_email
        }).to_list(None)

        rsvp_history = await db.events.find({
            "rsvps.email": user_email
        }).to_list(None)

        category_weights = defaultdict(float)
        subcategory_weights = defaultdict(float)
        total_clicks = 0

        for click in click_history:
            category_weights[click["category"]] += click["categoryCount"]
            total_clicks += click["categoryCount"]
            for sub in click["subCategories"]:
                subcategory_weights[sub["subCategory"]] += sub["subCategoryCount"]

        if total_clicks > 0:
            category_weights = {k: v/total_clicks for k, v in category_weights.items()}
            subcategory_weights = {k: v/total_clicks for k, v in subcategory_weights.items()}

        rsvp_patterns = analyze_rsvp_patterns(rsvp_history)

        current_date = datetime.utcnow()
        future_events = await db.events.find({
            "startDate": {"$gte": current_date}
        }).to_list(None)

        scored_events = []
        for event in future_events:
            scores = {
                "category": calculate_category_score(event, category_weights, subcategory_weights),
                "rsvp": calculate_rsvp_similarity(event, rsvp_patterns),
                "location": calculate_location_score(event, user_location),
                "interests": calculate_interest_match(event, user_profile.get("interests", [])),
                "price": calculate_price_compatibility(event, rsvp_patterns["price_range"])
            }

            final_score = sum(score * weights[key] for key, score in scores.items())
            scored_events.append({
                "event": event,
                "score": final_score,
                "score_breakdown": scores
            })

        scored_events.sort(key=lambda x: x["score"], reverse=True)
        return scored_events[:limit]

    except Exception as e:
        print(f"Error calculating recommendations: {e}")
        raise HTTPException(status_code=500, detail=str(e))

async def calculate_semantic_recommendations(
    db,
    user_id: str,
    user_email: str,
    limit: int = 10
) -> List[Dict[str, Any]]:
    try:
        user_events = await db.events.find({
            "rsvps.email": user_email
        }).to_list(None)
        
        if not user_events:
            return []
        
        user_profile_embedding = np.zeros(EMBEDDING_DIM)
        for event in user_events:
            if str(event['_id']) not in event_embeddings_cache:
                embedding = create_event_embedding(event)
                event_embeddings_cache[str(event['_id'])] = {
                    'embedding': embedding,
                    'event': event,
                    'index': len(event_embeddings_cache)
                }
            user_profile_embedding += event_embeddings_cache[str(event['_id'])]['embedding']
        
        if len(user_events) > 0:
            user_profile_embedding /= len(user_events)
        
        similar_events = await get_similar_events(user_profile_embedding, limit)
        
        scored_events = []
        for event in similar_events:
            event_embedding = event_embeddings_cache[str(event['_id'])]['embedding']
            similarity_score = 1 - np.dot(user_profile_embedding, event_embedding) / (
                np.linalg.norm(user_profile_embedding) * np.linalg.norm(event_embedding)
            )
            
            scored_events.append({
                "event": event,
                "score": similarity_score
            })
        
        return scored_events
        
    except Exception as e:
        print(f"Error in semantic recommendations: {e}")
        return []

def combine_recommendations(
    weighted_recs: List[Dict[str, Any]],
    semantic_recs: List[Dict[str, Any]],
    weight_collaborative: float = 0.6,
    weight_semantic: float = 0.4
) -> List[Dict[str, Any]]:
    combined_scores = {}
    
    def normalize_scores(recs):
        scores = [r['score'] for r in recs]
        min_score, max_score = min(scores), max(scores)
        score_range = max_score - min_score
        return {str(r['event']['_id']): (r['score'] - min_score) / score_range 
                if score_range > 0 else 0.5 for r in recs}
    
    weighted_scores = normalize_scores(weighted_recs)
    semantic_scores = normalize_scores(semantic_recs)
    
    all_events = set(weighted_scores.keys()) | set(semantic_scores.keys())
    for event_id in all_events:
        w_score = weighted_scores.get(event_id, 0)
        s_score = semantic_scores.get(event_id, 0)
        combined_scores[event_id] = (
            w_score * weight_collaborative + 
            s_score * weight_semantic
        )
    
    final_recs = []
    for event_id, score in sorted(combined_scores.items(), key=lambda x: x[1], reverse=True):
        event = next((r['event'] for r in weighted_recs + semantic_recs 
                     if str(r['event']['_id']) == event_id), None)
        if event:
            final_recs.append({
                "event": event,
                "score": score
            })
    
    return final_recs

def analyze_rsvp_patterns(rsvp_history: List[Dict[str, Any]]) -> Dict[str, Any]:
    patterns = {
        "categories": defaultdict(int),
        "subcategories": defaultdict(int),
        "price_range": {"min": float('inf'), "max": 0, "avg": 0},
        "venues": defaultdict(int),
        "total_rsvps": len(rsvp_history) or 1
    }

    total_price = 0
    price_count = 0

    for event in rsvp_history:
        patterns["categories"][event["main_category"]] += 1
        patterns["subcategories"][event["sub_category"]] += 1
        patterns["venues"][event["venue"]] += 1
        
        if event.get("registrationFee", 0) > 0:
            price = event["registrationFee"]
            patterns["price_range"]["min"] = min(patterns["price_range"]["min"], price)
            patterns["price_range"]["max"] = max(patterns["price_range"]["max"], price)
            total_price += price
            price_count += 1

    if price_count > 0:
        patterns["price_range"]["avg"] = total_price / price_count
    elif patterns["price_range"]["min"] == float('inf'):
        patterns["price_range"].update({"min": 0, "max": 0, "avg": 0})

    return patterns

def calculate_category_score(
    event: Dict[str, Any],
    category_weights: Dict[str, float],
    subcategory_weights: Dict[str, float]
) -> float:
    category_score = category_weights.get(event["main_category"], 0)
    subcategory_score = subcategory_weights.get(event["sub_category"], 0)
    return (category_score * 0.6 + subcategory_score * 0.4)

def calculate_rsvp_similarity(
    event: Dict[str, Any],
    rsvp_patterns: Dict[str, Any]
) -> float:
    if rsvp_patterns["total_rsvps"] == 0:
        return 0.5

    category_similarity = rsvp_patterns["categories"][event["main_category"]] / rsvp_patterns["total_rsvps"]
    subcategory_similarity = rsvp_patterns["subcategories"][event["sub_category"]] / rsvp_patterns["total_rsvps"]
    venue_similarity = rsvp_patterns["venues"][event["venue"]] / rsvp_patterns["total_rsvps"]
    
    return (category_similarity * 0.4 + subcategory_similarity * 0.4 + venue_similarity * 0.2)

def calculate_location_score(
    event: Dict[str, Any],
    user_location: Dict[str, Any]
) -> float:
    if not (event.get("location", {}).get("coordinates") and user_location.get("coordinates")):
        return 0.5

    distance = calculate_distance(
        user_location["coordinates"]["latitude"],
        user_location["coordinates"]["longitude"],
        event["location"]["coordinates"]["latitude"],
        event["location"]["coordinates"]["longitude"]
    )
    return max(0, 1 - (distance / 50))

def calculate_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    return R * c

def calculate_interest_match(
    event: Dict[str, Any],
    interests: List[str]
) -> float:
    if not interests:
        return 0.5

    event_text = f"{event['title']} {event['description']}".lower()
    matching_interests = sum(1 for interest in interests if interest.lower() in event_text)
    return matching_interests / len(interests)

def calculate_price_compatibility(
    event: Dict[str, Any],
    price_range: Dict[str, float]
) -> float:
    event_price = event.get("registrationFee", 0)
    
    if price_range["max"] == 0:
        return 1.0 if event_price == 0 else 0.5
        
    if event_price == 0:
        return 1.0
        
    price_diff = abs(event_price - price_range["avg"])
    price_range_size = price_range["max"] - price_range["min"]
    
    if price_range_size == 0:
        return 1.0 if event_price == price_range["avg"] else 0.0
        
    return max(0, 1 - (price_diff / price_range_size))

@app.get("/recommendations/{user_id}")
async def get_recommendations(
    user_id: str,
    user_email: str,
    latitude: float = None,
    longitude: float = None,
    limit: int = 10
):
    try:
        user_location = {
            "coordinates": {
                "latitude": latitude,
                "longitude": longitude
            }
        } if latitude and longitude else {}

        recommendations = await calculate_event_recommendations(
            db,
            user_id,
            user_email,
            user_location,
            limit
        )

        processed_recs = []
        for rec in recommendations:
            try:
                event = rec["event"]
                if not event.get("startDate"):
                    continue
                    
                processed_recs.append({
                    "title": str(event.get("title", "")),
                    "venue": str(event.get("venue", "")),
                    "date": event["startDate"].strftime("%Y-%m-%d"),
                    "score": {
                        "total": round(float(rec["score"]), 3),
                    }
                })
            except Exception as e:
                print(f"Error processing recommendation: {str(e)}")
                continue

        return {"recommendations": processed_recs}

    except Exception as e:
        print(f"Error in recommendations endpoint: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.on_event("startup")
async def startup_event():
    print("\nStarting FastAPI service...")
    required_vars = {
        "MONGO_URI": os.getenv("MONGO_URI"),
        "OPENAI_API_KEY": os.getenv("OPENAI_API_KEY"),
        "TICKETMASTER_API_KEY": os.getenv("TICKETMASTER_API_KEY"),
    }

    missing_vars = [var for var, value in required_vars.items() if not value]
    if missing_vars:
        print(f"Missing required environment variables: {', '.join(missing_vars)}")
        return

    try:
        await db.command('ping')
        await db.events.create_index("ticketmaster_id", sparse=True)
        await db.events.create_index([("title", 1), ("startDate", 1), ("venue", 1)])
        await update_recommendations_index()
        asyncio.create_task(start_scheduler())
        print("✓ Service started successfully")
    except Exception as e:
        print(f"Startup error: {e}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)