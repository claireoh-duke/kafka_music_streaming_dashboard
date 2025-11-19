import time
import json
import uuid
import random
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker

fake = Faker()

def create_music_event():
    """Creates synthetic Spotify-style music streaming events."""
    actions = ["play", "skip", "like", "add_to_playlist"]
    genres = ["Pop", "Hip-Hop", "Rock", "K-Pop", "EDM", "Jazz", "Classical"]
    devices = ["iPhone", "Android", "Web", "Windows", "Mac"]
    countries = ["US", "KR", "FR", "DE", "JP", "BR", "GB", "IN"]
    artists = [
        "NewJeans",
        "Taylor Swift",
        "Drake",
        "IU",
        "BTS",
        "Billie Eilish",
        "Blackpink",
        "Coldplay",
        "Ed Sheeran",
    ]
    
    action = random.choice(actions)
    genre = random.choice(genres)
    duration = random.randint(120, 300)
    played = min(duration, random.randint(10, duration))
    liked = action == "like" or (random.random() < 0.08)
    
    return {
        "event_id": str(uuid.uuid4())[:8],
        "user_id": str(uuid.uuid4())[:6],
        "country": random.choice(countries),
        "device": random.choice(devices),
        "artist": random.choice(artists),
        "song_title": fake.sentence(nb_words=3).replace(".", ""),
        "genre": genre,
        "action": action,
        "duration_sec": duration,
        "played_sec": played,
        "liked": liked,
        "event_time": datetime.now().isoformat(sep=" ", timespec="seconds"),
    }

def run_streamer():
    try:
        print("[Streamer] Connecting to Kafka at localhost:9092...")
        streamer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            # ✨ Performance optimization: batch sending
            linger_ms=100,  # Wait 100ms before sending batch
            batch_size=16384,  # 16KB batch size
            compression_type='lz4',  # Enable compression
            # ✨ Reduce timeout
            request_timeout_ms=10000,  # 30s → 10s
            max_block_ms=5000,  # 60s → 5s
            retries=3,  # 5 → 3
            acks=1,  # all → 1 (only leader confirms, faster)
        )
        print("[Streamer] ✓ Connected to Kafka successfully!")
        
        count = 0
        batch_count = 0
        
        while True:
            evt = create_music_event()
            
            # ✨ Async sending (remove blocking)
            if count % 10 == 0:  # Print only every 10 events
                print(f"[Streamer] Sending events #{count}-{count+9}...")
            
            future = streamer.send("music_stream", value=evt)
            
            # ✨ Check only on error (remove .get())
            future.add_errback(lambda e: print(f"[Streamer ERROR] {e}"))
            
            count += 1
            batch_count += 1
            
            # ✨ Flush every 100 events (remove flush every time)
            if batch_count >= 100:
                streamer.flush()
                print(f"[Streamer] ✓ Flushed {batch_count} events (Total: {count})")
                batch_count = 0
            
            # ✨ Reduce wait time
            time.sleep(random.uniform(0.1, 0.5))  # 0.5-2s → 0.1-0.5s
            
    except Exception as e:
        print(f"[Streamer ERROR] {e}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    run_streamer()
