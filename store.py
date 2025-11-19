import json
import psycopg2
import psycopg2.extras
from kafka import KafkaConsumer

def run_store():
    try:
        print("[Store] Connecting to Kafka at localhost:9092...")
        music_consumer = KafkaConsumer(
            "music_stream",
            bootstrap_servers="localhost:9092",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            group_id="music-stream-group",
            # ✨ Performance optimization: fetch in batch
            max_poll_records=100,  # Fetch 100 records at once
            fetch_min_bytes=1024,  # Fetch when at least 1KB accumulated
        )
        print("[Store] ✓ Connected to Kafka successfully!")
        
        print("[Store] Connecting to PostgreSQL...")
        conn = psycopg2.connect(
            dbname="kafka_db",
            user="kafka_user",
            password="kafka_password",
            host="localhost",
            port="5433",
        )
        conn.autocommit = False  # ✨ Set to False for batch processing
        cur = conn.cursor()
        print("[Store] ✓ Connected to PostgreSQL successfully!")
        
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS music_stream (
                event_id VARCHAR(50) PRIMARY KEY,
                user_id VARCHAR(50),
                country VARCHAR(10),
                device VARCHAR(20),
                artist VARCHAR(60),
                song_title VARCHAR(100),
                genre VARCHAR(30),
                action VARCHAR(20),
                duration_sec INT,
                played_sec INT,
                liked BOOLEAN,
                event_time TIMESTAMP
            );
            """
        )
        conn.commit()
        print("[Store] ✓ Table 'music_stream' ready.")
        
        print("[Store] 🎧 Listening for messages...\n")
        
        message_count = 0
        batch = []
        batch_size = 50  # ✨ Batch process 50 events at a time
        
        for message in music_consumer:
            try:
                data = message.value
                batch.append((
                    data["event_id"],
                    data["user_id"],
                    data["country"],
                    data["device"],
                    data["artist"],
                    data["song_title"],
                    data["genre"],
                    data["action"],
                    data["duration_sec"],
                    data["played_sec"],
                    data["liked"],
                    data["event_time"]
                ))
                
                # ✨ When batch is full, INSERT all at once
                if len(batch) >= batch_size:
                    insert_query = """
                        INSERT INTO music_stream 
                        (event_id, user_id, country, device, artist, song_title, genre, action, duration_sec, played_sec, liked, event_time)
                        VALUES %s
                        ON CONFLICT (event_id) DO NOTHING;
                    """
                    psycopg2.extras.execute_values(cur, insert_query, batch, page_size=batch_size)
                    conn.commit()
                    
                    message_count += len(batch)
                    print(f"[Store] ✓ Batch INSERT: {len(batch)} events (Total: {message_count})")
                    
                    # Print last event info
                    last_data = data
                    print(f"         Latest: {last_data['action']} by {last_data['artist']} | {last_data['song_title']}")
                    
                    batch = []  # Reset batch
                    
            except Exception as e:
                print(f"[Store ERROR] Failed to process message: {e}")
                conn.rollback()
                continue
                
    except Exception as e:
        print(f"[Store ERROR] {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        # ✨ Process remaining batch
        if batch:
            try:
                insert_query = """
                    INSERT INTO music_stream 
                    (event_id, user_id, country, device, artist, song_title, genre, action, duration_sec, played_sec, liked, event_time)
                    VALUES %s
                    ON CONFLICT (event_id) DO NOTHING;
                """
                psycopg2.extras.execute_values(cur, insert_query, batch)
                conn.commit()
                print(f"[Store] ✓ Final batch: {len(batch)} events")
            except:
                pass

if __name__ == "__main__":
    run_store()
