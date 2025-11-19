# 🎶 Real-Time Music Streaming Analytics Pipeline

A complete real-time data pipeline simulating music streaming events (like Spotify) using **Kafka, PostgreSQL, and Streamlit**.  
This project demonstrates event streaming, data processing, and live dashboard visualization.

---

## 📺 Demo


## ✨ Features

- **🎵 Music Event Generator**
  - Generates realistic events: play, skip, like, add_to_playlist
  - Simulates 9 popular artists across 7 genres
  - Random user behavior from 8 countries
  - Multiple device types: iPhone, Android, Web, Windows, Mac

- **🚀 Optimized Data Pipeline**
  - Batch Producer: Sends events in batches (100 events/flush)
  - Compression: LZ4 for efficient data transfer
  - Bulk Insert Consumer: 50 records at once using `execute_values()`
  - Performance: 10-20x faster than naive implementation

- **📊 Real-Time Dashboard**
  - Live KPIs: total events, unique users, unique tracks, top artist, liked tracks
  - Top 10 played songs bar chart
  - Distribution by genre, country, device
  - Customizable filters and refresh intervals
  - Optimized with Streamlit's `@st.cache_data`

---

## 🚀 Quick Start

1. **Clone the Repository**
    ```
    git clone https://github.com/claireoh-duke/kafka_music_streaming_dashboard.git
    cd kafka_music_streaming_dashboard
    ```

2. **Install Python Dependencies**
    ```
    pip install -r requirements.txt
    ```

3. **Start Kafka & PostgreSQL**
    ```
    docker-compose up -d
    ```
    - Zookeeper (port 2181)
    - Kafka (port 9092)
    - PostgreSQL (port 5433)

4. **Run the Pipeline**  
   Open 3 separate terminals:
    - **Producer**
      ```
      python music_streamer.py
      ```
    - **Consumer**
      ```
      python music_store.py
      ```
    - **Dashboard**
      ```
      streamlit run music_dashboard.py
      ```

5. **Access the Dashboard**  
   Go to [http://localhost:8501](http://localhost:8501) in your browser.


