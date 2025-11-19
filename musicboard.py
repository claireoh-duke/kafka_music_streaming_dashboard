import time
from datetime import datetime
import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text

st.set_page_config(page_title="🎶 Real-Time Musicboard", layout="wide")
st.title("🎶 Real-Time Music Streaming Dashboard")

DATABASE_URL = "postgresql://kafka_user:kafka_password@localhost:5433/kafka_db"


@st.cache_resource
def get_engine(url: str):
    return create_engine(url, pool_pre_ping=True, pool_size=5, max_overflow=10)


engine = get_engine(DATABASE_URL)


# ✨ Add caching: auto-refresh with TTL
@st.cache_data(ttl=5)
def load_music_events(
    action_filter: str | None = None, limit: int = 400
) -> pd.DataFrame:
    query = "SELECT * FROM music_stream"
    params = {}
    if action_filter and action_filter != "All":
        query += " WHERE action = :act"
        params["act"] = action_filter
    query += " ORDER BY event_time DESC LIMIT :limit"
    params["limit"] = limit

    try:
        with engine.connect() as conn:
            df = pd.read_sql_query(text(query), con=conn, params=params)

        if "event_time" in df.columns:
            df["event_time"] = pd.to_datetime(df["event_time"])

        return df
    except Exception as e:
        st.error(f"Error loading data from database: {e}")
        return pd.DataFrame()


# ✨ Cache statistics calculation
@st.cache_data(ttl=5)
def compute_stats(df: pd.DataFrame):
    if df.empty:
        return None

    stats = {
        "total_events": len(df),
        "unique_users": df["user_id"].nunique(),
        "unique_tracks": df["song_title"].nunique(),
        "top_artist": df["artist"].mode()[0] if len(df) > 0 else "",
        "liked_tracks": df[df["liked"] == True]["song_title"].nunique(),
    }
    return stats


# ✨ Cache chart data preparation
@st.cache_data(ttl=5)
def prepare_chart_data(df: pd.DataFrame):
    if df.empty:
        return None, None, None, None

    # Top Songs (play only)
    plays = (
        df[df["action"] == "play"]
        .groupby(["song_title", "artist"])["event_id"]
        .count()
        .reset_index(name="plays")
        .sort_values("plays", ascending=False)
        .head(10)
    )

    # Genre, Country, Device
    genre_counts = (
        df.groupby("genre")["event_id"]
        .count()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
    )

    country_counts = df.groupby("country")["event_id"].count().reset_index(name="count")

    device_counts = df.groupby("device")["event_id"].count().reset_index(name="count")

    return plays, genre_counts, country_counts, device_counts


# Sidebar
action_options = ["All", "play", "skip", "like", "add_to_playlist"]
chosen_action = st.sidebar.selectbox("Filter by Action", action_options)
update_sec = st.sidebar.slider(
    "Refresh Interval (seconds)", min_value=2, max_value=20, value=5
)
display_limit = st.sidebar.number_input(
    "Events to Load", min_value=100, max_value=2000, value=400, step=100
)

# ✨ Manual Refresh
if st.sidebar.button("Manual Refresh"):
    st.cache_data.clear()
    st.rerun()

# ✨ Auto-refresh (remove while loop)
if "last_refresh" not in st.session_state:
    st.session_state.last_refresh = time.time()

current_time = time.time()
if current_time - st.session_state.last_refresh >= update_sec:
    st.session_state.last_refresh = current_time
    st.rerun()

# ✨ Load data (cached)
df_music = load_music_events(chosen_action, limit=int(display_limit))

if df_music.empty:
    st.warning("No music events. Waiting for data...")
    time.sleep(2)
    st.rerun()

# ✨ Calculate stats (cached)
stats = compute_stats(df_music)

if stats:
    st.subheader(f"Events: {stats['total_events']} (Action: {chosen_action})")

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Events", stats["total_events"])
    k2.metric("Unique Users", stats["unique_users"])
    k3.metric("Unique Tracks", stats["unique_tracks"])
    k4.metric("Top Artist", stats["top_artist"])
    k5.metric("Liked Tracks", stats["liked_tracks"])

# Recent Events
st.markdown("### Recent Music Events (Top 10)")
st.dataframe(df_music.head(10), use_container_width=True)

# ✨ Prepare chart data (cached)
plays, genre_counts, country_counts, device_counts = prepare_chart_data(df_music)

if plays is not None and not plays.empty:
    # ✨ Create charts (simplified settings)
    fig_top = px.bar(
        plays, x="song_title", y="plays", color="artist", title="🎵 Top Played Songs"
    )
    fig_top.update_layout(showlegend=True, height=400)

    fig_genre = px.pie(
        genre_counts, names="genre", values="count", title="Events by Genre"
    )
    fig_genre.update_layout(height=400)

    fig_country = px.pie(
        country_counts, names="country", values="count", title="Events by Country"
    )
    fig_country.update_layout(height=400)

    fig_device = px.pie(
        device_counts, names="device", values="count", title="Events by Device"
    )
    fig_device.update_layout(height=400)

    # ✨ Layout
    col1, col2 = st.columns(2)
    with col1:
        st.plotly_chart(fig_top, use_container_width=True, key="chart_top")
        st.plotly_chart(fig_genre, use_container_width=True, key="chart_genre")
    with col2:
        st.plotly_chart(fig_country, use_container_width=True, key="chart_country")
        st.plotly_chart(fig_device, use_container_width=True, key="chart_device")

st.markdown("---")
st.caption(
    f"Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} · Auto-refresh every {update_sec}s"
)

# ✨ Wait for auto-refresh
time.sleep(0.1)
