import os
import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px

from dotenv import load_dotenv
from datetime import datetime
from sqlite import sqliteModel
from prophetModel import ProphetWrapper

load_dotenv()
SQLITE_PATH = os.getenv("SQLITE_PATH", "weather.db")
HOUR_SLICING = 8
MINUTE_SLICING = 16
HOURLY_FORECAST_LENGTH = 12
MINUTELY_FORECAST_LENGTH = 16

db_model = sqliteModel(SQLITE_PATH)

# Page configuration
st.set_page_config(
    page_title="Weather Dashboard",
    page_icon="🌤️",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #e0ebf4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .weather-card {
        background: linear-gradient(135deg, #60A5FA 0%, #0b3b7a 100%);
        padding: 20px;
        border-radius: 15px;
        color: white;
        text-align: center;
        margin: 10px 0;
    }
    .temp-large {
        font-size: 3rem;
        font-weight: bold;
        margin: 0;
    }
    .location-text {
        font-size: 1.2rem;
        opacity: 0.9;
    }
    .metric-card{
        background: #d4e8fc;
        padding: 15px;
        border-radius: 10px;
        border-left: 4px solid #1f77b4
    }
    .metric-card p, h4{
        color: #000000
    }
    .expand-card {
        background: linear-gradient(135deg, #60A5FA 0%, #1E40AF 100%);
        border-radius: 12px;
        padding: 20px;
        margin: 10px 0;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        transition: all 0.4s ease;
        cursor: pointer;
        border: 1px solid rgba(255, 255, 255, 0.1);
        overflow: hidden;
        height: 120px;
        display: flex;
        flex-direction: column;
        justify-content: center;
    }
    .expand-card:hover {
        height: auto;
        min-height: 200px;
        transform: translateY(-8px);
        box-shadow: 0 12px 35px rgba(59, 130, 246, 0.4);
        border: 1px solid rgba(255, 255, 255, 0.3);
    }
    .expand-card h4 {
        color: #F8FAFC;
        margin: 0 auto;
        margin: 0 0 0 0;
        font-size: 20px;
        font-weight: 600;
    }
    .expand-card .value {
        color: #E2E8F0;
        margin: 0 0 15px 0;
        font-size: 20px;
        font-weight: 700;
    }
    .expand-card .explanation {
        color: #CBD5E1;
        font-size: 13px;
        line-height: 1.5;
        opacity: 0;
        max-height: 0;
        transition: all 0.4s ease;
        margin-top: 10px;
    }
    .expand-card:hover .explanation {
        opacity: 1;
        max-height: 200px;
    }
    .expand-card .unit {
        color: #94A3B8;
        font-size: 12px;
        font-weight: normal;
    }
    .scrolling-container {
        display: flex;
        overflow-x: auto;
        padding: 10px;
        gap: 10px;
    }
    .scrolling-item {
        min-width: 200px;
        background-color: #f0f0f0;
        padding: 10px;
        border-radius: 8px;
        flex-shrink: 0;
    }
</style>
""", unsafe_allow_html=True)

def get_hist_hourly_data():
    # Feels Like Sequencial Making
    feels_like = db_model.get_all_hourly_feelslike()
    hours = feels_like["timestamp"].dt.strftime("%H:%M").to_list()[-HOUR_SLICING:]
    feelslike_historical = feels_like["feels_like_c"].to_list()[-HOUR_SLICING:]
    # Forecast + Concate
    train = feels_like.rename(columns={"timestamp":"ds", "feels_like_c":"y"})
    feelslike_model = ProphetWrapper()
    feelslike_model.fit(train)
    predict = feelslike_model.predict(periods=HOURLY_FORECAST_LENGTH)
    predict_value = predict["yhat"].to_list()
    feelslike_final = feelslike_historical + predict_value
    hours = hours + predict["ds"].dt.strftime("%H:%M").to_list()
    normalized_hours = [
    datetime.strptime(t, "%H:%M").replace(minute=0).strftime("%H:%M")
    for t in hours
    ]

    # Temperature Sequencial Making
    temps = db_model.get_all_hourly_temperature()
    temp_historical = temps["temperature_c"].to_list()[-HOUR_SLICING:]
    # Forecast + Concate
    train = temps.rename(columns={"timestamp":"ds", "temperature_c":"y"})
    temp_model = ProphetWrapper()
    temp_model.fit(train)
    predict = temp_model.predict(periods=HOURLY_FORECAST_LENGTH)
    predict_value = predict["yhat"].to_list()
    temp_final = temp_historical + predict_value

    # Humidity Sequencial Making
    humidity = db_model.get_all_hourly_humidity()
    humidity_historical = humidity["humidity_pct"].to_list()[-HOUR_SLICING:]
    # Forecast + Concate
    train = humidity.rename(columns={"timestamp":"ds", "humidity_pct":"y"})
    humidity_model = ProphetWrapper()
    humidity_model.fit(train)
    predict = humidity_model.predict(periods=HOURLY_FORECAST_LENGTH)
    predict_value = predict["yhat"].to_list()
    humidity_final = humidity_historical + predict_value

    # Precipitation Sequencial Making
    precipitation = db_model.get_hourly_precipitation()
    precipitation_historical = precipitation["precip_mm"].to_list()[-HOUR_SLICING:]
    # Forecast + Concate
    train = precipitation.rename(columns={"timestamp":"ds", "precip_mm":"y"})
    precipitation_model = ProphetWrapper()
    precipitation_model.fit(train)
    predict = precipitation_model.predict(periods=HOURLY_FORECAST_LENGTH)
    predict_value = predict["yhat"].to_list()
    precipitation_final = precipitation_historical + predict_value

    return pd.DataFrame({
        'Hour': normalized_hours,
        'Temperature': temp_final,
        'Humidity': humidity_final,
        'Feels_Like': feelslike_final,
        "Precipitation" : precipitation_final
    })

def round_down_to_nearest_15(t):
    dt = datetime.strptime(t, "%H:%M")
    minute = (dt.minute // 15) * 15
    return dt.replace(minute=minute, second=0).strftime("%H:%M")

def get_hist_quarterly_data():
    # Feels Like Sequencial Making
    feels_like = db_model.get_all_quarter_feelslike()
    feels_like = feels_like.groupby("timestamp", as_index=False).mean()
    hours = feels_like["timestamp"].dt.strftime("%H:%M").to_list()[-MINUTE_SLICING:]
    feelslike_historical = feels_like["feelslike_c"].to_list()[-MINUTE_SLICING:]
    # Forecast + Concate
    train = feels_like.rename(columns={"timestamp":"ds", "feelslike_c":"y"})
    feelslike_model = ProphetWrapper()
    feelslike_model.fit(train)
    predict = feelslike_model.predict(periods=MINUTELY_FORECAST_LENGTH)
    predict_value = predict["yhat"].to_list()
    feelslike_final = feelslike_historical + predict_value
    hours = hours + predict["ds"].dt.strftime("%H:%M").to_list()
    normalized_hours = [round_down_to_nearest_15(t) for t in hours]

    # Temperature Sequencial Making
    temps = db_model.get_all_quarter_temperature()
    temps = temps.groupby("timestamp", as_index=False).mean()
    temp_historical = temps["temp_c"].to_list()[-MINUTE_SLICING:]
    # Forecast + Concate
    train = temps.rename(columns={"timestamp":"ds", "temp_c":"y"})
    temp_model = ProphetWrapper()
    temp_model.fit(train)
    predict = temp_model.predict(periods=MINUTELY_FORECAST_LENGTH)
    predict_value = predict["yhat"].to_list()
    temp_final = temp_historical + predict_value

    # Humidity Sequencial Making
    humidity = db_model.get_all_quarter_humidity()
    humidity = humidity.groupby("timestamp", as_index=False).mean()
    humidity_historical = humidity["humidity"].to_list()[-MINUTE_SLICING:]
    # Forecast + Concate
    train = humidity.rename(columns={"timestamp":"ds", "humidity":"y"})
    humidity_model = ProphetWrapper()
    humidity_model.fit(train)
    predict = humidity_model.predict(periods=MINUTELY_FORECAST_LENGTH)
    predict_value = predict["yhat"].to_list()
    humidity_final = humidity_historical + predict_value

    # Precipitation Sequencial Making
    precipitation = db_model.get_quarter_precipitation()
    precipitation = precipitation.groupby("timestamp", as_index=False).mean()
    precipitation_historical = precipitation["precip_mm"].to_list()[-MINUTE_SLICING:]
    # Forecast + Concate
    train = precipitation.rename(columns={"timestamp":"ds", "precip_mm":"y"})
    precipitation_model = ProphetWrapper()
    precipitation_model.fit(train)
    predict = precipitation_model.predict(periods=MINUTELY_FORECAST_LENGTH)
    predict_value = predict["yhat"].to_list()
    precipitation_final = precipitation_historical + predict_value

    return pd.DataFrame({
        'Hour': normalized_hours,
        'Temperature': temp_final,
        'Humidity': humidity_final,
        'Feels_Like': feelslike_final,
        "Precipitation" : precipitation_final
    })

def make_data_card(slicing: int, mode: str = "quarter"):
    if mode == "quarter":
        precipitation = db_model.get_quarter_precipitation()
        precipitation = precipitation.groupby("timestamp", as_index=False).mean()
        precipitation_historical = precipitation["precip_mm"].to_list()[-slicing:]

        temps = db_model.get_all_quarter_temperature()
        temps = temps.groupby("timestamp", as_index=False).mean()
        temp_historical = temps["temp_c"].to_list()[-slicing:]

        hours = temps["timestamp"].dt.strftime("%H:%M").to_list()[-slicing:]
        icons_df = db_model.get_quarter_icon()
        icons = icons_df["icon"].to_list()[-slicing:]
        
    elif mode == "hour":
        temps = db_model.get_all_hourly_temperature()
        temp_historical = temps["temperature_c"].to_list()[-slicing:]
        hours = temps["timestamp"].dt.strftime("%H:%M").to_list()[-slicing:]

        precipitation = db_model.get_hourly_precipitation()
        precipitation_historical = precipitation["precip_mm"].to_list()[-slicing:]

        icons_df = db_model.get_hourly_icon()
        merged = pd.merge(temps, icons_df, on="timestamp", how="inner")
        icons = merged["icon"].to_list()[-slicing:]
    else:
        raise ValueError("Mode harus 'quarter' atau 'hour'.")
    
    data_card = pd.DataFrame({
        "Hour":hours,
        "Temperature":temp_historical,
        "Precipitation":precipitation_historical,
        "Icon":icons
    })

    return data_card.to_dict(orient="records")

def create_chart_with_slider(data, x_col, y_cols, title, colors, chart_type="line", current_time_index=None):
    """Create a chart with proper slider positioning"""
    
    if chart_type == "line":
        if isinstance(y_cols, list) and len(y_cols) > 1:
            # Multiple lines - create clean data with only needed columns
            plot_data = data[[x_col] + y_cols].copy()
            fig = px.line(
                plot_data,
                x=x_col,
                y=y_cols,
                title=title,
                color_discrete_map=colors
            )
        else:
            # Single line
            y_col = y_cols[0] if isinstance(y_cols, list) else y_cols
            fig = px.line(
                data,
                x=x_col,
                y=y_col,
                title=title,
                color_discrete_sequence=[colors.get(y_col, "#1f77b4")]
            )
    else:  # bar chart
        y_col = y_cols[0] if isinstance(y_cols, list) else y_cols
        fig = px.bar(
            data,
            x=x_col,
            y=y_col,
            title=title,
            color=y_col,
            color_continuous_scale='Blues'
        )
    
    # Add "Now" marker if current_time_index is provided
    if current_time_index is not None:
        fig.add_vline(
            x=current_time_index,
            line_dash="dash",
            line_color="yellow",
            annotation_text="Now",
            annotation_position="top"
        )
    
    # Calculate slider range around current time
    if current_time_index is not None:
        start_range = max(0, current_time_index - 2)
        end_range = min(len(data) - 1, current_time_index + 3)
    else:
        start_range = 0
        end_range = min(4, len(data) - 1)
    
    # Configure layout
    fig.update_layout(
        height=350,
        xaxis=dict(
            rangeslider=dict(visible=True),
            type="category",
            showgrid=True,
            gridcolor='rgba(255,255,255,0.2)',
            range=[start_range, end_range],
            fixedrange=False
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor='rgba(255,255,255,0.2)'
        ),
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color='white'),
        title_font=dict(color='white', size=14),
        margin=dict(l=40, r=40, t=40, b=80),
        showlegend=(isinstance(y_cols, list) and len(y_cols) > 1)
    )
    
    # Add scroll instruction
    fig.add_annotation(
        text="💡 Drag the slider below or use mouse wheel to scroll through time",
        xref="paper", yref="paper",
        x=0.5, y=-0.25,
        showarrow=False,
        font=dict(size=10, color="lightblue"),
        bgcolor="rgba(0,0,0,0.3)",
        bordercolor="rgba(255,255,255,0.2)",
        borderwidth=1
    )
    
    return fig

# Sidebar
with st.sidebar:
    st.title("🌤️ Weather Dashboard")

    st.info("🔽Interval Selector")

    # Interval selector
    interval = st.selectbox(
        "Select Interval",
        ["15 Minutely", "Hourly"]
    )
    
    # Refresh button
    if st.button("🔄 Refresh Data"):
        st.rerun()
    
    st.markdown("---")

# Main content
st.markdown('<h1 class="main-header">Weather Dashboard</h1>', unsafe_allow_html=True)

current_time = datetime.now()

# Get data
if interval == "Hourly":
    current_weather = db_model.get_recent_hourly_weather()
    sequence_data = get_hist_hourly_data()
    weather_card_data = make_data_card(slicing=HOUR_SLICING, mode="hour")
    current_time_index = HOUR_SLICING - 1
elif interval == "15 Minutely":
    current_weather = db_model.get_recent_quarterly_weather()
    sequence_data = get_hist_quarterly_data()
    weather_card_data = make_data_card(slicing=MINUTE_SLICING, mode="quarter")
    current_time_index = MINUTE_SLICING - 1

# Current weather section
col1, col2, col3 = st.columns([2, 1, 1])

with col1:
    st.markdown(f"""
    <div class="weather-card">
        <div class="location-text" style="font-size: 1.4rem; font-weight: 600;">Malang</div>
        <div style="font-size: 0.85rem; opacity: 0.7; margin-bottom: 15px; font-weight: 300;">
            {current_time.strftime("%A, %d %B %Y")} • {current_time.strftime("%H:%M")}
        </div>
        <div class="temp-large" style="font-size: 3.5rem; margin: 15px 0;">{current_weather['temp_c']}°</div>    
        <div style="margin-top: 15px; font-size: 1.1rem; font-weight: 500;">
            💧 Humidity: {current_weather['humidity']}% /
            💨 Wind: {current_weather['wind_kph']:.2f} km/h
        </div>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.metric(
        label="Feels Like",
        value=f"{current_weather['feelslike_c']}°",
        delta=f"{(current_weather['feelslike_c'] - current_weather['temp_c']):.2f}°"
    )
    st.metric(
        label="Wind Gust",
        value=current_weather['gust_kph'],
        delta="6 Km/h"
    )

with col3:
    st.metric(
        label="Pressure",
        value=f"{current_weather['pressure']} hPa"
    )
    st.metric(
        label="Cloud Coverage",
        value=f"{current_weather['humidity']} %"
    )

st.markdown("---")
st.subheader("📊 24-Hour Overview")

# Main temperature chart with both Temperature and Feels Like
st.subheader("🌡️ Temperature vs Feels Like")
fig_temp_main = create_chart_with_slider(
    sequence_data,
    x_col='Hour',
    y_cols=['Temperature', 'Feels_Like'],
    title='Temperature & Feels Like (Scroll →)',
    colors={
        "Temperature": "#ff6600",
        "Feels_Like": "#00ccff"
    },
    current_time_index=current_time_index
)
st.plotly_chart(fig_temp_main, use_container_width=True)

# Secondary charts
overview_col1, overview_col2 = st.columns(2)

with overview_col1:
    st.subheader("🌧️ Precipitation")
    fig_precip = create_chart_with_slider(
        sequence_data,
        x_col='Hour',
        y_cols=['Precipitation'],
        title='Precipitation Forecast (Scroll →)',
        colors={"Precipitation": "#4A90E2"},
        current_time_index=current_time_index
    )
    st.plotly_chart(fig_precip, use_container_width=True)

with overview_col2:
    st.subheader("💧 Humidity Levels")
    fig_humidity = create_chart_with_slider(
        sequence_data,
        x_col='Hour',
        y_cols=['Humidity'],
        title='Humidity Levels (Scroll →)',
        colors={"Humidity": "#1E88E5"},
        chart_type="bar",
        current_time_index=current_time_index
    )
    st.plotly_chart(fig_humidity, use_container_width=True)

st.markdown("---")

# Weather cards
st.subheader("🕐 Recent Weather Timeline")
html = '<div style="display: flex; overflow-x: auto; padding: 10px;-ms-overflow-style: none;scrollbar-width: none;">'

for card_data in weather_card_data:
    html += f"""
    <div style="flex: 0 0 auto; border: 1px solid #ddd; padding: 10px; margin-right: 10px;
                text-align: center; background-color: #000000; min-width: 150px; font-family:'Source Sans Pro', sans-serif; color: white;
                border-radius: 8px;">
        <h4>{card_data["Hour"]}</h4>
        <img src="{card_data["Icon"]}" width="80"><br>
        <p>Temp : {card_data["Temperature"]:.2f}°C</p>
        <p>Precip : {card_data["Precipitation"]:.2f}mm</p>
    </div>
    """

html += "</div>"
components.html(html, height=300)

st.markdown("---")

# Information cards
info_col1, info_col2 = st.columns(2)

with info_col1:
    st.markdown("""
    <div class="expand-card">
        <h4>🌪️ Wind Gust</h4>
        <div class="explanation">
            <strong>Wind Gust</strong> adalah kecepatan angin tertinggi yang tercatat dalam periode waktu tertentu (biasanya 3 detik). 
            Berbeda dengan kecepatan angin rata-rata, wind gust menunjukkan lonjakan angin sesaat yang bisa lebih kuat. 
            Penting untuk aktivitas outdoor dan penerbangan.
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div class="expand-card">
        <h4>🌫️ Cloud Cover</h4>
        <div class="explanation">
            <strong>Cloud Cover</strong> mengukur persentase langit yang tertutup awan. 
            0% = langit cerah, 100% = langit tertutup sepenuhnya. 
            Mempengaruhi suhu, radiasi matahari, dan kemungkinan hujan. 
            Nilai rendah seperti ini menandakan cuaca cerah.
        </div>
    </div>
    """, unsafe_allow_html=True)

with info_col2:
    st.markdown("""
    <div class="expand-card">
        <h4>🌡️ Feels Like Temperature</h4>
        <div class="explanation">
            <strong>Feels Like</strong> atau "Heat Index" adalah suhu yang dirasakan tubuh dengan mempertimbangkan 
            kelembaban udara dan kecepatan angin. Bisa berbeda dengan suhu aktual karena kelembaban tinggi 
            membuat tubuh merasa lebih panas, sedangkan angin membuat terasa lebih sejuk.
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div class="expand-card">
        <h4>📊 Atmospheric Pressure</h4>
        <div class="explanation">
            <strong>Tekanan Atmosfer</strong> diukur dalam hectopascal (hPa). 
            Tekanan normal sekitar 1013 hPa. Tekanan tinggi = cuaca cerah, 
            tekanan rendah = kemungkinan hujan/badai. 
            Perubahan tekanan membantu prediksi cuaca dan mempengaruhi kesehatan beberapa orang.
        </div>
    </div>
    """, unsafe_allow_html=True)

# Footer
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: #666; padding: 20px;'>
        <p>Weather Dashboard • Last updated: {} • Data refreshes every 15 minutes</p>
    </div>
    """.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
    unsafe_allow_html=True
)