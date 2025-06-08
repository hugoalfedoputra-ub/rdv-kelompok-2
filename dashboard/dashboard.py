import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime

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

# Mock weather data
def get_weather_data():
    return {
        'current_temp': 24,
        'condition': 'Partly Cloudy',
        'humidity': 65,
        'wind_speed': 12,
        'pressure': 1013,
        'Wind_Speed': 6,
        'humidity': 66,
        'feels_like': 26
    }

def get_forecast_data():
    days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    conditions = ['Cloudy', 'Sunny', 'Rainy', 'Sunny', 'Partly Cloudy', 'Sunny', 'Cloudy']
    temps_high = [25, 28, 22, 30, 27, 29, 24]
    temps_low = [15, 18, 12, 20, 17, 19, 14]
    
    return pd.DataFrame({
        'Day': days,
        'Condition': conditions,
        'High': temps_high,
        'Low': temps_low
    })

def get_hourly_data():
    hours = [f"{i:02d}:00" for i in range(24)]
    feels_like = np.random.normal(26, 3, 24)
    temps = np.random.normal(24, 3, 24)
    humidity = np.random.normal(65, 10, 24)
    
    return pd.DataFrame({
        'Hour': hours,
        'Temperature': temps,
        'Humidity': humidity,
        'Feels_Like': feels_like
    })

# Sidebar
with st.sidebar:
    st.title("🌤️ Weather Dashboard")

    st.info("🔽Interval Selector")

    # Interval selector
    interval = st.selectbox(
        "Select Interval",
        ["Hourly", "15 Minutely"]
    )
    
    st.markdown("---")
    st.info("🔽Unit Selector")

    # Unit selector
    temp_units = st.radio(
        "Temperature Units",
        ["Celsius", "Fahrenheit"]
    )

    wind_units = st.radio(
        "Wind Units",
        ["Miles/Hour", "Km/Hour"] 
    )
    
    # Refresh button
    if st.button("🔄 Refresh Data"):
        st.rerun()
    
    st.markdown("---")

# Main content
st.markdown('<h1 class="main-header">Weather Dashboard</h1>', unsafe_allow_html=True)

# Get data
current_weather = get_weather_data()
forecast_data = get_forecast_data()
hourly_data = get_hourly_data()
hourly_data2 = get_hourly_data()

# Current weather section
col1, col2, col3 = st.columns([2, 1, 1])

with col1:
    st.markdown(f"""
    <div class="weather-card">
        <div class="location-text">Malang</div>
        <div class="temp-large">{current_weather['current_temp']}°</div>    
        <div style="margin-top: 10px;">
            💧 Humidity: {current_weather['humidity']}% /
            💨 Wind: {current_weather['wind_speed']} km/h
        </div>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.metric(
        label="Feels Like",
        value=f"{current_weather['feels_like']}°",
        delta=f"{current_weather['feels_like'] - current_weather['current_temp']}°"
    )
    st.metric(
        label="Wind Speed",
        value=current_weather['wind_speed'],
        delta="6 Km/h"
    )

with col3:
    st.metric(
        label="Pressure",
        value=f"{current_weather['pressure']} hPa"
    )
    st.metric(
        label="Humidity",
        value=f"{current_weather['humidity']} %"
    )

st.markdown("---")
st.subheader("📊 24-Hour Overview")

st.subheader("💧 Temperature")
    
# Temperature chart with horizontal scroll
fig_temp2 = px.line(
    hourly_data2, 
    x='Hour', 
    y=['Temperature', "Feels_Like"],
    title='Hourly Temperature (Scroll →)',
    color_discrete_map={
        "Temperature": "#ff6600",
        "FeelsLike": "#00ccff"
    }
)

# Configure layout for horizontal scrolling
fig_temp2.update_layout(
    height=350,
    xaxis=dict(
        rangeslider=dict(visible=True),  # Add range slider for navigation
        type="category",
        showgrid=True,
        gridcolor='rgba(255,255,255,0.2)',
        # Show only 5 ticks initially
        range=[0, 4],  # Show first 5 hours
        fixedrange=False  # Allow zooming and panning
    ),
    yaxis=dict(
        showgrid=True,
        gridcolor='rgba(255,255,255,0.2)'
    ),
    plot_bgcolor='rgba(0,0,0,0)',
    paper_bgcolor='rgba(0,0,0,0)',
    font=dict(color='white'),
    title_font=dict(color='white', size=14),
    margin=dict(l=40, r=40, t=40, b=80)  # Extra bottom margin for range slider
)

# Add annotation for scroll instruction
fig_temp2.add_annotation(
    text="💡 Drag the slider below or use mouse wheel to scroll through hours",
    xref="paper", yref="paper",
    x=0.5, y=-0.25,
    showarrow=False,
    font=dict(size=10, color="lightblue"),
    bgcolor="rgba(0,0,0,0.3)",
    bordercolor="rgba(255,255,255,0.2)",
    borderwidth=1
)

st.plotly_chart(fig_temp2, use_container_width=True)

# Hourly data and additional metrics
overview_col1, overview_col2 = st.columns(2)

with overview_col1:
    st.subheader("💧 Precipitation")
    
    # Temperature chart with horizontal scroll
    fig_temp = px.line(
        hourly_data, 
        x='Hour', 
        y='Temperature',
        title='Hourly Temperature (Scroll →)',
        color_discrete_sequence=["#bbd8ff"]
    )
    
    # Configure layout for horizontal scrolling
    fig_temp.update_layout(
        height=350,
        xaxis=dict(
            rangeslider=dict(visible=True),  # Add range slider for navigation
            type="category",
            showgrid=True,
            gridcolor='rgba(255,255,255,0.2)',
            # Show only 5 ticks initially
            range=[0, 4],  # Show first 5 hours
            fixedrange=False  # Allow zooming and panning
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor='rgba(255,255,255,0.2)'
        ),
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color='white'),
        title_font=dict(color='white', size=14),
        margin=dict(l=40, r=40, t=40, b=80)  # Extra bottom margin for range slider
    )
    
    # Add annotation for scroll instruction
    fig_temp.add_annotation(
        text="💡 Drag the slider below or use mouse wheel to scroll through hours",
        xref="paper", yref="paper",
        x=0.5, y=-0.25,
        showarrow=False,
        font=dict(size=10, color="lightblue"),
        bgcolor="rgba(0,0,0,0.3)",
        bordercolor="rgba(255,255,255,0.2)",
        borderwidth=1
    )
    
    st.plotly_chart(fig_temp, use_container_width=True)

with overview_col2:
    st.subheader("💧 Humidity Levels")
    
    # Humidity chart with horizontal scroll
    fig_humidity = px.bar(
        hourly_data,  # Show all hours instead of every 4th
        x='Hour', 
        y='Humidity',
        title='Hourly Humidity (Scroll →)',
        color='Humidity',
        color_continuous_scale='Blues'
    )
    
    # Configure layout for horizontal scrolling
    fig_humidity.update_layout(
        height=350,
        xaxis=dict(
            rangeslider=dict(visible=True),
            type="category",
            showgrid=True,
            gridcolor='rgba(255,255,255,0.2)',
            # Show only 5 ticks initially
            range=[0, 4],  # Show first 5 hours
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
        showlegend=False  # Hide colorbar legend to save space
    )
    
    # Add annotation for scroll instruction
    fig_humidity.add_annotation(
        text="💡 Drag the slider below or use mouse wheel to scroll through hours",
        xref="paper", yref="paper",
        x=0.5, y=-0.25,
        showarrow=False,
        font=dict(size=10, color="lightblue"),
        bgcolor="rgba(0,0,0,0.3)",
        bordercolor="rgba(255,255,255,0.2)",
        borderwidth=1
    )
    
    st.plotly_chart(fig_humidity, use_container_width=True)

st.markdown("---")

# Data cuaca untuk ditampilkan
weather_data = [
    {
        "image": "https://cdn2.iconfinder.com/data/icons/weather-flat-14/64/weather02-512.png",
        "day": "Senin",
        "temp": "30°C",
        "precip": "10%"
    },
    {
        "image": "https://cdn2.iconfinder.com/data/icons/weather-flat-14/64/weather02-512.png",
        "day": "Selasa",
        "temp": "28°C",
        "precip": "60%"
    },
    {
        "image": "https://cdn2.iconfinder.com/data/icons/weather-flat-14/64/weather02-512.png",
        "day": "Rabu",
        "temp": "31°C",
        "precip": "20%"
    }
]

data_cuaca = [
    {"hari": "Senin", "suhu": "30°C", "presipitasi": "10%"},
    {"hari": "Selasa", "suhu": "28°C", "presipitasi": "60%"},
    {"hari": "Rabu", "suhu": "31°C", "presipitasi": "20%"},
    {"hari": "Kamis", "suhu": "32°C", "presipitasi": "25%"},
    {"hari": "Jumat", "suhu": "29°C", "presipitasi": "50%"},
    {"hari": "Sabtu", "suhu": "27°C", "presipitasi": "70%"},
    {"hari": "Minggu", "suhu": "30°C", "presipitasi": "15%"},
]

# Icon cuaca
icon_url = "https://cdn2.iconfinder.com/data/icons/weather-flat-14/64/weather02-512.png"

# HTML untuk cards cuaca
html = '<div style="display: flex; overflow-x: auto; padding: 10px;-ms-overflow-style: none;scrollbar-width: none;">'

for cuaca in data_cuaca:
    html += f"""
    <div style="flex: 0 0 auto; border: 1px solid #ddd; padding: 10px; margin-right: 10px;
                text-align: center; background-color: #000000; min-width: 150px; font-family:'Source Sans Pro', sans-serif; color: white;
                border-radius: 8px;">
        <h4>{cuaca["hari"]}</h4>
        <img src="{icon_url}" width="80"><br>
        <p>Suhu: {cuaca["suhu"]}</p>
        <p>Presipitasi: {cuaca["presipitasi"]}</p>
    </div>
    """

html += "</div>"

# Tampilkan
components.html(html, height=300)

st.markdown("---")

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