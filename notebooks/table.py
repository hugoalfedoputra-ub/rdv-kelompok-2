import streamlit as st
import streamlit.components.v1 as components

st.markdown("## Prakiraan Cuaca Minggu Ini")

# Data prakiraan cuaca
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
                text-align: center; background-color: #000000; min-width: 150px; color: white;
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