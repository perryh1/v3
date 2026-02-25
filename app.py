# v1.2
import streamlit as st
import pandas as pd
import requests

# v1.3
@st.cache_data
def calculate_midland_temp_distribution():
    # Midland, TX coordinates
    lat, lon = 31.9973, -102.0779
    
    # Fetching historical data
    url = f"https://archive-api.open-meteo.com/v1/archive?latitude={lat}&longitude={lon}&start_date=2019-01-01&end_date=2024-01-01&hourly=temperature_2m"
    response = requests.get(url)
    response.raise_for_status()
    
    hourly_data = response.json().get('hourly', {}).get('temperature_2m', [])
    temps = [t for t in hourly_data if t is not None]
    
    total_records = len(temps)
    
    # Initialize threshold bins
    bins = {
        "< 10°C": 0, "10-16°C": 0, "16-18°C": 0, "18-20°C": 0,
        "20-22°C": 0, "22-24°C": 0, "24-26°C": 0, "26-28°C": 0,
        "28-30°C": 0, "30-32°C": 0, "32-34°C": 0, "34-36°C": 0,
        "36-38°C": 0, "38-40°C": 0, "40-42°C": 0, "42-44°C": 0,
        "> 45°C": 0
    }

    # Categorize temperatures
    for temp in temps:
        if temp < 10: bins["< 10°C"] += 1
        elif 10 <= temp < 16: bins["10-16°C"] += 1
        elif 16 <= temp < 18: bins["16-18°C"] += 1
        elif 18 <= temp < 20: bins["18-20°C"] += 1
        elif 20 <= temp < 22: bins["20-22°C"] += 1
        elif 22 <= temp < 24: bins["22-24°C"] += 1
        elif 24 <= temp < 26: bins["24-26°C"] += 1
        elif 26 <= temp < 28: bins["26-28°C"] += 1
        elif 28 <= temp < 30: bins["28-30°C"] += 1
        elif 30 <= temp < 32: bins["30-32°C"] += 1
        elif 32 <= temp < 34: bins["32-34°C"] += 1
        elif 34 <= temp < 36: bins["34-36°C"] += 1
        elif 36 <= temp < 38: bins["36-38°C"] += 1
        elif 38 <= temp < 40: bins["38-40°C"] += 1
        elif 40 <= temp < 42: bins["40-42°C"] += 1
        elif 42 <= temp <= 44: bins["42-44°C"] += 1
        elif temp > 45: bins["> 45°C"] += 1

    # Calculate percentages and output as a formatted table for Streamlit
    table_data = []
    for bin_range, hours in bins.items():
        pct = round((hours / total_records) * 100, 4) if total_records > 0 else 0
        table_data.append({
            "Temperature Range": bin_range, 
            "Hours": hours, 
            "Percentage": f"{pct}%"
        })

    return pd.DataFrame(table_data)

# Commit changes
