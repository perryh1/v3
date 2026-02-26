# v1.6
import streamlit as st
import pandas as pd
import requests

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

    # Hardcoded data from cooling estimate charts mapping upper bound of bins to Ambient ratings
    data_114kw = {
        "10-16°C": (28.4, 42.3), "16-18°C": (30.1, 44.0), "18-20°C": (31.8, 45.7),
        "20-22°C": (33.5, 47.4), "22-24°C": (35.2, 49.1), "24-26°C": (36.9, 50.8),
        "26-28°C": (38.6, 52.5), "28-30°C": (40.3, 54.2), "30-32°C": (42.0, 55.9),
        "32-34°C": (43.7, 57.6), "34-36°C": (45.4, 59.3), "36-38°C": (47.1, 61.0),
        "38-40°C": (48.8, 62.7), "40-42°C": (50.5, 64.4), "42-44°C": (52.2, 66.1),
        "> 45°C": (53.0, 66.9)
    }

    data_95kw = {
        "10-16°C": (26.5, 38.1), "16-18°C": (28.2, 39.8), "18-20°C": (29.9, 41.5),
        "20-22°C": (31.6, 43.2), "22-24°C": (33.3, 44.9), "24-26°C": (35.0, 46.6),
        "26-28°C": (36.7, 48.3), "28-30°C": (38.4, 50.0), "30-32°C": (40.1, 51.7),
        "32-34°C": (41.8, 53.4), "34-36°C": (43.5, 55.1), "36-38°C": (45.2, 56.8),
        "38-40°C": (46.9, 58.5), "40-42°C": (48.6, 60.2), "42-44°C": (50.3, 61.9),
        "> 45°C": (51.2, 62.7)
    }

    table_data = []
    for bin_range, hours in bins.items():
        pct = round((hours / total_records) * 100, 4) if total_records > 0 else 0
        
        inlet_114, outlet_114 = data_114kw.get(bin_range, ("-", "-"))
        inlet_95, outlet_95 = data_95kw.get(bin_range, ("-", "-"))

        table_data.append({
            "Ambient Temp": bin_range, 
            "Hours": hours, 
            "% of Time": f"{pct}%",
            "114kW Inlet (°C)": inlet_114,
            "114kW Outlet (°C)": outlet_114,
            "95kW Inlet (°C)": inlet_95,
            "95kW Outlet (°C)": outlet_95
        })

    return pd.DataFrame(table_data)

# Streamlit App UI
st.set_page_config(page_title="Midland Temp APM", layout="centered")
st.title("Midland, TX Temperature APM")

with st.spinner("Fetching and calculating historical data..."):
    df = calculate_midland_temp_distribution()
    st.dataframe(df, hide_index=True)

# Commit changes
