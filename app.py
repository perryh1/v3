# v1.10
import streamlit as st
import pandas as pd
import requests

US_STATES = [
    "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut",
    "Delaware", "Florida", "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa",
    "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan",
    "Minnesota", "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada", "New Hampshire",
    "New Jersey", "New Mexico", "New York", "North Carolina", "North Dakota", "Ohio",
    "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota",
    "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington", "West Virginia",
    "Wisconsin", "Wyoming"
]

def get_coordinates(city, state):
    url = f"https://geocoding-api.open-meteo.com/v1/search?name={city}&count=10&language=en&format=json"
    response = requests.get(url).json()
    if "results" in response:
        for result in response["results"]:
            if result.get("country_code") == "US" and result.get("admin1") == state:
                return result["latitude"], result["longitude"]
    return None, None

# v1.11 - Snippet 1
# Replace your entire `calculate_temp_distribution` function with this updated version

@st.cache_data
def calculate_temp_distribution(lat, lon, target_temp):
    url = f"https://archive-api.open-meteo.com/v1/archive?latitude={lat}&longitude={lon}&start_date=2019-01-01&end_date=2024-01-01&hourly=temperature_2m"
    response = requests.get(url)
    response.raise_for_status()
    
    hourly_data = response.json().get('hourly', {}).get('temperature_2m', [])
    temps = [t for t in hourly_data if t is not None]
    
    total_records = len(temps)
    
    bins = {
        "< 10°C": 0, "10-16°C": 0, "16-18°C": 0, "18-20°C": 0,
        "20-22°C": 0, "22-24°C": 0, "24-26°C": 0, "26-28°C": 0,
        "28-30°C": 0, "30-32°C": 0, "32-34°C": 0, "34-36°C": 0,
        "36-38°C": 0, "38-40°C": 0, "40-42°C": 0, "42-44°C": 0,
        "> 45°C": 0
    }

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

    data_114kw = {
        "< 10°C": (28.4, 42.3), # Mapped to lowest known bin for baseline
        "10-16°C": (28.4, 42.3), "16-18°C": (30.1, 44.0), "18-20°C": (31.8, 45.7),
        "20-22°C": (33.5, 47.4), "22-24°C": (35.2, 49.1), "24-26°C": (36.9, 50.8),
        "26-28°C": (38.6, 52.5), "28-30°C": (40.3, 54.2), "30-32°C": (42.0, 55.9),
        "32-34°C": (43.7, 57.6), "34-36°C": (45.4, 59.3), "36-38°C": (47.1, 61.0),
        "38-40°C": (48.8, 62.7), "40-42°C": (50.5, 64.4), "42-44°C": (52.2, 66.1),
        "> 45°C": (53.0, 66.9)
    }

    data_95kw = {
        "< 10°C": (26.5, 38.1), # Mapped to lowest known bin for baseline
        "10-16°C": (26.5, 38.1), "16-18°C": (28.2, 39.8), "18-20°C": (29.9, 41.5),
        "20-22°C": (31.6, 43.2), "22-24°C": (33.3, 44.9), "24-26°C": (35.0, 46.6),
        "26-28°C": (36.7, 48.3), "28-30°C": (38.4, 50.0), "30-32°C": (40.1, 51.7),
        "32-34°C": (41.8, 53.4), "34-36°C": (43.5, 55.1), "36-38°C": (45.2, 56.8),
        "38-40°C": (46.9, 58.5), "40-42°C": (48.6, 60.2), "42-44°C": (50.3, 61.9),
        "> 45°C": (51.2, 62.7)
    }

    table_data = []
    cumulative_pct = 0.0
    pct_114_below_target = 0.0
    pct_95_below_target = 0.0
    
    for bin_range, hours in bins.items():
        pct = (hours / total_records) * 100 if total_records > 0 else 0
        cumulative_pct += pct
        
        inlet_114, outlet_114 = data_114kw.get(bin_range, ("-", "-"))
        inlet_95, outlet_95 = data_95kw.get(bin_range, ("-", "-"))

        # Dynamically accumulate percentages based on the UI dropdown
        if isinstance(inlet_114, (int, float)) and inlet_114 < target_temp:
            pct_114_below_target += pct
        if isinstance(inlet_95, (int, float)) and inlet_95 < target_temp:
            pct_95_below_target += pct

        table_data.append({
            "Ambient Temp": bin_range, 
            "Hours": hours, 
            "% of Time": f"{round(pct, 4)}%",
            "Cumulative %": f"{round(cumulative_pct, 4)}%",
            "114kW Inlet (°C)": inlet_114,
            "114kW Outlet (°C)": outlet_114,
            "95kW Inlet (°C)": inlet_95,
            "95kW Outlet (°C)": outlet_95
        })

    return pd.DataFrame(table_data), pct_114_below_target, pct_95_below_target
# Commit changes

# Streamlit App UI
st.set_page_config(page_title="Temperature APM", layout="wide")
st.title("Temperature APM & Cooling Estimate")

# Location Selection UI - Removed Country, adjusted columns
col1, col2 = st.columns(2)
with col1:
    state = st.selectbox("State", US_STATES, index=US_STATES.index("Texas"))
with col2:
    city = st.text_input("City", "Midland")

if st.button("Calculate APM"):
    with st.spinner(f"Finding coordinates for {city}, {state}..."):
        lat, lon = get_coordinates(city, state)
        
    if lat is None or lon is None:
        st.error(f"Could not find coordinates for '{city}, {state}'. Please check the spelling.")
    else:
        with st.spinner(f"Fetching 5-year historical data for {city}..."):
            df, pct_114, pct_95 = calculate_temp_distribution(lat, lon)
            st.dataframe(df, hide_index=True, use_container_width=True)
            
            # Additional logic for the sub-34°C metrics
            st.markdown("### Sub-34°C Inlet Oil Time Analysis")
            col_a, col_b, col_c = st.columns(3)
            col_a.metric("114kW Option (<34°C Inlet)", f"{pct_114:.2f}%")
            col_b.metric("95kW Option (<34°C Inlet)", f"{pct_95:.2f}%")
            col_c.metric("Delta", f"{(pct_95 - pct_114):.2f}%")

# Commit changes
