# v1.2
import streamlit as st
import pandas as pd
import requests

# Add the Streamlit cache decorator to prevent re-fetching on every UI interaction
@st.cache_data
def calculate_midland_temp_distribution():
    # [Insert your existing fetch, parsing, and precise binning logic here exactly as before]
    
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

# Streamlit App UI
st.set_page_config(page_title="Midland Temp APM", layout="centered")
st.title("Midland, TX Temperature APM")

with st.spinner("Fetching and calculating historical data..."):
    df = calculate_midland_temp_distribution()
    st.table(df)

# Commit changes
