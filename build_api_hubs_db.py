import pandas as pd
import sqlite3
import gridstatusio
from datetime import datetime
import time

# --- CONFIGURATION ---
API_KEY = "17fd6eb144fe46afa0c0894453ba867d"
DB_NAME = "api_iso_hubs_1yr.db"
YEARS_BACK = 1

# Trimmed to the "Core 8"
ISO_API_MAPPINGS = {
    "ERCOT": {
        "dataset": "ercot_spp_real_time_15_min",
        "node_col": "location",
        "price_col": "spp",
        "locations": ["HB_WEST", "HB_NORTH"]
    },
    "SPP": {
        "dataset": "spp_lmp_real_time_5_min", 
        "node_col": "location",
        "price_col": "lmp",
        "locations": ["SPP_NORTH_HUB", "SPP_SOUTH_HUB"] 
    },
    "CAISO": {
        "dataset": "caiso_lmp_real_time_5_min",
        "node_col": "location",
        "price_col": "lmp",
        "locations": ["TH_NP15_GEN-APND", "TH_SP15_GEN-APND"]
    },
    "PJM": {
        "dataset": "pjm_lmp_real_time_5_min",
        "node_col": "location",
        "price_col": "lmp",
        "locations": ["WESTERN HUB"]
    },
    "NYISO": {
        "dataset": "nyiso_lmp_real_time_5_min",
        "node_col": "location",
        "price_col": "lmp",
        "locations": ["HUD VL"]
    },
    "MISO": {
        "dataset": "miso_lmp_real_time_5_min",
        "node_col": "location",
        "price_col": "lmp",
        "locations": ["INDIANA.HUB"]
    }
}

def setup_database():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS historical_prices (
            timestamp DATETIME,
            iso TEXT,
            location TEXT,
            price REAL,
            UNIQUE(timestamp, iso, location)
        )
    ''')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_query ON historical_prices(iso, location, timestamp)')
    conn.commit()
    return conn

def get_smart_resume_date(conn, iso, loc, default_start):
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(timestamp) FROM historical_prices WHERE iso=? AND location=?", (iso, loc))
    result = cursor.fetchone()[0]
    
    if result:
        latest_db_time = pd.to_datetime(result, utc=True)
        return latest_db_time + pd.Timedelta(minutes=1)
    
    return default_start

def fetch_and_store_data(conn):
    client = gridstatusio.GridStatusClient(api_key=API_KEY)
    end_date = pd.Timestamp.now(tz="US/Central").floor('D')
    global_start_date = end_date - pd.Timedelta(days=365 * YEARS_BACK)
    
    print(f"=====================================================")
    print(f" INITIATING 1-YEAR CORE 8 DATA PULL (v1.5 SDK)")
    print(f" Target Lookback: {YEARS_BACK} Year")
    print(f"=====================================================\n")

    for iso_name, metadata in ISO_API_MAPPINGS.items():
        dataset_id = metadata["dataset"]
        node_col = metadata["node_col"]
        price_col = metadata["price_col"]
        
        for loc in metadata["locations"]:
            print(f"\n   📍 Target Hub: {loc} ({iso_name})")
            
            current_date = get_smart_resume_date(conn, iso_name, loc, global_start_date)
            
            if current_date >= end_date:
                print(f"      ✓ Data fully up-to-date locally. Skipping API call.")
                continue
                
            print(f"      🔄 Fetching API data from: {current_date.strftime('%Y-%m-%d %H:%M')}")
            
            while current_date < end_date:
                chunk_end = min(current_date + pd.Timedelta(days=30), end_date)
                
                try:
                    df = client.get_dataset(
                        dataset=dataset_id,
                        start=current_date,
                        end=chunk_end,
                        filter_column=node_col,
                        filter_value=loc,
                        verbose=False
                    )
                    
                    if df is not None and not df.empty:
                        # Find time column
                        time_col = "Interval Start" if "Interval Start" in df.columns else df.columns[0]
                        for col in df.columns:
                            if col.lower() in ["interval_start_utc", "interval start", "time"]:
                                time_col = col
                                break
                        
                        # Find price column
                        actual_price_col = price_col
                        if price_col not in df.columns:
                            for col in df.columns:
                                if col.lower() == price_col.lower() or "price" in col.lower() or "lmp" in col.lower() or "spp" in col.lower():
                                    actual_price_col = col
                                    break

                        db_df = pd.DataFrame({
                            'timestamp': pd.to_datetime(df[time_col], utc=True),
                            'iso': iso_name,
                            'location': loc,
                            'price': pd.to_numeric(df[actual_price_col], errors='coerce')
                        })
                        
                        db_df = db_df.dropna(subset=['price'])
                        
                        db_df.to_sql('historical_prices_temp', conn, if_exists='replace', index=False)
                        conn.execute('''
                            INSERT OR IGNORE INTO historical_prices (timestamp, iso, location, price)
                            SELECT timestamp, iso, location, price FROM historical_prices_temp
                        ''')
                        conn.commit()
                        print(f"      ✓ Downloaded & Saved: {current_date.strftime('%Y-%m-%d')} to {chunk_end.strftime('%Y-%m-%d')}")
                        
                except Exception as e:
                    print(f"      ⚠️ API Error ({current_date.strftime('%Y-%m-%d')}): {e}")
                    if "403" in str(e) or "limit reached" in str(e).lower() or "Unauthorized" in str(e):
                        print("\n⛔ CRITICAL: API Quota Limit Reached. Terminating script.")
                        return
                
                current_date = chunk_end
                time.sleep(1.0) 

    print("\n✅ API Database Core 8 build complete!")

if __name__ == "__main__":
    db_conn = setup_database()
    fetch_and_store_data(db_conn)
    db_conn.close()
