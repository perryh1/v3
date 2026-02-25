import sqlite3
import pandas as pd

# --- CONFIGURATION ---
DB_FILE = "api_iso_hubs_5yr.db"

def audit_database():
    try:
        conn = sqlite3.connect(DB_FILE)
        
        query = """
        SELECT 
            iso AS "ISO",
            location AS "Hub / Node",
            MIN(timestamp) AS "First Record",
            MAX(timestamp) AS "Last Record",
            COUNT(*) AS "Total Rows Captured"
        FROM historical_prices
        GROUP BY iso, location
        ORDER BY iso, location;
        """
        
        print(f"\n🔍 Scanning Database: {DB_FILE}...\n")
        df = pd.read_sql_query(query, conn)
        
        if df.empty:
            print("⚠️ The database exists but contains zero rows of data.")
            return

        df['First Record'] = pd.to_datetime(df['First Record']).dt.strftime('%Y-%m-%d %H:%M')
        df['Last Record'] = pd.to_datetime(df['Last Record']).dt.strftime('%Y-%m-%d %H:%M')
        
        print("✅ AUDIT COMPLETE. HERE IS EXACTLY WHAT YOU HAVE:\n")
        print(df.to_string(index=False))
        print("\n==========================================================================")
        print(f"Total Rows Across All ISOs: {df['Total Rows Captured'].sum():,}")
        print("==========================================================================\n")
        
        conn.close()
        
    except sqlite3.OperationalError:
        print(f"❌ ERROR: Could not find '{DB_FILE}'. Make sure you are in the correct folder.")
    except Exception as e:
        print(f"❌ ERROR: {e}")

if __name__ == "__main__":
    audit_database()
