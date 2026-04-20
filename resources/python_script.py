import requests
import pyodbc
import time
from datetime import datetime

# ==============================
# FUNCTION: RUN PIPELINE
# ==============================
def run_pipeline():

    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,cardano,solana,ripple,polkadot,dogecoin,tron,litecoin,chainlink,avalanche-2,polygon,stellar,cosmos,monero,uniswap,vechain,tezos,filecoin,theta-token&vs_currencies=usd"

    # Fetch API
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print(f"API Error: {e}")
        return

    current_time = datetime.now()
    rows = []

    # Transform
    for crypto, value in data.items():
        try:
            price = float(value["usd"])

            if price <= 0:
                continue

            category = "High" if price > 1000 else "Low"
            change = "Stable"

            rows.append((crypto, price, category, change, current_time))

        except Exception as e:
            print(f"Skipping {crypto}: {e}")

    # Connect to Azure SQL (CREDENTIALS REMOVED)
    try:
        conn = pyodbc.connect(
            "DRIVER={ODBC Driver 18 for SQL Server};"
            "SERVER=YOUR_SERVER_NAME;"
            "DATABASE=YOUR_DATABASE_NAME;"
            "UID=YOUR_USERNAME;"
            "PWD=YOUR_PASSWORD;"
            "Encrypt=yes;"
            "TrustServerCertificate=no;"
            "Connection Timeout=30;"
        )
        cursor = conn.cursor()

    except Exception as e:
        print(f"DB Connection Error: {e}")
        return

    # Insert data
    try:
        for row in rows:
            cursor.execute("""
                INSERT INTO crypto_prices 
                (crypto_name, price, price_category, price_change, timestamp)
                VALUES (?, ?, ?, ?, ?)
            """, row)

        conn.commit()
        print(f"Inserted {len(rows)} records at {current_time}")

    except Exception as e:
        print(f"Insert Error: {e}")

    finally:
        cursor.close()
        conn.close()


# ==============================
# CONTROLLED LOOP
# ==============================
max_runs = 100
run_count = 0

while run_count < max_runs:
    print(f"Run {run_count + 1}/{max_runs}")

    run_pipeline()

    run_count += 1
    time.sleep(30)

print("Pipeline completed successfully")