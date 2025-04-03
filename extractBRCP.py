import time
import pandas as pd

from fetchData import max_retries, OUTPUT_DATABASE, get_connection, retry_delay


def fetch_data_from_database(date):
    """Fetch data from the database and return a DataFrame with retries."""
    for attempt in range(1, max_retries + 1):
        conn = get_connection(OUTPUT_DATABASE)
        if conn is None:
            continue

        try:
            query = """
                SELECT * 
FROM brcpData 
WHERE CONVERT(DATE, TRY_CAST(Today_Date AS DATETIME)) = ?  
ORDER BY Today_Date ASC;
            """
            df = pd.read_sql_query(query, conn, params=(date,))

            if df.empty:
                print("No data found for the given UID.")
                return None
            print(f"Data Fetching Success: {df.shape[0]} conversation IDs, columns: {list(df.columns)}")

            return df

        except Exception as e:
            print(f"[Attempt {attempt}/{max_retries}] Error fetching data: {e}")
            time.sleep(retry_delay * attempt)

        finally:
            conn.close()

    return None  # Return None if all retries fail
date = '02-04-2025'
df = fetch_data_from_database(date)
df.to_excel(f"CRED_BRCP_{date}.xlsx")