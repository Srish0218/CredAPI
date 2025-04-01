import time
from datetime import datetime
import pandas as pd
import requests
from fastapi import FastAPI
from apscheduler.schedulers.background import BackgroundScheduler

from ZulipMessenger import reportTranscriptGenerated, reportError, reportStatus
from analyseData import analyse_data_using_gemini_for_brcp
from fetchData import fetch_data_from_database, upload_cred_result_on_database, is_latest_uid_present, INPUT_DATABASE

app = FastAPI()
scheduler = BackgroundScheduler()

def fetch_api_result(uid: str, max_retries=100, retry_delay=5):
    reportStatus(f"Generating transcripts for {uid}")
    url = f"https://tmc.wyzmindz.com/CredASR/api/CredASR/FetchResultFromVocab?uploaded_id={uid}"
    headers = {"accept": "*/*"}

    for attempt in range(1, max_retries + 1):
        start_time = time.time()
        try:
            response = requests.post(url, headers=headers)
            end_time = time.time()
            response_time = round(end_time - start_time, 2)

            if response.status_code in [200, 204]:
                reportTranscriptGenerated(uid)
                return {"status": "Success", "message": f"Request successful in {response_time} seconds"}

            elif 500 <= response.status_code < 600:
                reportError(f"Attempt {attempt}: Server error {response.status_code}, retrying...")
            else:
                reportError(f"HTTP {response.status_code}: {response.text}")
                return {"status": "Failed", "error_code": response.status_code, "message": response.text}

        except requests.Timeout:
            print(f"Attempt {attempt}: API request timed out. Retrying in {retry_delay} seconds...")
        except requests.ConnectionError:
            print(f"Attempt {attempt}: Connection error. Retrying in {retry_delay} seconds...")
        except requests.RequestException as e:
            print(f"Attempt {attempt}: Unexpected error - {e}")
            return {"status": "Error", "message": str(e)}

        if attempt < max_retries:
            time.sleep(retry_delay)

    error = {"status": "Failed", "message": f"API request failed after {max_retries} attempts"}
    reportError(error)
    return error

def generate_output_brcp(uid):
    """Fetch, analyze, and upload data with enhanced error handling."""
    try:
        df = fetch_data_from_database(uid)
        if df is None or df.empty:
            error_msg = "Failed to fetch data from the database or DataFrame is empty."
            reportError(error_msg)
            return {"status": "Failed", "message": error_msg}

        final_df = analyse_data_using_gemini_for_brcp(df)
        final_df.to_excel(f"CRED_BRCP_{uid}.xlsx")
        if not isinstance(final_df, pd.DataFrame) or final_df.empty or final_df is None:
            error_msg = "Data analysis failed. The output DataFrame is either missing or incorrect."
            reportError(error_msg)
            return {"status": "Failed", "message": error_msg}

        msg = upload_cred_result_on_database(final_df, uid)
        if "successfully" in msg.lower():
            return {"status": "Success", "message": msg}
        else:
            error_msg = f"Uploading failed: {msg}"
            reportError(error_msg)
            return {"status": "Uploading Failed", "message": msg}

    except Exception as e:
        error_msg = f"Unexpected error in generate_output_brcp: {e}"
        reportError(error_msg)
        return {"status": "Error", "message": str(e)}

@app.get("/")
def home():
    return {"message": "CRED API Server is running!"}

@app.get("/brcp")
def get_brcp_result():
    """Fetch result from external API and process data using Gemini."""
    status, uid = is_latest_uid_present(INPUT_DATABASE)

    if status:
        reportStatus(f"{uid} UID is already present in tPrimaryInfo.")
        print(f"{uid} UID is already present in tPrimaryInfo.")
    else:
        reportStatus(f"{uid} UID is NOT present in tPrimaryInfo.")
        print(f"{uid} UID is NOT present in tPrimaryInfo.")

    if uid:
        transmon_response = fetch_api_result(uid)
        gemini_response = generate_output_brcp(uid)
        status = {"TransmonResponse": transmon_response, "GeminiResponse": gemini_response}
        reportStatus(status)
    else:
        status = {"status": "Fetching latest Upload ID Failed", "message": "Upload Id not found"}
        reportError(status)

    return status

def run_brcp_periodically():
    """Function to run get_brcp_result() periodically."""
    print(f"Running BRCP process at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    get_brcp_result()

# Start the scheduler
scheduler.add_job(run_brcp_periodically, "interval", hours=1, minutes=5)
scheduler.start()

# Run the job once at startup
print("Running BRCP process immediately on startup...")
run_brcp_periodically()  # Call once immediately

print("Started BRCP scheduler...")
