import time
from datetime import datetime, timedelta

import pandas as pd
import pytz
import requests
from fastapi import FastAPI

from ZulipMessenger import reportTranscriptGenerated, reportError, reportStatus
from analyseData import analyse_data_using_gemini_for_brcp, analyse_data_for_soft_skill
from fetchData import fetch_data_from_database, upload_cred_result_on_database, fetch_data_softskill, get_latest_uid, \
    is_latest_uid_present, INPUT_DATABASE

app = FastAPI()


def fetch_api_result(uid: str, max_retries=100, retry_delay=5):
    reportStatus(f"Generating transcripts for {uid}")
    """Fetch API result from external service with retry mechanism."""
    url = f"https://tmc.wyzmindz.com/CredASR/api/CredASR/FetchResultFromVocab?uploaded_id={uid}"
    headers = {"accept": "*/*"}

    for attempt in range(1, max_retries + 1):
        start_time = time.time()

        try:
            response = requests.post(url, headers=headers)  # Added timeout for reliability
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
        # Fetch data from the database
        df = fetch_data_from_database(uid)
        if df is None or df.empty:
            error_msg = "Failed to fetch data from the database or DataFrame is empty."
            reportError(error_msg)
            return {"status": "Failed", "message": error_msg}

        # Analyze data using Gemini model
        final_df = analyse_data_using_gemini_for_brcp(df)
        final_df.to_excel(f"CRED_BRCP_{uid}.xlsx")
        if not isinstance(final_df, pd.DataFrame) or final_df.empty or final_df is None:
            error_msg = "Data analysis failed. The output DataFrame is either missing or incorrect."
            reportError(error_msg)
            return {"status": "Failed", "message": error_msg}

        # Upload processed data to the database
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
    # uid = get_latest_uid()
    if uid:
        transmon_response = fetch_api_result(uid)
        gemini_response = generate_output_brcp(uid)
        status = {"TransmonResponse": transmon_response, "GeminiResponse": gemini_response}
        reportStatus(status)
    else:
        status = {"status": "Fetching latest Upload ID Failed", "message": "Upload Id not found"}
        reportError(status)
    return status


print("started")


def generate_output_softskill(date: str):
    responseSoftSkill = {}
    try:
        # Fetch data
        primaryInfo_df, transcript_df, transcriptChat_df, responseDB = fetch_data_softskill(date)
        responseSoftSkill['responseDB'] = responseDB

        # Function to check if a DataFrame is invalid
        def is_invalid_df(df):
            return df is None or df.empty or (len(df) == 1 and df.columns.tolist() == df.iloc[0].tolist())

        # Validate data
        if is_invalid_df(primaryInfo_df) or is_invalid_df(transcript_df) or is_invalid_df(transcriptChat_df):
            error = "❌ One or more dataframes are empty, None, or contain only a header row. Cannot proceed."
            reportError(error)
            responseSoftSkill["FetchError"] = error

        # Analyze data
        analysisResponse = analyse_data_for_soft_skill(primaryInfo_df, transcript_df, transcriptChat_df, date)
        responseSoftSkill["AnalysisResponse"] = analysisResponse

    except Exception as e:
        reportError(f"❌ Error in generate_output_softskill: {str(e)}")
        responseSoftSkill["ExceptionOccurred"] = e
    return responseSoftSkill


@app.get("/softskill")
def get_softskill_result():
    ist = pytz.timezone('Asia/Kolkata')
    date = (datetime.now(ist) - timedelta(days=1)).date()
    print("req date in IST:", date)
    reportStatus(f"Starting Softskill Parameter for {date}")
    softskill_response = generate_output_softskill(date)
    reportStatus(softskill_response)

    return {"database response": softskill_response}

