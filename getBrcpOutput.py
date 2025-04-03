from ZulipMessenger import reportStatus, reportError
from fetchData import is_latest_uid_present, INPUT_DATABASE
from main import fetch_api_result, generate_output_brcp, get_brcp_result
def get_brcp_result():
    """Fetch result from external API and process data using Gemini."""
    status, uid, created_on = is_latest_uid_present(INPUT_DATABASE)

    if status:
        # reportStatus(f"{uid} UID is already present in tPrimaryInfo.")
        print(f"{uid} UID is already present in tPrimaryInfo.")
        return {"status": "Success", "message": "NO new ID found"}
    else:
        reportStatus(f"{uid} UID is NOT present in tPrimaryInfo. Generating transcripts for {uid}")
        print(f"{uid} UID is NOT present in tPrimaryInfo.")
    # uid = "ETL_74027"
    if uid:
        transmon_response = fetch_api_result(uid)
        gemini_response = generate_output_brcp(uid, created_on)
        status = {"TransmonResponse": transmon_response, "GeminiResponse": gemini_response}
        reportStatus(status)
    else:
        status = {"status": "Fetching latest Upload ID Failed", "message": "Upload Id not found"}
        reportError(status)

    return status

print(get_brcp_result())
