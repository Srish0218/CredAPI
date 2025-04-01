from ZulipMessenger import reportStatus, reportError
from fetchData import is_latest_uid_present, INPUT_DATABASE
from main import fetch_api_result, generate_output_brcp


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

get_brcp_result()