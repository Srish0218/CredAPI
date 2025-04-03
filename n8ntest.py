import pandas as pd
from datetime import datetime, timedelta
import pytz
from fetchData import fetch_data_softskill

# Set IST timezone
ist = pytz.timezone('Asia/Kolkata')

# Get today's date
date = (datetime.now(ist) - timedelta(days=0)).date()

# Fetch data
primary_info_df, transcript_df, transcriptchat_df, response = fetch_data_softskill(date)

# Define Excel file name with timestamp
file_name = f"SoftSkill_Data_{date}.xlsx"

# Save data to Excel with multiple sheets
with pd.ExcelWriter(file_name, engine="xlsxwriter") as writer:
    primary_info_df.to_excel(writer, sheet_name="Primary Info", index=False)
    transcript_df.to_excel(writer, sheet_name="Transcript", index=False)
    transcriptchat_df.to_excel(writer, sheet_name="Transcript Chat", index=False)

print(f"Excel file '{file_name}' created successfully with 3 sheets!")
