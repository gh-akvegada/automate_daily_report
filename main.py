# import pandas as pd
# import gspread
# import os 
# from google.oauth2.credentials import Credentials
# from google.auth.transport.requests import Request

# # SCOPES define access level: spreadsheet reading/writing
# SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']


# def get_cell_value(df, row_id, column_name):
#     row = df[df['row_id'] == row_id]
#     if row.empty:
#         return None
#     return row.iloc[0][column_name]


# def get_credentials():
#     if not os.path.exists('token.json'):
#         raise Exception("Token file not found. Please run 'generate_token.py' to run initial set-up. ")
    
#     creds = Credentials.from_authorized_user_file('token.json', SCOPES)

#     # Automatically refresh token if it's expired
#     if creds.expired and creds.refresh_token:
#         creds.refresh(Request())
#         with open('token.json', 'w') as token_file:
#             token_file.write(creds.to_json())

#     return creds


# creds = get_credentials()
# # STEP 2: Authorize gspread with the credentials
# gc = gspread.authorize(creds)

# sheet_name = "Akash's Copy of Instantaneous Capacity Calculator V2"
# # STEP 3: Open spreadsheet by name
# spreadsheet = gc.open(sheet_name)  # Replace with your Sheet name
# worksheet = spreadsheet.worksheet("Input and Dashboard")  # Or use .worksheet("SheetName")

# # STEP 4: Load data into a pandas DataFrame
# data = worksheet.get_all_values()
# df = pd.DataFrame(data)

# print(df)

# # print("columns : ",df.columns)

# # print("value : ",get_cell_value(df, 7, 2))




# # # STEP 5: Generate 5-character unique row_id column
# # def generate_id(existing):
# #     while True:
# #         rid = ''.join(random.choices(string.ascii_uppercase + string.digits, k=5))
# #         if rid not in existing:
# #             return rid

# # existing_ids = set()
# # df['row_id'] = [generate_id(existing_ids) for _ in range(len(df))]

# # # STEP 6: Update the spreadsheet with new data
# # worksheet.clear()  # Optional: wipe sheet first
# # worksheet.update([df.columns.values.tolist()] + df.values.tolist())

# # print("Sheet updated with row_id column.")

import pygsheets
from datetime import datetime, timedelta


# Authenticate using service account
gc = pygsheets.authorize(service_file='automate-daily-report-459818-784da1560980.json')

# Open the Google Sheet by name or URL
sheet = gc.open("Akash's Copy of Ops Huddle Scorecard 2025")  # OR: gc.open_by_url('URL_HERE')

# Select the 2nd worksheet
wks = sheet[1]

# Get all values as a list of lists
# data = wks.get_all_values()

# OR get it as a pandas DataFrame
df = wks.get_as_df()

today = datetime.today().strftime('%-m/%-d/%y')  # e.g., '5/20/25'


seg_df = df[df['Owner'] == 'Ian Advincula/James Grayson']


col_index = ""
row_index = 100

# Find the index of this column in df.columns
try:
    col_index = seg_df.columns.get_loc(today)
    
    # Convert index (0-based) to Excel/Sheets style (A, B, ..., AA, AB...)
    def colnum_to_letters(n):
        result = ''
        while n >= 0:
            result = chr(n % 26 + ord('A')) + result
            n = n // 26 - 1
        return result

    col_letter = colnum_to_letters(col_index)
    print(f"Column '{today}' is at index {today} => Google Sheets column: {col_letter}")

except KeyError:
    print(f"Column '{today}' not found in the DataFrame.")

# if today in seg_df.columns:
#     column_data = seg_df[today]
#     print(column_data)


# print(seg_df)

# Write a value to a specific cell
cell = f"{col_index}{row_index}"
wks.update_value("EN99", '90%')

# Update multiple rows/columns
# wks.update_values('A2:B3', [['Name', 'Age'], ['Alice', '25']])
