
'''
    This script appends daily capacity data to the Oncology-Ops Huddle google sheet. This script is automatically run daily as a systemd service.

    It does this by: 
        1. Connecting to the Google sheet to append the data to.
        2. Connecting to DB which already has the required data.
        3. Filtering and fetching today's data from DB.
        4. Adding data to necessary cells in the google sheet.

'''

from dotenv import load_dotenv # pip install python-dotenv
import os
from sqlalchemy import create_engine, text
import pandas as pd 
import pygsheets
from datetime import datetime
import logging 

# Loading credentials from .env file 
load_dotenv()

DATABASE_NAME = os.getenv('DATABASE_NAME')
DATABASE_HOST = os.getenv('DATABASE_HOST')
DATABASE_PORT = os.getenv('DATABASE_PORT') 
DATABASE_WRITE_USER = os.getenv('DATABASE_WRITE_USER') 
DATABASE_WRITE_PASSWORD = os.getenv('DATABASE_WRITE_PASSWORD')
QUALER_DB_ENGINE = None 


GSHEET_LINE_ITEMS = [
    'NextSeq 550 Utilization',
    'NovaSeq 6000 Utilization',
    'NovaSeq X+ Utilization',
    'QiaSymphony Utilization',
    'BSM Onco Liquid - Avanti J-15R Centrifuge',
    'G360 CDX v2.11 STARlet Utilization',
    'G360 CDX v2.11 STAR Utilization',
    'G360 LDT STAR-PRE Utilization',
    'G360 LDT STAR-POST Utilization',
    'Reveal EP1 Pre-STAR Utilization',
    'Reveal EP1 Post STAR Utilization',
    'Tissue v2 AutoLys',
    'Tissue v2 RNA STAR-Pre',
    'Tissue v2 DNA STAR-Pre',
    'Tissue v2 STAR-Post',
    'Tissue v2 King Fisher - RNA',
    'Tissue v2 King Fisher - DNA',
    'Screening NovaSeq Utilization',
    'Screening QiaSymphony Utilization',
    'Screening EZ-Blood Utilization',
    'Screening STAR-PRE Utilization',
    'Screening STAR-POST Utilization',
    'Histology - Sakura embedding station',
    'Histology - Sakura Auto Stainer',
    'Histology - Dako Auto Stainer',
    'Histology - Leica Scanner',
    'Histology - Leica Microtome',
    'Histology - Olympus Microscope'
]



def connect_to_qualer_db():
    '''
        This function creates an engine to connect to DB. 
        Updates value of Global variable 'QUALER_DB_ENGINE'
        Returns -> None 
    '''
    logging.info('Connecting to DB.')
    try: 
        global QUALER_DB_ENGINE 
        QUALER_DB_ENGINE = create_engine(f"postgresql://{DATABASE_WRITE_USER}:{DATABASE_WRITE_PASSWORD}@{DATABASE_HOST}:{DATABASE_PORT}/{DATABASE_NAME}") 
        logging.info('Success.')

        return True 
    except Exception as e: 
        logging.error(f'DB connection UNSUCCESSFUL. {e}')
        return False 



def execute_qualer_db_query(query):
    '''
        This function exectues DB query provided to it. 

        Returns -> Result of the query as a df 
    ''' 
    query = text(query)
    # print(f"Calling execute_query function for query: {query}")
    data = pd.read_sql(query, QUALER_DB_ENGINE)
    
    return data



def fetch_input_data(today_date):
    '''
        This function fetches data to be input in the google sheet.

        Returns the data as a df. 
    '''

    query = f"""
                SELECT * 
                FROM "capacity"."silver_capacity_output"
                WHERE upload_date = '{today_date}'; 
            """

    df = execute_qualer_db_query(query)
    df['gsheet_line_item_index'] = df['gsheet_line_item'].str.strip()

    df = df.set_index('gsheet_line_item_index')


    return df



def connect_to_gsheet():
    '''
        This function is used to create a connection to the google sheet we are trying to update. 

        Returns: 
            1. True if connection is successful, else False
            2. worksheet if connection is successful, esle None 
            3. wokrsheet_df if connection is successful, esle None 
    '''
    
    logging.info('Connecting to Google Sheets.')
    try:
        gc = pygsheets.authorize(service_file='automate-daily-report-459818-784da1560980.json')         # Authenticate using service account

        spreadsheet = gc.open("Akash's Copy of Ops Huddle Scorecard 2025")  # OR: gc.open_by_url('URL_HERE') # Open the Google Sheet by name or URL

        wks = spreadsheet.worksheet('title', 'Oncology-Ops Huddle')
        # wks = spreadsheet[1] # Select the 2nd worksheet

        worksheet_df = wks.get_as_df()

        logging.info('Success.')
        return True, wks, worksheet_df 
    except Exception as e:
        logging.error(f"Google Sheets connection UNSUCCESSFUL. {e}")
        return False, None, None 



def main():

    # Create and configure logger
    logging.basicConfig(filename='automate_daily_report.log', 
                        filemode = "a", 
                        level=logging.INFO, 
                        format = '%(asctime)s [%(levelname)s] %(message)s', )

    gheets_connection_successful, wks, worksheet_df = connect_to_gsheet()
    
    if gheets_connection_successful is False:
        return 


    db_connection_successful = connect_to_qualer_db()

    if db_connection_successful is False:
        return 

    first_col = worksheet_df.columns[0] # Since first col doesn't have a name
    today_date = datetime.today()

    today_date = datetime(2025, 6, 4) # only for testing

    input_df = []
    data_fetch_successful = True 

    try: 
        logging.info("Fetching today's data from DB.")
        input_df = fetch_input_data(today_date.strftime('%Y-%m-%d'))
        logging.info("Success.")
    except Exception as e:
        logging.error(f"Data fetch UNSUCCESSFUL. {e}")
        data_fetch_successful = False 
    
    if data_fetch_successful is False:
        return 


    def get_row_index(gsheet_line_item):
        offset = 2         
        idx = worksheet_df.index[(worksheet_df[first_col].str.strip()) == gsheet_line_item].tolist()
        if len(idx) > 0:
            return idx[0] + offset 
        # print("NOT FOUND")
        return -1
    
    
    def get_col_index(column_name):
        col_index = worksheet_df.columns.get_loc(column_name)
        n = col_index
        result = ''
    
        # Convert index (0-based) to Excel/Sheets style (A, B, ..., AA, AB...)
        while n >= 0:
            result = chr(n % 26 + ord('A')) + result
            n = n // 26 - 1
        return result


    col = get_col_index(column_name = today_date.strftime('%-m/%-d/%y')) # Today's date's col. This is the column we want to append data to 
    owner_col = get_col_index(column_name = 'Owner')

    for item in GSHEET_LINE_ITEMS:
        row = get_row_index(item)
        if row == -1:
            logging.warning(f"Could not locate '{item}' in Google sheet.")
        else:
            try:
                input_data = input_df.loc[item, 'utilized_capacity']
                input_data = round((input_data * 100), 1)
                # print(f"Item : {item},    Data: {input_data},    Index: {row}")
                owner = (wks.cell(f"{owner_col}{row}").value).strip()
                if owner == "Ian Advincula/James Grayson":  # only update if owner is SEG 
                    wks.update_value(f"{col}{row}", f'{input_data}%')

            except Exception as e: 
                logging.warning(f"Data missing for '{item}' in DB.")



if __name__ == "__main__":
    main()
    logging.info("--------------------------------------------------------------------------------------------------------------")
