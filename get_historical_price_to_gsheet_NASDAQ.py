### Get Historical Stock Price ###
# Google Sheet Link: https://docs.google.com/spreadsheets/d/1imdvzBE58lMf6TIzHG0pmGYEoQbsJ1ai17o7yw_Bfm4/edit#gid=0

import time
import pandas as pd
import requests
import gspread
from google.oauth2.service_account import Credentials

# Create a scope for Google Sheets API
scope = ['https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive']

# Use the key to authenticate and create a client object
credentials = Credentials.from_service_account_file('/kaggle/input/dataset/service_account.json', scopes=scope)
client = gspread.authorize(credentials)

# Open the Google Sheet by its key
# sheet_key = '1imdvzBE58lMf6TIzHG0pmGYEoQbsJ1ai17o7yw_Bfm4'    # 1~330 Tickers
sheet_key = '14RabAgDXNixuG6NSNQ-UxGmQjkI2ehjdcaGjAq94sus'    # 331~
sheet = client.open_by_key(sheet_key)

# Load the csv file from the uploaded data
df_stocks = pd.read_csv('/kaggle/input/dataset/stock_tickers.csv')

root_url = 'https://financialmodelingprep.com/api/v3/historical-price-full/'
apikey = 'qbYcpSiayLUNXFlhXQLseObPPG7FcWgU'

# Iterate over tickers
for index, row in df_stocks[330:11468].iterrows():
    ticker = row['Exch_NASDAQ']

    # Check if the sheet exists
    worksheet = None
    for existing_sheet in sheet.worksheets():
        if existing_sheet.title == ticker:
            worksheet = existing_sheet
            break

    if worksheet is None:
        worksheet = sheet.add_worksheet(title=ticker, rows="1", cols="11")

    # Request API
    url = f"{root_url}{ticker}?from=1980-01-01&apikey={apikey}"
    try:
        res = requests.get(url)
        res_json = res.json()
        print(f"No: {index + 1} ===> Completed getting JSON from ticker: {ticker}")
    except Exception as e:
        print(f"Error fetching data for ticker: {ticker}. Exception: {e}")
        continue

    if 'historical' in res_json:
        headers = ['Date', 'Open', 'High', 'Low', 'Close', 'adjClose', 'Volume', 'Change']
        values = []
        for data in res_json['historical']:
            try:
                values.append([
                    data['date'],
                    data['open'],
                    data['high'],
                    data['low'],
                    data['close'],
                    data['adjClose'],
                    data['volume'],
                    data['change']
                ])
            except KeyError as ke:
                print(f"Key error processing data for ticker:{ticker}. Missing key: {ke}")
                continue

        if values:
            new_values = [headers] + values
            worksheet.clear()  # Clear existing data in the worksheet
            worksheet.update(values=new_values, range_name='A1')
            print(f"No: {index + 1} ---> Completed writing into Google Sheet from ticker:{ticker} JSON")

    # Delay to avoid overwhelming the API server
    time.sleep(0.1)

# Confirm completion
print("=====> JSON list has been written to separate worksheets in the Google Sheet.")