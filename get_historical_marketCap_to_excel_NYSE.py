import time
import pandas as pd
import requests
from openpyxl import Workbook
from datetime import datetime

# Load the csv file from the uploaded data
df_stocks = pd.read_csv('/kaggle/input/dataset/stock_tickers.csv')

root_url = 'https://financialmodelingprep.com/api/v3/historical-market-capitalization/'
apikey = 'qbYcpSiayLUNXFlhXQLseObPPG7FcWgU'

# Create a Workbook object to save data in a single XLSX file
wb = Workbook()

# Record the start time
start_time = datetime.now()

# Iterate over tickers
for index, row in df_stocks[:4348].iterrows():
    ticker = row['Exch_NYSE']

    # Request API
    url = f"{root_url}{ticker}?from=1980-01-01&apikey={apikey}"
    try:
        res = requests.get(url)
        res_json = res.json()
        print(f"No: {index + 1} ===> Completed getting JSON from ticker: {ticker}")
    
        values = []
        for data in res_json:
            try:
                values.append([
                    data['date'],
                    data['marketCap']
                ])
            except KeyError as ke:
                print(f"Key error processing data for ticker:{ticker}. Missing key: {ke}")
                continue

        if values:
            df = pd.DataFrame(values, columns=['Date', 'MarketCap'])
            # Create a worksheet for each ticker and save data in the XLSX file
            ws = wb.create_sheet(ticker)
            ws.append(list(df.columns))
            for _, r in df.iterrows():
                ws.append(r.tolist())
                
    except Exception as e:
        print(f"Error fetching data for ticker: {ticker}. Exception: {e}")
        continue

    # Delay to avoid overwhelming the API server
    time.sleep(0.1)

# Remove the default sheet created when the workbook was initialized
wb.remove(wb['Sheet'])

# Specify the output file name
file_name = "historical_marketCap_NYSE.xlsx"

# Save the workbook as an XLSX file
wb.save(file_name)

# Calculate the time taken in minutes
end_time = datetime.now()
time_taken = (end_time - start_time).total_seconds() / 60

print(f"=====> Combined data saved as {file_name}.")
print(f"Completed time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Time taken: {time_taken:.2f} minutes")