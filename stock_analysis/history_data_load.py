import pandas as pd
import requests
from io import StringIO
from datetime import datetime, timedelta
import logging

# List of NIFTY100 and NIFTY50 stocks
NIFTY100 = ['ABB', 'ACC', 'ADANIGREEN', 'ADANIENT', 'ADANIPORTS', 'ATGL', 'AWL', 'AMBUJACEM',
            'APOLLOHOSP', 'ASIANPAINT', 'DMART', 'AXISBANK', 'BAJAJ-AUTO', 'BAJFINANCE', 'BAJAJFINSV',
            'BAJAJHLDNG', 'BANKBARODA', 'BERGEPAINT', 'BEL', 'BHARTIARTL', 'BOSCHLTD', 'BPCL', 'BRITANNIA',
            'CANBK', 'CHOLAFIN', 'CIPLA', 'COALINDIA', 'COLPAL', 'DABUR', 'DIVISLAB', 'DLF', 'DRREDDY',
            'EICHERMOT', 'GAIL', 'GODREJCP', 'GRASIM', 'HAVELLS', 'HCLTECH', 'HDFCAMC', 'HDFCBANK', 'HDFCLIFE',
            'HEROMOTOCO', 'HINDALCO', 'HAL', 'HINDUNILVR', 'ICICIBANK', 'ICICIGI', 'ICICIPRULI', 'INDUSTOWER',
            'INDUSINDBK', 'NAUKRI', 'INFY', 'INDIGO', 'IOC', 'IRCTC', 'ITC', 'JINDALSTEL', 'JSWSTEEL', 'KOTAKBANK',
            'LT', 'LIC', 'M&M', 'MARICO', 'MARUTI', 'MUTHOOTFIN', 'NESTLEIND', 'NTPC', 'NYKAA', 'ONGC', 'PGHH',
            'PAGEIND', 'PIIND', 'PIDILITIND', 'POWERGRID', 'RELIANCE', 'MOTHERSUMI', 'SBIN', 'SBICARD', 'SBILIFE',
            'SHREECEM', 'SIEMENS', 'SRF', 'SUNPHARMA', 'TATACONSUM', 'TATAMOTORS', 'TATAPOWER', 'TATASTEEL', 'TCS',
            'TECHM', 'TITAN', 'TORNTPHARM', 'ULTRACEMCO', 'MCDOWELL-N', 'UPL', 'VBL', 'VEDL', 'ZOMATO']

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

def get_urls(start_dt, end_dt):
    holiday_dates = ["22012024", "26012024", "08032024", "25032024", "29032024", "11042024", "17042024", "01052024"]
    holidays = [datetime.strptime(date, '%d%m%Y') for date in holiday_dates]
    start_date = datetime.strptime(start_dt, '%d%m%Y')
    end_date = datetime.strptime(end_dt, '%d%m%Y')

    date_list = [single_date for single_date in (start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1))
                 if single_date.weekday() < 5 and single_date not in holidays]

    url_template = "https://archives.nseindia.com/products/content/sec_bhavdata_full_{date}.csv"
    urls = [url_template.format(date=date.strftime('%d%m%Y')) for date in date_list]
    return urls

def process_data(df, stock_list):
    # Print columns for debugging
    # logger.info(f"Columns available in DataFrame: {df.columns.tolist()}")

    # # Check if 'SERIES' column exists
    # if 'SERIES' not in df.columns:
    #     logger.error("DataFrame does not contain 'SERIES' column.")
    #     return pd.DataFrame()  # Return empty DataFrame or handle as needed

    # Process data only if 'SERIES' column is available
    df = df.rename(columns=lambda x: x.strip())
    df = df[df['SERIES'].str.strip() == 'EQ']
    df = df[df['SYMBOL'].isin(stock_list)]
    df['DATE'] = pd.to_datetime(df['DATE1'].str.strip()).dt.date
    # df = df.rename(columns=lambda x: x.strip())
    # df = df[['SYMBOL', 'DATE', 'OPEN_PRICE', 'HIGH_PRICE', 'LOW_PRICE', 'CLOSE_PRICE']]
    # df.columns = ['SYMBOL', 'DATE', 'OPEN', 'HIGH', 'LOW', 'CLOSE']
    # df[['OPEN', 'HIGH', 'LOW', 'CLOSE']] = df[['OPEN', 'HIGH', 'LOW', 'CLOSE']].apply(pd.to_numeric).round(2)
    df = df[['SYMBOL', 'DATE', 'CLOSE_PRICE']]
    df.columns = ['SYMBOL', 'DATE', 'CLOSE']
    df[['CLOSE']] = df[['CLOSE']].apply(pd.to_numeric).round(2)
    df.sort_values(by=['SYMBOL', 'DATE'], inplace=True)
    return df

def get_history_data(start_dt, end_dt, stock_list, file_path,filename):
    combined_data = pd.DataFrame()
    urls = get_urls(start_dt, end_dt)

    for url in urls:
        try:
            response = requests.get(url)
            response.raise_for_status()
            single_day_data = pd.read_csv(StringIO(response.text))
            processed_data = process_data(single_day_data, stock_list)
            combined_data = pd.concat([combined_data, processed_data], ignore_index=True)
        except requests.RequestException as e:
            logger.error(f"Failed to download data from {url}: {str(e)}")

    if not combined_data.empty:
        filepath = file_path + file_name
        combined_data.to_csv(filepath, index=False)
        logger.info('Data processed and written to file successfully')
    else:
        logger.info('No data available to write to file.')

if __name__ == "__main__":
    start_date = '01012024'
    end_date = '15052024'
    file_name = 'nifty100_master_data.csv'
    stock_list = NIFTY100
    file_path = "/Users/snigdha/Documents/nse/"
    get_history_data(start_date, end_date, stock_list, file_path,file_name)
