import pandas as pd
from io import StringIO
from datetime import datetime
import requests
import logging
import os
NIFTY100 = ['ABB', 'ACC', 'ADANIGREEN', 'ADANIENT', 'ADANIGREEN', 'ADANIPORTS', 'ATGL', 'AWL', 'AMBUJACEM',
                'APOLLOHOSP', 'ASIANPAINT', 'DMART', 'AXISBANK', 'BAJAJ-AUTO', 'BAJFINANCE', 'BAJAJFINSV',
                'BAJAJHLDNG', 'BANKBARODA', 'BERGEPAINT', 'BEL','BHARTIARTL', 'BOSCHLTD', 'BPCL', 'BRITANNIA',
                'CANBK', 'CHOLAFIN', 'CIPLA', 'COALINDIA', 'COLPAL', 'DABUR', 'DIVISLAB', 'DLF', 'DRREDDY',
                'EICHERMOT', 'GAIL', 'GODREJCP', 'GRASIM', 'HAVELLS', 'HCLTECH', 'HDFCAMC', 'HDFCBANK', 'HDFCLIFE',
                'HEROMOTOCO', 'HINDALCO', 'HAL', 'HINDUNILVR', 'ICICIBANK', 'ICICIGI', 'ICICIPRULI', 'INDUSTOWER',
                'INDUSINDBK', 'NAUKRI', 'INFY', 'INDIGO', 'IOC', 'IRCTC', 'ITC', 'JINDALSTEL',
                'JSWSTEEL', 'KOTAKBANK', 'LT', 'LIC', 'M&M', 'MARICO', 'MARUTI', 'MUTHOOTFIN', 'NESTLEIND', 'NTPC',
                'NYKAA', 'ONGC', 'PGHH', 'PAGEIND', 'PIIND', 'PIDILITIND', 'POWERGRID', 'RELIANCE', 'MOTHERSUMI',
                'SBIN', 'SBICARD', 'SBILIFE', 'SHREECEM', 'SIEMENS', 'SRF', 'SUNPHARMA', 'TATACONSUM', 'TATAMOTORS',
                'TATAPOWER', 'TATASTEEL', 'TCS', 'TECHM', 'TITAN', 'TORNTPHARM', 'ULTRACEMCO', 'MCDOWELL-N', 'UPL',
                'VBL', 'VEDL', 'ZOMATO']

NIFTY50 = ['ACC', 'ADANIPORTS', 'AMBUJACEM', 'ASIANPAINT',
                      'AXISBANK', 'BAJAJ-AUTO', 'BANKBARODA', 'BHEL', 'BPCL',
                      'BHARTIARTL', 'BOSCHLTD', 'CAIRN', 'CIPLA', 'COALINDIA',
                      'DRREDDY', 'GAIL', 'GRASIM', 'HCLTECH', 'HDFCBANK', 'HEROMOTOCO',
                      'HINDALCO', 'HINDUNILVR', 'HDFC', 'ITC', 'ICICIBANK',
                      'IDEA', 'INDUSINDBK', 'INFY', 'KOTAKBANK', 'LT',
                      'LUPIN', 'M&M', 'MARUTI', 'NTPC', 'ONGC', 'POWERGRID',
                      'PNB', 'RELIANCE', 'SBIN', 'SUNPHARMA', 'TCS', 'TATAMOTORS',
                      'TATAPOWER', 'TATASTEEL', 'TECHM', 'ULTRACEMCO', 'VEDL', 'WIPRO', 'ZEEL']
# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

def get_daily_data(dt):
    url = f"https://archives.nseindia.com/products/content/sec_bhavdata_full_{dt}.csv"
    combined_data = pd.DataFrame()

    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors
        combined_data = pd.read_csv(StringIO(response.text))
        logger.info("Column names in the downloaded data: " + ", ".join(combined_data.columns))  # Debugging line
    except requests.RequestException as e:
        logger.error(f"Failed to download data from {url}: {str(e)}")
        return None

    return combined_data

def process_data(df, stock_list):
    if df is None:
        logger.error("No data to process.")
        return pd.DataFrame()

    try:
        df = df.rename(columns=lambda x: x.strip())  # Strip spaces from all column names
        df = df[df['SERIES'].str.strip() == 'EQ']  # Ensure to strip any leading/trailing spaces
        df = df[df['SYMBOL'].isin(stock_list)]
        df['DATE'] = pd.to_datetime(df['DATE1'].str.strip()).dt.date
        df = df[['SYMBOL', 'DATE', 'CLOSE_PRICE']]
        df.columns = ['SYMBOL', 'DATE','CLOSE']  # Rename columns for consistency
        df[['CLOSE']] = df[['CLOSE']].apply(pd.to_numeric).round(2)
        df.sort_values(by=['SYMBOL', 'DATE'], inplace=True)
    except Exception as e:
        logger.error(f"Error processing DataFrame: {e}")
        return pd.DataFrame()

    return df

def main(stock_list, file_path,filename, dt):
    # master_file_path = f'{filename}_master_data.csv'
    master_file_path = file_path+filename

    today_df = get_daily_data(dt)
    if today_df is not None and not today_df.empty:
        processed_df = process_data(today_df, stock_list)
        if not processed_df.empty:
            if os.path.exists(master_file_path):
                master_df = pd.read_csv(master_file_path)
                combined_df = pd.concat([master_df, processed_df], ignore_index=True)
            else:
                combined_df = processed_df

            combined_df.sort_values(by=['SYMBOL', 'DATE'], inplace=True)
            combined_df.to_csv(master_file_path, index=False)
            logger.info('Data processed and written to master file successfully')
        else:
            logger.info('Processed data is empty, no changes made to master file.')
    else:
        logger.info('No new data available to process.')

if __name__ == "__main__":
    run_date = '23052024'  # Format should match your requirement
    file_name = 'nifty100_master_data.csv'
    stock_list = NIFTY100
    file_path = '/Users/snigdha/Documents/nse/'
    main(stock_list, file_path,file_name, run_date)
