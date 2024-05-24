import pandas as pd
from datetime import datetime
import os
import logging
import ta  # Technical Analysis library

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


def calculate_rsi(data, period=14):
    """
    Calculate the RSI for the given data.
    - data: DataFrame with 'CLOSE' prices.
    - period: The period over which to calculate the RSI.
    """
    data['RSI'] = ta.momentum.rsi(data['CLOSE'], window=4)
    data['RSI'] = data['RSI'].round(2)
    return data


def process_and_filter_entries(entry_df, new_entries, run_date):
    """
    Process and update entries with RSI calculations.
    - entry_df: DataFrame containing existing entries.
    - new_entries: DataFrame containing potential new entries with price data.
    - run_date: Current run date as a datetime.date object.
    """
    if not entry_df.empty:
        entry_df.set_index('SYMBOL', inplace=True, drop=False)

    new_entries.set_index('SYMBOL', inplace=True, drop=False)
    new_entries = calculate_rsi(new_entries)  # Calculate RSI for new entries
    new_entries['Entry Signal'] = new_entries['RSI'] < 15  # Example entry condition

    # Filter out existing symbols and where Entry Signal is True
    new_entries = new_entries[~new_entries.index.isin(entry_df.index) & new_entries['Entry Signal']]

    if not new_entries.empty:
        new_entries.reset_index(drop=True, inplace=True)
        new_entries['Run Date'] = run_date
        new_entries['Entry Date'] = run_date
        entry_df = pd.concat([entry_df, new_entries], ignore_index=True)

        # Filter for today's entries and where the entry signal is True
    today_entries = entry_df[(entry_df['Run Date'] == run_date) & (entry_df['Entry Signal'] == True)]
    return today_entries

    # return entry_df


def update_entry_file(entry_file_path, run_date):
    """
    Update the entry file by filtering and writing today's qualified entries.
    - entry_file_path: Path to the entry file.
    - run_date: The run date in 'YYYY-MM-DD' format.
    """
    run_date = datetime.strptime(run_date, '%Y-%m-%d').date()

    if os.path.exists(entry_file_path):
        entry_df = pd.read_csv(entry_file_path)
        qualified_entries_today = process_and_filter_entries(entry_df, run_date)
        if not qualified_entries_today.empty:
            output_path = os.path.join(os.path.dirname(entry_file_path), f'qualified_entries_{run_date}.csv')
            qualified_entries_today.to_csv(output_path, index=False)
            logger.info(f"Qualified entries for {run_date} written to {output_path}.")
        else:
            logger.info("No qualified entries for today.")
    else:
        logger.error("Entry file does not exist.")


def main(filepath,entry_file_nm,exit_file_nm,rundt,master_data):
    # directory ='/Users/snigdha/Documents/nse/'
    entry_file_path = os.path.join(filepath, entry_file_nm)
    exit_file_path = os.path.join(filepath,exit_file_nm)
    run_date = rundt  # Example run date
    formatted_run_date = pd.to_datetime(run_date).date()
    master_file_name = master_data

    # Sample new entries data (replace with actual data fetching logic)
    master_data = pd.read_csv(master_file_name)
    master_data.rename(columns={'DATE': 'RunDate'}, inplace=True)
    master_data.sort_values(by=['SYMBOL', 'RunDate'], inplace=True)
    master_rsi_data = master_data.groupby('SYMBOL').apply(calculate_rsi)
    if os.path.exists(entry_file_path) :
        print("file exist. This is not the first run")
        # find the symbols already opened for entries
        existing_entries = pd.read_csv(entry_file_path)
        # print(existing_entries.head(10))
        entry_symbols = existing_entries[(existing_entries['Entry Signal'] == True) & (existing_entries['Exit Signal'] == False)]
        # print(entry_symbols['SYMBOL'])
        entry_signals = master_rsi_data[(master_rsi_data['RSI']<15)
                                        & (master_rsi_data['RunDate'] == run_date )
                                        & (~master_rsi_data['SYMBOL'].isin(entry_symbols['SYMBOL']))]
        # entry_signals['ENTRY_DT'] = pd.to_datetime(run_date).date()
        entry_signals['Entry Signal'] = (entry_signals['RSI'] < 15)
        entry_signals['Exit Signal'] = (entry_signals['RSI'] > 50)

        # print(entry_signals.head(10))
        # Find Exit symbols -- PENDING
        # Filter for exit condition --
        # 1. rsi>50, today's rundate,its in existing entry symbols and not in existing exit symbols

        # For the first exit run, only check the entry file and subsequent check the exit files
        if os.path.exists(exit_file_path):
            exit_existing_data = pd.read_csv(exit_file_path)
            exit_signals = master_rsi_data[
                (master_rsi_data['RSI'] > 50) &
                (master_rsi_data['RunDate'] == run_date) &
                (master_rsi_data['SYMBOL'].isin(entry_symbols['SYMBOL'])) &
                (~master_rsi_data['SYMBOL'].isin(exit_existing_data['SYMBOL']))
                ]
            print(exit_signals.head(1))
            exit_data = pd.concat([exit_existing_data, exit_signals], ignore_index=True)
            clean_exit_date = exit_data.drop_duplicates()
            clean_exit_date.to_csv(exit_file_path, index=False)
        else:
            print("There is no exit file")
            # exit_existing_data = pd.DataFrame()
            exit_signals = master_rsi_data[
                (master_rsi_data['RSI'] > 50) &
                (master_rsi_data['RunDate'] == run_date) &
                (master_rsi_data['SYMBOL'].isin(entry_symbols['SYMBOL']))]
            print(exit_signals.head(1))
            exit_signals.to_csv(exit_file_path, index=False)

        combined_data = pd.concat([existing_entries, entry_signals], ignore_index=True)
        combined_data.to_csv(entry_file_path, index=False)
        # print("Exit signals updated and appended to the existing file.")
        #Update the exit signal in the existing entry file

            #
            # exit_existing_data.to_csv(exit_file_path, index=False)  # Save the updated file
        # print("Exit signal column updated for existing symbols.")
        # else:
        #     exit_signals.to_csv(exit_file_path, index=False)
        #     print("New exit signals file created.")

        # if not exit_signals.empty:
            # symbols_with_exit = exit_signals['SYMBOL'].unique()
            # existing_entries['Exit Signal'] = existing_entries['SYMBOL'].apply(lambda x: True if x in symbols_with_exit and not x['Exit Signal'] else False)
            # existing_entries['Exit_Date'] = existing_entries.apply(
            #     lambda row: formatted_run_date if row['SYMBOL'] in symbols_with_exit else row.get(
            #         'Exit_Date', pd.NaT),axis=1)
            # Check if the exit file exists and append new data or create a new file

        # else:
        #     print("No Exit signal for today")



    else:
        print("file doesn't exist. This is the first run. No Exit signal will be generated")
        entry_signals = master_rsi_data[(master_rsi_data['RSI']<15) & (master_rsi_data['RunDate'] == run_date )]
        # entry_signals['ENTRY_DT'] = pd.to_datetime(run_date).date()
        entry_signals['Entry Signal'] = (entry_signals['RSI'] < 15)
        entry_signals['Exit Signal'] = (entry_signals['RSI'] > 50)

        print(entry_signals.head(10))
        # write to csv
        entry_signals.to_csv(entry_file_path,index=False)

    # update_entry_file(entry_file_path, new_entries, run_date)


if __name__ == "__main__":
    directory = '/Users/snigdha/Documents/nse/'
    entry_file_name = 'nifty100_entries.csv'
    exit_file_name = 'nifty100_exit.csv'
    run_date = '2024-05-23'
    master_file_nm = 'nifty100_master_data.csv'
    master_data = os.path.join(directory,master_file_nm)
    main(directory,entry_file_name,exit_file_name,run_date,master_data)
