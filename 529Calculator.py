import os
import pandas as pd
import numpy as np

# Directory where the CSV files are stored
directory = 'C:/2024Q4DST/'

# Output file
output_file = 'outputQ42024.csv'

# List of columns to check
columns = ['TrailerPayoutDate', 'TradeDate', 'RepNumber', 'CustomAcctNum', 'DlrCommissionAmt', 'TrailerFee', 'Commission', 'CommissionAmount']

if os.path.isfile(output_file):
    os.remove(output_file)

# Iterate over every file in the directory
for filename in os.listdir(directory):
    if filename.endswith(".csv"):  # Check the file extension
        # Full path to the file
        file_path = os.path.join(directory, filename)

        # Read the CSV file
        df = pd.read_csv(file_path)

        # Preprocess to remove extra characters from 'SocialCode' column
        df['SocialCode'] = df['SocialCode'].str.replace('="', '').str.replace('"', '')

        # Filter rows where SocialCode equals '529'
        df_filtered = df[df['SocialCode'] == '529']

        # Convert 'TradeDate' to datetime type if it exists
        if 'TradeDate' in df_filtered.columns:
            df_filtered['TradeDate'] = pd.to_datetime(df_filtered['TradeDate'].astype(str), format='%Y%m%d') 
        
        # Convert 'TrailerPayoutDate' to datetime type if it exists
        if 'TrailerPayoutDate' in df_filtered.columns:
            df_filtered['TrailerPayoutDate'] = pd.to_datetime(df_filtered['TrailerPayoutDate'].astype(str), format='%Y%m%d')

        # Only process files that contain at least one of the columns
        if any(column in df_filtered.columns for column in columns):

            # Add any missing columns with empty values
            for column in columns:
                if column not in df_filtered.columns:
                    df_filtered[column] = ''

            # Select the desired columns
            df_filtered = df_filtered[columns]

            # Check if the output file exists
            if os.path.isfile(output_file):
                # Append to the file without writing the header
                df_filtered.to_csv(output_file, mode='a', header=False, index=False)
            else:
                # Write to a new file with a header
                df_filtered.to_csv(output_file, mode='w', index=False)

# Add a final row that is the sum of each numeric column in the output file
output_df = pd.read_csv(output_file)
sum_row = pd.DataFrame(output_df.select_dtypes(include=[np.number]).sum()).transpose()
sum_row.index = ['Total']
output_df = pd.concat([output_df, sum_row], ignore_index=False)

# Overwrite the output file with the new DataFrame
output_df.to_csv(output_file, index=False)
