import os

def extract_new_files(directory, processed_files):
    csv_files = [f for f in os.listdir(directory) if f.endswith('.csv') and f not in processed_files]
    return csv_files
