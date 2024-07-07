import os
import pandas as pd
import sqlite3
import requests
import webbrowser

# URLs for the datasets

urls = {
    "data1": "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/ten00124?format=TSV&compressed=false",
    "data2": "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/sdg_13_10?format=TSV&compressed=false",
    "data3": "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/tps00128?format=TSV&compressed=false"
}

# Data directory
output_dir = "output"
tsv_paths = {
    "data1": os.path.join(output_dir, "final_energy_consumption_by_sector.tsv"),
    "data2": os.path.join(output_dir, "net_greenhouse_gas_emissions.tsv"),
    "data3": os.path.join(output_dir, "deaths_by_pneumonia.tsv")
}
excel_paths = {
    "data1": os.path.join(output_dir, "final_energy_consumption_by_sector.xlsx"),
    "data2": os.path.join(output_dir, "net_greenhouse_gas_emissions.xlsx"),
    "data3": os.path.join(output_dir, "deaths_by_pneumonia.xlsx")
}
database_paths = {
    "database1": os.path.join(output_dir, "final_energy_consumption_by_sector.db"),
    "database2": os.path.join(output_dir, "net_greenhouse_gas_emissions.db"),
    "database3": os.path.join(output_dir, "deaths_by_pneumonia.db")
}

# Creating output directory if it doesn't exist
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Function to download and save files
def download_file(url, file_path):
    if not os.path.exists(file_path):
        response = requests.get(url)
        response.raise_for_status()
        with open(file_path, 'wb') as file:
            file.write(response.content)
        print(f"Downloaded {file_path}")
    else:
        print(f"File {file_path} already exists. Skipping download.")

# Downloading the datasets
print("Downloading datasets...")
download_file(url1, tsv_paths["data1"])
download_file(url2, tsv_paths["data2"])
download_file(url3, tsv_paths["data3"])
print("Download complete.")

# Reading the datasets into DataFrames
print("Reading datasets into DataFrames...")
energy_consumption = pd.read_csv(tsv_paths["data1"], delimiter='\t', encoding='ISO-8859-1', error_bad_lines=False, warn_bad_lines=True)
greenhouse_emissions = pd.read_csv(tsv_paths["data2"], delimiter='\t', encoding='ISO-8859-1', error_bad_lines=False, warn_bad_lines=True)
deaths = pd.read_csv(tsv_paths["data3"], delimiter='\t', encoding='ISO-8859-1', error_bad_lines=False, warn_bad_lines=True)

# Filling missing values with 0
print("Filling missing values...")
energy_consumption.fillna(0, inplace=True)
greenhouse_emissions.fillna(0, inplace=True)
deaths.fillna(0, inplace=True)

# Cleaning column names (strip and lowercase)
print("Cleaning column names...")
energy_consumption.columns = [col.strip().lower() for col in energy_consumption.columns]
greenhouse_emissions.columns = [col.strip().lower() for col in greenhouse_emissions.columns]
deaths.columns = [col.strip().lower() for col in deaths.columns]

# Saving cleaned DataFrames to Excel files
def save_to_excel(df, excel_path):
    if os.path.exists(excel_path):
        os.remove(excel_path)
    df.to_excel(excel_path, index=False)
    print(f"Data saved to {excel_path}")

# Saving cleaned DataFrames as Excel files
save_to_excel(energy_consumption, excel_paths["data1"])
save_to_excel(greenhouse_emissions, excel_paths["data2"])
save_to_excel(deaths, excel_paths["data3"])

print("Data cleaning and saving completed.")

# Saving DataFrames to SQLite databases
def save_to_sqlite(df, db_path, table_name):
    if os.path.exists(db_path):
        os.remove(db_path)
    conn = sqlite3.connect(db_path)
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    conn.close()
    print(f"Data saved to {db_path} in table {table_name}")

save_to_sqlite(energy_consumption, database_paths["database1"], "final_energy_consumption_by_sector")
save_to_sqlite(greenhouse_emissions, database_paths["database2"], "net_greenhouse_gas_emissions")
save_to_sqlite(deaths, database_paths["database3"], "deaths_by_pneumonia")

print("Data pipeline execution completed.")

# Opening files with specified names
def open_files(file_paths):
    for file_path, display_name in file_paths.items():
        if os.path.exists(file_path):
            print(f"Opening {display_name} at {file_path}")
            webbrowser.open(f'file://{os.path.abspath(file_path)}')
        else:
            print(f"{file_path} does not exist.")

# Opening Excel files and SQLite databases with different names
excel_files = {
    excel_paths["data1"]: "Final Energy Consumption by Sector Data",
    excel_paths["data2"]: "Net Greenhouse Gas Emissions Data",
    excel_paths["data3"]: "Deaths caused by Pneumonia"
}
open_files(excel_files)

sqlite_dbs = {
    database_paths["database1"]: "Final Energy Consumption by Sector Database",
    database_paths["database2"]: "Net Greenhouse Gas Emissions Database",
    database_paths["database3"]: "Deaths caused by Pneumonia"
}
open_files(sqlite_dbs)

