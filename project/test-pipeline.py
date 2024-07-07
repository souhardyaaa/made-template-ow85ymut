import os
import unittest
import pandas as pd
import sqlite3
import requests

# Define your data URLs and file paths
urls = {
    "energy_consumption": "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/ten00124?format=TSV&compressed=false",
    "greenhouse_emissions": "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/sdg_13_10?format=TSV&compressed=false",
    "deaths": "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/tps00128?format=TSV&compressed=false"
}

output_dir = "project/output"
tsv_paths = {
    "data1": os.path.join(output_dir, "final_energy_consumption_by_sector.tsv"),
    "data2": os.path.join(output_dir, "net_greenhouse_gas_emissions.tsv"),
    "data3": os.path.join(output_dir, "deaths_by_pneumonia.tsv")
}
database_paths = {
    "data1": os.path.join(output_dir, "final_energy_consumption_by_sector.db"),
    "data2": os.path.join(output_dir, "net_greenhouse_gas_emissions.db"),
    "data3": os.path.join(output_dir, "deaths_by_pneumonia.db")
}

# Ensure output directory exists
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

class TestDataPipeline(unittest.TestCase):

    def download_file(self, url, file_path):
        response = requests.get(url)
        response.raise_for_status()
        with open(file_path, 'wb') as file:
            file.write(response.content)
        print(f"Downloaded {file_path}")

    def setUp(self):
        # Download datasets for testing
        for key, url in urls.items():
            self.download_file(url, tsv_paths[key])
        
        # Load datasets into DataFrames
        self.energy_consumption = pd.read_csv(tsv_paths["data1"], delimiter='\t', encoding='ISO-8859-1')
        self.greenhouse_emissions = pd.read_csv(tsv_paths["data2"], delimiter='\t', encoding='ISO-8859-1')
        self.deaths = pd.read_csv(tsv_paths["data3"], delimiter='\t', encoding='ISO-8859-1')

    def test_fill_missing_values(self):
        # Test filling missing values with 0
        self.energy_consumption.fillna(0, inplace=True)
        self.greenhouse_emissions.fillna(0, inplace=True)
        self.deaths.fillna(0, inplace=True)

        self.assertFalse(self.energy_consumption.isnull().values.any())
        self.assertFalse(self.greenhouse_emissions.isnull().values.any())
        self.assertFalse(self.deaths.isnull().values.any())

    def test_clean_column_names(self):
        # Test cleaning column names
        self.energy_consumption.columns = [col.strip().lower() for col in self.energy_consumption.columns]
        self.greenhouse_emissions.columns = [col.strip().lower() for col in self.greenhouse_emissions.columns]
        self.deaths.columns = [col.strip().lower() for col in self.deaths.columns]

        for df in [self.energy_consumption, self.greenhouse_emissions, self.deaths]:
            for col in df.columns:
                self.assertTrue(col == col.strip().lower())

    def clean_columns(self, df, columns):
        for column in columns:
            df[column] = df[column].astype(str).str.replace('b', '').str.replace('p', '')
        return df

    def test_clean_columns(self):
        # Test removing 'b' and 'p' from specific columns
        columns_to_clean = [str(year) for year in range(2020, 2023)]
        self.greenhouse_emissions = self.clean_columns(self.greenhouse_emissions, columns_to_clean)

        for col in columns_to_clean:
            self.assertFalse(self.greenhouse_emissions[col].str.contains('b').any())
            self.assertFalse(self.greenhouse_emissions[col].str.contains('p').any())

    def transpose_and_set_index(self, df):
        df_transposed = df.transpose()
        df_transposed.columns = df_transposed.iloc[0]  # Set the first row as column headers
        df_transposed = df_transposed[1:]  # Exclude the first row as it's now the header
        df_transposed.index.name = 'year'  # Rename the index to 'year'
        df_transposed.reset_index(inplace=True)  # Reset the index to make 'year' a column
        return df_transposed

    def test_transpose_and_set_index(self):
        # Test transposing DataFrame and setting index
        transposed_df = self.transpose_and_set_index(self.energy_consumption)

        self.assertEqual(transposed_df.index.name, 'year')
        self.assertTrue('year' in transposed_df.columns)

    def save_to_sqlite(self, df, db_path, table_name):
        if os.path.exists(db_path):
            os.remove(db_path)
        conn = sqlite3.connect(db_path)
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        conn.close()
        print(f"Data saved to {db_path} in table {table_name}")

    def test_save_to_sqlite(self):
        # Test saving DataFrame to SQLite database
        transposed_df = self.transpose_and_set_index(self.energy_consumption)
        self.save_to_sqlite(transposed_df, database_paths["data1"], "final_energy_consumption_by_sector")

        conn = sqlite3.connect(database_paths["data1"])
        df_from_db = pd.read_sql_query("SELECT * FROM final_energy_consumption_by_sector", conn)
        conn.close()

        self.assertFalse(df_from_db.empty)

if __name__ == "__main__":
    unittest.main()
