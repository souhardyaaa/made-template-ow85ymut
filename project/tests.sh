#!/bin/bash

# Defining the output files expected from the pipeline
OUTPUT_FILES=(
    "output/final_energy_consumption_by_sector.tsv"
    "output/net_greenhouse_gas_emissions.tsv"
    "output/deaths_by_pneumonia.tsv"
    "output/final_energy_consumption_by_sector.xlsx"
    "output/net_greenhouse_gas_emissions.xlsx"
    "output/deaths_by_pneumonia.xlsx"
    "output/final_energy_consumption_by_sector.db"
    "output/net_greenhouse_gas_emissions.db"
    "output/deaths_by_pneumonia.db"
)

# Run the data pipeline
echo "Running the data pipeline..."
python3 data_pipeline.py

# Check if the output files are created
all_files_exist=true

for file in "${OUTPUT_FILES[@]}"; do
    if [ ! -f "$file" ]; then
        echo "Test failed: Output file $file does not exist."
        all_files_exist=false
    fi
done

if [ "$all_files_exist" = true ]; then
    echo "Test passed: All output files exist."
    exit 0
else
    echo "Test failed: Not all output files were created."
    exit 1
fi

echo "Test completed successfully."
