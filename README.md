# cars_rental_data_engineering_mage_ai_project
A car rental business customers data engineering project, using mage ai 

# Car Rental Data Engineering Pipeline

This project is a data engineering pipeline that processes car rental customer data. The pipeline extracts flat CSV files generated using the Python `faker` library, transforms the data into dimensional tables, and loads it into Google BigQuery. The entire ETL (Extract, Transform, Load) process is orchestrated using [Mage AI](https://www.mage.ai/).

## Project Overview

### Key Components:
- **Data Generation:** 
  - Utilizes the `faker` library to create synthetic car rental customer data in CSV format.
- **ETL Orchestration:**
  - Mage AI is used to manage and orchestrate the ETL process, ensuring data flows seamlessly from raw CSV files to dimensional tables.
- **Data Transformation:**
  - Data is transformed from flat CSV structures into well-organized dimensional tables, making it ready for analysis.
- **Data Loading:**
  - Transformed data is loaded into Google BigQuery for storage and further analysis.

### Technology Stack:
- **Python:** Core programming language used for data manipulation and transformation.
- **Faker:** Python library for generating synthetic data.
- **Mage AI:** Tool for orchestrating ETL workflows.
- **Google BigQuery:** Data warehouse for storing the transformed data.
- **CSV:** Format used for initial data storage.

## Setup Instructions

### Prerequisites:
- Python 3.7 or higher
- Google Cloud SDK (for BigQuery integration)
- Mage AI
- Required Python packages (listed in `requirements.txt`)

### Steps to Run the Project:

1. **Clone the Repository:**
    ```bash
    git clone https://github.com/yourusername/car-rental-data-pipeline.git
    cd car-rental-data-pipeline
    ```

2. **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

3. **Configure BigQuery:**
    - Set up your Google Cloud Project.
    - Update the BigQuery settings in the `config/bigquery_config.json`.

4. **Generate Synthetic Data:**
    ```bash
    python scripts/generate_data.py
    ```
    - This will generate the raw customer data CSV files in the `data/raw/` directory.

5. **Run the ETL Pipeline:**
    - Use Mage AI to start the ETL process and load data into BigQuery.
    - You can trigger the pipeline from the Mage AI UI or using the CLI.

6. **Verify Data in BigQuery:**
    - Once the ETL process is complete, check your BigQuery tables to ensure the data has been loaded correctly.

## Future Enhancements

- Integrate more complex data transformations.
- Add data validation and quality checks.
- Implement automated testing for the ETL pipeline.

## Images

![Architecture](https://github.com/Azim1588/cars_rental_data_engineering_mage_ai_project/blob/main/images/Architecture.png.png?raw=true)

## Data Modeling

![Data Modeling](https://github.com/Azim1588/cars_rental_data_engineering_mage_ai_project/blob/main/images/Data_Modeling.png?raw=true)

## Mage AI Workflow


![Export](https://github.com/Azim1588/cars_rental_data_engineering_mage_ai_project/blob/main/images/export.png?raw=true)
![Load](https://github.com/Azim1588/cars_rental_data_engineering_mage_ai_project/blob/main/images/load.png?raw=true)
![Transform](https://github.com/Azim1588/cars_rental_data_engineering_mage_ai_project/blob/main/images/transform.png?raw=true)
![Mage DAG](https://github.com/Azim1588/cars_rental_data_engineering_mage_ai_project/blob/main/images/mage_dag.png?raw=true)




