# EHR Data Pipeline

An ETL pipeline for Electronic Health Records (EHR) data extraction, transformation, and loading, using Python and Apache Airflow to ensure clean, structured data for PostgreSQL storage.

## Overview

This project focuses on processing large-scale EHR data files by building an automated ETL pipeline. The goal is to clean, validate, and store EHR data in a PostgreSQL database, making it suitable for analysis and downstream processes. The pipeline utilizes Apache Airflow for orchestration and automation, ensuring a scalable and efficient workflow.

## Features

- **Data Extraction**: Extracts raw EHR data from local JSON files.
- **Data Transformation**: Cleans and transforms the data using Python (Pandas), including handling missing values, fixing logical inconsistencies, and removing duplicates.
- **Data Loading**: Loads the cleaned and transformed data into PostgreSQL for easy access and further analysis.
- **Automation**: Uses Apache Airflow to schedule and automate the entire ETL process.

## Project Structure

```
.
├── dags
│   └── ehr_data_pipeline.py  # Main DAG definition
├── data
│   └── fhir                 # Folder containing EHR JSON files
├── utils
│   └── data_quality_checker.py  # Data quality and cleaning functions
├── README.md
└── requirements.txt          # Python dependencies
```

## Prerequisites

- Python 3.6+
- Apache Airflow 2.0+
- PostgreSQL
- Pandas

## Installation

1. Clone the repository:

   ```sh
   git clone https://github.com/yourusername/ehr-data-pipeline.git
   cd ehr-data-pipeline
   ```

2. Install dependencies:

   ```sh
   pip install -r requirements.txt
   ```

3. Set up Apache Airflow:

   ```sh
   airflow db init
   airflow users create -u admin -p admin -r Admin -e admin@example.com -f Admin -l User
   ```

4. Start Airflow web server and scheduler:

   ```sh
   airflow scheduler &
   airflow webserver -p 8080
   ```

5. Update PostgreSQL connection details in Airflow connections UI.

## Usage

- Place your EHR JSON files in the `data/fhir` directory.
- Trigger the `ehr_data_pipeline` DAG from the Airflow web interface.

## Data Quality

Data quality checks are performed during the transformation step, including:
- Schema validation
- Missing value handling
- Logical consistency checks

## Contributing

Contributions are welcome! Feel free to submit a pull request or open an issue for suggestions.

## License

This project is licensed under the Apache License. See the [LICENSE](LICENSE) file for details.

## Contact

For questions or collaboration opportunities, contact vanessaljt0202@gmail.com.
