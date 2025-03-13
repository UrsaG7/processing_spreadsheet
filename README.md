# Google Spreadsheet Processing with Apache Airflow & DAG

### Tools Used:
- Docker (with docker-compose.yaml & make sure to add the dependencies)
- Apache Airflow
- Python
- Google Spreadsheet
- Google Big Query (for service account)

### Intended Purpose:
This repository is to store my DAG file for various spreadsheet processing needs. More will be added as my data pipeline grows, though right now it's only for a simple act of processing spreadsheet with flows as follows:
- Check if there are files inside "INPUT_FOLDER"
- Download files inside said folder into a temporary directory
- Read Excel files and drop columns by request (Currently hard coded)
- Upload the processed files into "OUTPUT_FOLDER"
- Download the processed file from said folder
- Merge processed files into a single file
- Upload the merged file into "FINAL_FOLDER"
