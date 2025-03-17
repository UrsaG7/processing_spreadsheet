from airflow import DAG
from airflow.operators.python import PythonOperator
#from airflow.operators.python import DummyOperator
from datetime import datetime, timedelta
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
import os
import tempfile

INPUT_FOLDER_ID = "YOUR_FOLDER_ID"
OUTPUT_FOLDER_ID = "YOUR_FOLDER_ID"
FINAL_FOLDER_ID = "YOUR_FOLDER_ID"
COLUMNS_DROP_FOLDER_ID = "YOUR_FOLDER_ID"

def get_drive_service(**context):
    """To create and return a Google Drive service"""
    context['ti'].log.info("Creating Google Drive service...")
    try:
        hook = GoogleBaseHook(gcp_conn_id='google-drive-connection')
        credentials = hook.get_credentials()
        drive_service = build("drive", "v3", credentials=credentials)
        context['ti'].log.info("Successfully created Google Drive service")
        return drive_service
    except Exception as e:
        context['ti'].log.error(f"Error creating Google Drive service: {str(e)}")
        raise

def check_excel_files(folder_id, **context):
    """To check all Excel files from the folder"""
    context['ti'].log.info(f"Fetching Excel files from folder ID: {folder_id}")
    
    drive_service = get_drive_service(**context)
    
    query = f"'{folder_id}' in parents and mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'"
    results = drive_service.files().list(
        q=query,
        fields="files(id, name)",
        pageSize=20
    ).execute()
    
    files = results.get("files", [])
    
    if not files:
        context['ti'].log.info("No Excel files found in the input folder")
        return []
    
    context['ti'].log.info(f"Found {len(files)} Excel files in the input folder")
    for file in files:
        context['ti'].log.info(f"File found: {file['name']} (ID: {file['id']})")

    context['ti'].xcom_push(key='excel_file', value=files)

def download_excel(isColumnsCheck, **context):
    """To download the files"""

    excel_files = context["ti"].xcom_pull(task_id='check_for_files', key='excel_file')
    
    if not excel_files:
        context['ti'].log.info("No files Detected. Exiting.")
        return
    
    drive_service = get_drive_service(**context)

    downloaded_files = []

    with tempfile.TemporaryDirectory() as temp_dir:

        for file_info in excel_files:
            try:
                file_id = file_info["id"]
                file_name = file_info["name"]

                input_path = os.path.join(temp_dir, file_name)
                output_path = os.path.join(temp_dir, f"processed_{file_name}")

                context['ti'].log.info(f"Downloaded file: {file_name} (ID: {file_id})")

                request = drive_service.files().get_media(fileId=file_id)
                with open(input_path, "wb") as f:
                    f.write(request.execute())

                downloaded_files.append({"file_name": file_name, "input_path": input_path, "output_path": output_path})

            except Exception as e:
                context['ti'].log.error(f"Failed to download {file_name}: {str(e)}")

        
        if isColumnsCheck:
            context['ti'].xcom.push(key='columns_list', value=downloaded_files)
        else:   
            context['ti'].xcom.push(key='downloaded_excel_files', value=downloaded_files)

def read_columns_list(**context):
    """To read the file containing columns to be dropped, and push it with xcom"""
    columns_list = context['ti'].xcom.pull(task_id='get_file', key='columns_list')

    if not columns_list:
        context['ti'].log.info('There are no file inside the directory, Exitting!')
        return
        
    col_list = []
    
    for file in columns_list:
        context['ti'].log.info("Processing Excel data...")
        try:
            input_path = file['input_path']

            df = pd.read_excel(input_path)

            for i in df['columns_to_drop']:
                col_list.append(i)

        except Exception as e:
            context['ti'].log.error(f'{e}')

    context['ti'].xcom.push(key='columns_to_drop_list', value=col_list)

def process_excel_file(isColumnsCheck=False,**context):
    """To process the files by dropping columns based on the list provided by read_columns_list"""
    downloaded_files = context['ti'].xcom.pull(task_id='get_file', key='downloaded_excel_files')

    if isColumnsCheck:
        columns_to_drop = context['ti'].xcom.pull(task_id='read_columns_list', key='columns_to_drop_list')

    if not downloaded_files:
        context['ti'].log.info('There are no file inside the directory, Exitting!')
        return {"There's no file!"}
    
    processed_excel = []
    dataframes = []
    
    for file in downloaded_files:
        context['ti'].log.info("Processing Excel data...")
        try:
            file_name = file['file_name']
            input_path = file['input_path']
            output_path = file['output_path']

            df = pd.read_excel(input_path)
            original_shape = df.shape
            context['ti'].log.info(f"Original dataframe shape: {original_shape}")

            dataframes.append(df)
            
            existing_columns = [col for col in columns_to_drop if col in df.columns]
            
            if existing_columns:
                df = df.drop(columns=existing_columns)
                context['ti'].log.info(f"Dropped columns: {existing_columns}")
            else:
                context['ti'].log.info("None of the specified columns found in the file")
            
            df.to_excel(output_path, index=False)
            context['ti'].log.info(f"New dataframe shape: {df.shape}")
            context['ti'].log.info(f"Saved processed file to {output_path}")
            
            # This is the information necessary in order for the file to be uploaded later
            file_metadata = {
                "name": f"processed_{file_name}",
                "parents": [OUTPUT_FOLDER_ID],
                "mimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            }
            
            media = MediaFileUpload(
                output_path,
                mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                resumable=True
            )

            processed_excel.append({
                'metadata':file_metadata,
                'media':media
            })
            
        except Exception as e:
            context['ti'].log.error(f"Error processing file {file_name}: {str(e)}")
            raise

    context['ti'].xcom.push(key='processed_file', value=processed_excel)
    context['ti'].xcom.push(key='processed_merged_dataframes', value=dataframes)

def upload_excel(isMerging=False,**context):
    """To upload the files into the destination folder"""
    context['ti'].log.info("Uploading excel to destination folder in Google Drive")

    drive_service = get_drive_service(**context)

    if not isMerging:
        file_bodies = context['ti'].xcom.pull(task_id='process_file', key='processed_file')
    else:
        file_bodies = context['ti'].xcom.pull(task_id='merge_processed_file', key='merged_file')

    if file_bodies:
        for file_body in file_bodies:
            try:
                metadata = file_body['metadata']
                media = file_body['media']

                uploaded_file = drive_service.files().create(
                    body=metadata,
                    media_body=media,
                    fields="id,name"
                ).execute()

                context['ti'].log.info(f"Uploaded file to output folder: {uploaded_file.get('name')} (ID: {uploaded_file.get('id')})")
            except Exception as e:
                context['ti'].log.error(f'Error while uploading file: {str(e)}')

    return {'Upload Complete!'}

def merge_all_files(**context):
    """To merge the processed files into one single file"""
    dataframes = context['ti'].xcom.pull(task_id='process_file', key='processed_merged_dataframes')

    with tempfile.TemporaryDirectory() as temp_dir:
        
        output_path = os.path.join(temp_dir, 'merged_QRIS_Merchant')

        if dataframes:
            try:
                context['ti'].log.info('Merging file')
                dfMerge = pd.concat(dataframes)

                dfMerge.to_excel(output_path, index=False)

                context['ti'].log.info('File merged')
                context['ti'].log.info(f"New dataframe shape: {dfMerge.shape}")
                context['ti'].log.info(f"Saved to : {output_path}")

                yesterday = datetime.now().date() - timedelta(days=1)
                formated_date = yesterday.strftime('%y-%m-%d')
                

                file_metadata = {
                    "name": f"merged_QRIS_Merchant {formated_date}",
                    "parents": [FINAL_FOLDER_ID],
                    "mimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                }
                
                media = MediaFileUpload(
                    output_path,
                    mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    resumable=True
                )

                merged_file = {
                    'metadata':file_metadata,
                    'media':media
                }

                context['ti'].xcom.push(key='merged_file', value=merged_file)

            except Exception as e:
                context['ti'].log.error(f'Error merging file {str(e)}')


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=30),
}

dag = DAG(
    'gdrive_excel_processor',
    default_args=default_args,
    description='Process Excel files from Google Drive by dropping specified columns',
    schedule_interval=timedelta(hours=1),
    catchup=False,
)

check_file = PythonOperator(
    task_id='check_for_files',
    python_callable=check_excel_files,
    op_kwargs={'folder_id':INPUT_FOLDER_ID},
    provide_context=True,
    dag=dag
)

download_file = PythonOperator(
    task_id='get_file',
    python_callable=download_excel,
    provide_context=True,
    dag=dag
)

process_excel_file = PythonOperator(
    task_id='process_file',
    python_callable=process_excel_file,
    provide_context=True,
    dag=dag
)

upload_file = PythonOperator(
    task_id='upload_to_gdrive',
    python_callable=upload_excel,
    provide_context=True,
    dag=dag
)

check_parameters_columns = PythonOperator(
    task_id='check_columns_list',
    python_callable=check_excel_files,
    op_kwargs={'folder_id':COLUMNS_DROP_FOLDER_ID},
    provide_context=True,
    dag=dag
)

download_columns_list = PythonOperator(
    task_id='get_processed_file',
    python_callable=download_excel,
    op_kwargs={'isColumnsCheck':True},
    provide_context=True,
    dag=dag
)

read_columns_drop_list = PythonOperator(
    task_id='read_columns_list',
    python_callable=read_columns_list,
    provide_context=True,
    dag=dag
)

check_processed_file_after = PythonOperator(
    task_id='check_processed_file',
    python_callable=check_excel_files,
    op_kwargs={'folder_id':OUTPUT_FOLDER_ID},
    provide_context=True,
    dag=dag
)

download_processed_file = PythonOperator(
    task_id='get_processed_file',
    python_callable=download_excel,
    provide_context=True,
    dag=dag
)

merge_processed_file = PythonOperator(
    task_id='merge_processed_file',
    python_callable=merge_all_files,
    provide_context=True,
    dag=dag
)

upload_merged_file = PythonOperator(
    task_id='upload_merged_file_to_gdrive',
    python_callable=upload_excel,
    op_kwargs={'isMerging':True},
    provide_context=True,
    dag=dag
)


check_file >> download_file >>  check_parameters_columns >> download_columns_list >> read_columns_drop_list >> process_excel_file >> upload_file >> check_processed_file_after >> download_processed_file >> merge_processed_file >> upload_merged_file