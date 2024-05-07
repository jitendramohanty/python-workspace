import uuid, pandas as pd
from datetime import datetime
from io import BytesIO

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.jdbc.hooks.jdbc import JdbcHook
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.exceptions import AirflowSkipException


from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


email = Variable.get('itdw_email_list')
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 13),
    'email': [email],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0
}

env = Variable.get('env')
meta_file_location = Variable.get('metafile')
ariba_bq_project_id = 'boeing-{}-bcdap-dataops'.format(env)
ariba_bq_dataset_id = 'ARIBA'
ariba_source_db_name = 'BCDAP_LOAD'
ariba_gcs_bucket = 'boeing-{}-bcdap-dataops-ariba'.format(env)
ariba_load_replication_id = str(uuid.uuid4())
ariba_process_day =  datetime.utcnow().strftime('%Y%m%d')
ariba_process_timestamp = datetime.utcnow()
ariba_gcp_conn_id = 'gcp-bcdap-ariba'
ariba_saphana_conn_id = 'ariba-saphana-jdbc'
ariba_google_cloud_storage_conn_id = 'gcp-bcdap-ariba'
ariba_table_clean_up_limit = 3

_metadata_file = '{}SAPHANA/data/saphana_metadata_for_ingestion.csv'.format(meta_file_location)


def run_this_func(**context):
    print("Trigger run value received {context['dag_run'].conf['message']} for key=message")

def ariba_data_extract_to_gcs(object_name, source_table_name, schema_name, dest_table_name, col_name, order_by, delta_condition):
    batch_size = 500000
    offset = 0
    column_names_str = col_name.replace("[", "").replace("]", "").replace(" ","").strip()
    order_by_cols = order_by.replace("[", "").replace("]", "").replace(" ","").strip()
    file_path = 'data/ariba/{}'.format(ariba_process_day)
    bucket_name=ariba_gcs_bucket
    try:
        hook = JdbcHook(jdbc_conn_id=ariba_saphana_conn_id)
        count=1
        total_rows=0
        while True:
            sql_query = build_query(source_table_name, schema_name, delta_condition, batch_size, offset, column_names_str, order_by_cols)
            print('Query is : ',sql_query)
            try:
                df = hook.get_pandas_df(sql_query)
            except Exception as e:
                print("Data extract error: " + str(e))
                raise Exception("Some condition failed. Task should fail.")                         
            if len(df) > 0:
                print('The Data is processsed from the table :',source_table_name)
                print("Size of this data pull is : ",len(df))
                df = df.drop(columns=['row_num'], errors='ignore')
                df['EVENT_TIMESTAMP'] = ariba_process_timestamp
                df['PROCESS_TIMESTAMP'] = ariba_process_timestamp
                df['EVENT_COMMIT_NUMBER'] = '12345'
                df['TRANSACTION_TYPE'] = 'INSERT'
                df['REPLICATION_ID'] = ariba_load_replication_id

                gcs_hook = GoogleCloudStorageHook(gcp_conn_id=ariba_gcp_conn_id)
                file_name = f'{file_path}/dataExport_{source_table_name}_{dest_table_name}_{count}.csv'
                csv_data = df.to_csv(index=False, sep='~')
                csv_buffer = BytesIO()
                csv_buffer.write(csv_data.encode())
                csv_buffer.seek(0)
                
                gcs_hook.upload(
                    bucket_name=bucket_name,
                    object_name=file_name,
                    data=csv_buffer.getvalue(),
                    mime_type='text/csv'
                )
                count=count+1
                total_rows+=offset
            elif count==1:
                raise AirflowSkipException("No records to pull from the source. Task skipped or deferred.")
            else:
                print("No more data available for the table : ",source_table_name)
                break
            offset += batch_size 
    except Exception as ex:
        print("Data connection error: " + str(ex))
        raise ex
    

def build_query(source_table_name, schema_name, delta_condition, batch_size, offset, column_names_str, order_by_cols):
    sql_query = f"""select * from ( SELECT {column_names_str},ROW_NUMBER() OVER (ORDER BY {order_by_cols}) AS row_num FROM {schema_name}.{source_table_name}) as  numbered_rows """

    sql_query += f"""  where row_num  >{offset} AND row_num <= {offset + batch_size} """

    # print(sql_query)
    return sql_query


with DAG('SAP_HANA_pipeline',
         schedule_interval='0 6 * * *',
         default_args=default_args,
         catchup=False,
         template_searchpath=['/airflow/dags/SAPHANA/sql'],
         tags=['ariba', 'ITDW']
         ) as dag1:

    run_this_ariba_DAG = PythonOperator(task_id="run_this_ariba_DAG", python_callable=run_this_func)

    # Deletes data from the last load in GCS
    delete_ariba_gcs_files_ARIBA = GCSDeleteObjectsOperator(
        task_id="delete_ariba_gcs_files_from_earlier_load_ARIBA",
        bucket_name=ariba_gcs_bucket,
        gcp_conn_id=ariba_gcp_conn_id,
        prefix="data/ariba/"
    )

    #Moves data to backup folder
    copy_files_to_backup = GCSToGCSOperator(
        task_id='backup_gcsfiles',
        source_bucket=ariba_gcs_bucket,
        source_objects=['data/ariba/*.csv'],
        destination_bucket=ariba_gcs_bucket,
        destination_object='data/ariba_backup/',
        gcp_conn_id=ariba_google_cloud_storage_conn_id
    )
    
    fetch_src_tables_columns = pd.read_csv(_metadata_file)
    print(type(fetch_src_tables_columns))
    print(zip(fetch_src_tables_columns['object_name'],fetch_src_tables_columns['source_table_name'], fetch_src_tables_columns['schema_name'], fetch_src_tables_columns['dest_table_name'], fetch_src_tables_columns['col_name'], fetch_src_tables_columns['order_by'], fetch_src_tables_columns['delta_condition']))

    for (object_name, source_table_name, schema_name, dest_table_name, col_name, order_by, delta_condition) in zip(
                                                                        fetch_src_tables_columns['object_name'],
                                                                        fetch_src_tables_columns['source_table_name'],
                                                                        fetch_src_tables_columns['schema_name'],
                                                                        fetch_src_tables_columns['dest_table_name'],
                                                                        fetch_src_tables_columns['col_name'],
                                                                        fetch_src_tables_columns['order_by'],
                                                                        fetch_src_tables_columns['delta_condition']):
        
        saphana_extract_gcs_task = PythonOperator( task_id='Saphana_to_GCS_{}'.format(dest_table_name),
                                python_callable=ariba_data_extract_to_gcs,
                                op_args=[object_name, source_table_name, schema_name,dest_table_name, col_name, order_by, delta_condition], 
                                dag=dag1
                                )

        bq_table_id = str.upper(dest_table_name)
         
        GCS_to_BQ_Ariba = GCSToBigQueryOperator(
                task_id='Export_GCS_to_BQ_Ariba_{}'.format(bq_table_id),
                bucket=ariba_gcs_bucket,
                source_objects=['data/ariba/{{params.process_day}}/dataExport_{{params.source_table_name}}_{{params.dest_table_name}}_*.csv'],
                #dataExport_{source_table_name}_{dest_table_name}.csv
                schema_object='metadata/schemas/bq/json/{}.json'.format(bq_table_id),
                destination_project_dataset_table='boeing-{{params.env}}-bcdap-dataplane:{{params.bq_dataset_id}}.{{'
                                                'params.bq_table_id}}',
                params={'process_day': ariba_process_day,
                        'bq_dataset_id': ariba_bq_dataset_id,
                        'bq_table_id': bq_table_id,
                        'env': env,
                        'process_timestamp': ariba_process_timestamp,
                        'source_table_name': source_table_name,
                        'dest_table_name': dest_table_name},
                source_format='CSV',
                autodetect=False,
                # compression='GZIP',
                field_delimiter='~',
                allow_quoted_newlines=True,
                skip_leading_rows=1,
                create_disposition='CREATE_NEVER',
                write_disposition='WRITE_TRUNCATE',
                allow_jagged_rows=True,
                # bigquery_conn_id=efbi_gcp_conn_id,
                gcp_conn_id=ariba_gcp_conn_id,
                # paremeter to add
                ignore_unknown_values=True,
                dag=dag1)   
           
        run_this_ariba_DAG >> delete_ariba_gcs_files_ARIBA >> saphana_extract_gcs_task >> GCS_to_BQ_Ariba >> copy_files_to_backup
        