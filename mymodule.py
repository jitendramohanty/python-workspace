import uuid
from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.transfers.oracle_to_gcs import OracleToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
)


from airflow.exceptions import AirflowSkipException
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook


# bax-data-proc -> bcdap-dataops
# bax-protected-repo -> bcdap-dataops

email = Variable.get("email_list")
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2021, 11, 28),
    "email": [email],
    # 'email': ['matthew.m.ott@boeing.com'],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 0,
}

env = Variable.get("env")
meta_file_location = Variable.get("metafile")
esats_bq_project_id = "my-project-dev-dataset"
esats_bq_dataset_id = "sample dataset"
esats_source_db_name = "sample_source"
esats_gcs_bucket = "my-project-dev-dataset"
esats_load_replication_id = uuid.uuid4()
esats_process_day = datetime.utcnow().strftime("%Y%m%d")
esats_process_timestamp = datetime.utcnow()
esats_gcp_conn_id = "gcp-dataset"
esats_oracle_conn_id = "oracle_source_connection_id"
esats_google_cloud_storage_conn_id = "not_available"

# _metadata_file = '/airflow/dags/ESATS/data/esats_metadata_for_ingestion.csv'
_metadata_file = "{}ESATS/data/esats_metadata_for_ingestion.csv".format(
    meta_file_location
)
_esats_error_scripts = "{}ESATS/data/esats_scripts_for_error_codes.csv".format(
    meta_file_location
)

esat_table_clean_up_limit = 3


def run_this_func(**context):
    print(
        "Trigger run value received {context['dag_run'].conf['message']} for key=message"
    )


def check_gcs_file_existence(bucket_name, file_path, bq_table_id):
    hook = GoogleCloudStorageHook(gcp_conn_id=esats_google_cloud_storage_conn_id)
    if hook.exists(bucket_name=bucket_name, object_name=file_path):
        return "Export_GCS_to_BQ_{}".format(bq_table_id)
    else:
        raise AirflowSkipException(
            "No records to pull from the source. Task skipped or deferred."
        )


with DAG(
    "esats_pipeline",
    schedule_interval="0 */8 * * *",
    default_args=default_args,
    catchup=False,
    template_searchpath=["/airflow/dags/ESATS/sql"],
    tags=["ITDW", "esats"],
) as dag:
    run_this = PythonOperator(task_id="run_this", python_callable=run_this_func)

    # copy data(csv) to backup folder in GCS
    copy_files_to_backup = GCSToGCSOperator(
        task_id="backup_gcsfiles",
        source_bucket=esats_gcs_bucket,
        source_objects=["data/esats/*.gz"],
        destination_bucket=esats_gcs_bucket,
        destination_object="data/esats_backup/",
        gcp_conn_id=esats_google_cloud_storage_conn_id,
    )

    # Deletes data from the last load in GCS
    delete_esats_gcs_files = GCSDeleteObjectsOperator(
        task_id="delete_esats_gcs_files_from_earlier_load",
        bucket_name=esats_gcs_bucket,
        gcp_conn_id=esats_gcp_conn_id,
        prefix="data/esats/",
    )

    cleanup_done = DummyOperator(task_id="cleanup_done")

    fetch_src_tables_columns = pd.read_csv(_metadata_file)

    for object_name, source_table_name, schema_name, dest_table_name, col_name in zip(
        fetch_src_tables_columns["object_name"],
        fetch_src_tables_columns["source_table_name"],
        fetch_src_tables_columns["schema_name"],
        fetch_src_tables_columns["dest_table_name"],
        fetch_src_tables_columns["col_name"],
    ):
        sql_file_path = "sql/data_import_query.sql"
        ORACLE_to_GCS = OracleToGCSOperator(
            task_id="ORACLE_to_GCS_{}".format(source_table_name),
            sql=f'{{% include "{sql_file_path}" %}}',
            params={
                "schema_name": schema_name,
                "source_tablename": object_name,
                "process_timestamp": esats_process_timestamp,
                "replication_id": esats_load_replication_id,
                "source_tablename": object_name,
                "gcs_bucket": esats_gcs_bucket,
                "colname": str(col_name)[1:-1],
            },
            bucket="{{ params.gcs_bucket}}",
            filename="data/esats/{}/dataExport_{}".format(
                esats_process_day, source_table_name
            )
            + ".gz",
            # schema_filename='metadata/schemas/bq/json/{}.json'.format(source_table_name),
            oracle_conn_id=esats_oracle_conn_id,
            gzip=True,
            gcp_conn_id=esats_google_cloud_storage_conn_id,
            export_format="csv",
            field_delimiter="~",
            dag=dag,
        )

        bq_table_id = str.upper(dest_table_name)

        check_gcs_file_task = PythonOperator(
            task_id="check_gcs_file_{}".format(bq_table_id),
            python_callable=check_gcs_file_existence,
            provide_context=True,
            op_args=[
                esats_gcs_bucket,
                "data/esats/{}/dataExport_{}.gz".format(
                    esats_process_day, source_table_name
                ),
                bq_table_id,
            ],
            dag=dag,
        )

        GCS_to_BQ = GCSToBigQueryOperator(
            task_id="Export_GCS_to_BQ_{}".format(bq_table_id),
            bucket=esats_gcs_bucket,
            source_objects=[
                "data/esats/{{params.process_day}}/dataExport_{{params.source_table_name}}.gz"
            ],
            schema_object="metadata/schemas/bq/json/{}.json".format(bq_table_id),
            destination_project_dataset_table="boeing-{{params.env}}-bcdap-dataplane:{{params.bq_dataset_id}}.{{"
            "params.bq_table_id}}",
            params={
                "process_day": esats_process_day,
                "bq_dataset_id": esats_bq_dataset_id,
                "bq_table_id": bq_table_id,
                "env": env,
                #'process_timestamp': esats_process_timestamp,
                "source_table_name": source_table_name,
            },
            source_format="CSV",
            autodetect=False,
            compression="GZIP",
            field_delimiter="~",
            allow_quoted_newlines=True,
            skip_leading_rows=1,
            create_disposition="CREATE_NEVER",
            write_disposition="WRITE_APPEND",
            allow_jagged_rows=True,
            # bigquery_conn_id=esats_gcp_conn_id,
            gcp_conn_id=esats_gcp_conn_id,
            dag=dag,
        )

        DELETE_ROWS_QUERY = f"DELETE from boeing-{env}-bcdap-dataplane.{esats_bq_dataset_id}.{bq_table_id} where PROCESS_TIMESTAMP in (select distinct PROCESS_TIMESTAMP from boeing-{env}-bcdap-dataplane.{esats_bq_dataset_id}.{bq_table_id} where PROCESS_TIMESTAMP not in (SELECT PROCESS_TIMESTAMP FROM boeing-{env}-bcdap-dataplane.{esats_bq_dataset_id}.{bq_table_id} group by PROCESS_TIMESTAMP order by PROCESS_TIMESTAMP desc limit {esat_table_clean_up_limit}));"
        delete_query_job = BigQueryInsertJobOperator(
            task_id="BQ_DATA_CLEAN_UP_{}".format(bq_table_id),
            gcp_conn_id=esats_google_cloud_storage_conn_id,
            configuration={
                "query": {
                    "query": DELETE_ROWS_QUERY,
                    "useLegacySql": False,
                }
            },
            # location=location,
        )

        run_this >> copy_files_to_backup >> delete_esats_gcs_files >> cleanup_done
        (
            cleanup_done
            >> ORACLE_to_GCS
            >> check_gcs_file_task
            >> GCS_to_BQ
            >> delete_query_job
        )

with DAG(
    "esats_pipeline_error_codes_staging",
    schedule_interval="0 0 1 * *",
    default_args=default_args,
    catchup=False,
    template_searchpath=["/airflow/dags/ESATS/sql"],
    max_active_runs=1,
    concurrency=1,
    tags=["ITDW", "esats"],
) as dag2:

    run_this_ESATS_DAG_2 = PythonOperator(
        task_id="started_run_esats_pipeline_error_codes_staging",
        python_callable=run_this_func,
    )
    completed_ESATS_DAG_2 = PythonOperator(
        task_id="completed_run_esats_pipeline_error_codes_staging",
        python_callable=run_this_func,
    )

    fetch_stg_tables_columns = pd.read_csv(_esats_error_scripts)
    bq_master_error_table_name = "ESATS_BUSAPP_DQ_ERROR_DETAIL_STG"

    DELETE_STG_ROWS_QUERY = f"DELETE from boeing-{env}-bcdap-dataplane.{esats_bq_dataset_id}.{bq_master_error_table_name} where 1=1;"

    delete_stg_old_data_job = BigQueryInsertJobOperator(
        task_id="BQ_DATA_CLEAN_DATA_{}".format(bq_master_error_table_name),
        gcp_conn_id=esats_google_cloud_storage_conn_id,
        configuration={
            "query": {
                "query": DELETE_STG_ROWS_QUERY,
                "useLegacySql": False,
            }
        },
        dag=dag2,
    )

    delete_stg_old_data_job >> run_this_ESATS_DAG_2

    for error_id, sql_script in zip(
        fetch_stg_tables_columns["ERROR_ID"], fetch_stg_tables_columns["SQL_SCRIPT"]
    ):

        sql_query = sql_script.replace("[", "").replace("]", "").strip()
        errors_transfer_task = BigQueryExecuteQueryOperator(
            task_id="Export_Errors_{}".format(error_id),
            sql=sql_query,
            params={
                "projectId": "boeing-{}-bcdap-dataplane".format(env),
                "dataset": esats_bq_dataset_id,
                "process_timestamp": esats_process_timestamp,
                "replication_id": esats_load_replication_id,
            },
            destination_dataset_table="boeing-{}-bcdap-dataplane:{}.{}".format(
                env, esats_bq_dataset_id, bq_master_error_table_name
            ),
            write_disposition="WRITE_APPEND",
            use_legacy_sql=False,
            create_disposition="CREATE_NEVER",
            gcp_conn_id=esats_gcp_conn_id,
            dag=dag2,
        )

        run_this_ESATS_DAG_2 >> errors_transfer_task >> completed_ESATS_DAG_2

with DAG(
    "esats_pipeline_error_codes_detail",
    schedule_interval="0 1 1 * *",
    default_args=default_args,
    catchup=False,
    template_searchpath=["/airflow/dags/ESATS/sql"],
    max_active_runs=1,
    concurrency=1,
    tags=["ITDW", "esats"],
) as dag3:

    run_this_ESATS_DAG_3 = PythonOperator(
        task_id="started_run_esats_pipeline_error_codes_detail",
        python_callable=run_this_func,
    )
    completed_ESATS_DAG_3 = PythonOperator(
        task_id="completed_run_esats_pipeline_error_codes_detail",
        python_callable=run_this_func,
    )

    bq_master_error_details_table_name = "ESATS_BUSAPP_DQ_ERROR_DETAIL"
    sql_file_path_error = "sql/error_data_insertion.sql"

    errors_transfer_task_detail = BigQueryExecuteQueryOperator(
        task_id="Export_Errors_Details_{}".format(bq_master_error_details_table_name),
        sql=f'{{% include "{sql_file_path_error}" %}}',
        params={
            "projectId": "boeing-{}-bcdap-dataplane".format(env),
            "dataset": esats_bq_dataset_id,
            "process_timestamp": esats_process_timestamp,
            "replication_id": esats_load_replication_id,
        },
        destination_dataset_table="boeing-{}-bcdap-dataplane:{}.{}".format(
            env, esats_bq_dataset_id, bq_master_error_details_table_name
        ),
        write_disposition="WRITE_APPEND",
        use_legacy_sql=False,
        create_disposition="CREATE_NEVER",
        gcp_conn_id=esats_gcp_conn_id,
        dag=dag3,
    )

    bq_temp_resolved_errors_table_name = "ESATS_BUSAPP_DQ_ERROR_RESOLVED_TEMP"
    sql_file_path_error_resolved_temp = "sql/error_data_resolved.sql"

    resolved_errors_transfer_task_temp = BigQueryExecuteQueryOperator(
        task_id="Export_Errors_Details_Temp_{}".format(
            bq_temp_resolved_errors_table_name
        ),
        sql=f'{{% include "{sql_file_path_error_resolved_temp}" %}}',
        params={
            "projectId": "boeing-{}-bcdap-dataplane".format(env),
            "dataset": esats_bq_dataset_id,
            "process_timestamp": esats_process_timestamp,
            "replication_id": esats_load_replication_id,
        },
        destination_dataset_table="boeing-{}-bcdap-dataplane:{}.{}".format(
            env, esats_bq_dataset_id, bq_temp_resolved_errors_table_name
        ),
        write_disposition="WRITE_TRUNCATE",
        use_legacy_sql=False,
        create_disposition="CREATE_NEVER",
        gcp_conn_id=esats_gcp_conn_id,
        dag=dag3,
    )

    sql_file_path_update_error_details = "sql/error_data_update_details.sql"

    update_errors_details_task = BigQueryExecuteQueryOperator(
        task_id="Update_Errors_Details_Temp_{}".format(
            bq_master_error_details_table_name
        ),
        sql=f'{{% include "{sql_file_path_update_error_details}" %}}',
        params={"projectId": "boeing-{}-bcdap-dataplane".format(env)},
        write_disposition="WRITE_APPEND",
        use_legacy_sql=False,
        create_disposition="CREATE_NEVER",
        gcp_conn_id=esats_gcp_conn_id,
        dag=dag3,
    )

    (
        run_this_ESATS_DAG_3
        >> errors_transfer_task_detail
        >> resolved_errors_transfer_task_temp
        >> update_errors_details_task
        >> completed_ESATS_DAG_3
    )
