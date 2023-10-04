# The purpose of this package is to create custom operators for Google Cloud Platform
import json
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.transfers.mssql_to_gcs import MSSQLToGCSOperator
from airflow.providers.google.cloud.transfers.mysql_to_gcs import MySQLToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airmelt_airflow_operators import general
from google.cloud import bigquery

NEWLINE_DELIMITED_JSON = "NEWLINE_DELIMITED_JSON"
CSV = "CSV"


class BigQuery(object):
    def __init__(
        self, 
        bq_project_id: str, 
        credentials_path=None
    ):
        """
        BigQuery class
        bq_project_id: Name of BQ project
        credentials_path: Path to the credentials file
        """
        self.bq_project_id = bq_project_id
        if not credentials_path:
            self.bq_client = bigquery.Client(project=self.bq_project_id)
        else:
            self.bq_client = bigquery.Client.from_service_account_json(
                credentials_path, project=self.bq_project_id
            )
    
    def create_table(
        self,
        datasetName,
        tableName,
        schema,
        partition_col_name=None,
        expirtion_days=None,
    ):
        table_ref = self.bq_client.dataset(datasetName).table(tableName)
        table = self.bq_client.get_table(table_ref)
        if table:
            return True
        else:
            if expirtion_days:
                expiration_ms = 86400000 * expirtion_days
            else:
                expiration_ms = None
            table = bigquery.Table(table_ref, schema=schema)
            if partition_col_name:
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field=partition_col_name,
                    expiration_ms=expiration_ms,
                )
            table = self.bq_client.create_table(table)
            return table

class MSSQLToBigQueryOperator(BaseOperator):
    """
    Handles query transfers from a Microsoft SQL Server database to a Google Cloud Storage bucket,
    then to Google BigQuery.

    Paramters
    ---------
    mssql_conn_id : str, required
        The source Microsoft SQL Server connection id.
    gcp_conn_id : str, required
        The destination Google Cloud connection id.
    list_processes_to_run: str, required
        List of processes to run. By default, all processes are run. In template variable use ["process1", "process2"]
    sql : str, required
        The SQL query to execute on the Microsoft SQL Server database.
    bucket : str
        The intermediate Google Cloud Storage bucket where the data should be written.
    gs_path : str, required.
        The path to store GS intermediate files 'data/customers'
    table_schema : dict, optional
        expected BQ table schema dictionary
    destination_project_id : str, required
        The destination Google BigQuery project id.
    destination_table_id : str, required
        The destination Google BigQuery table id (including dataset name). The dataset must already exist.
        dataset.table_name
    create_disposition : str, required
        The create disposition if the table doesn't exist. Default is CREATE_IF_NEEDED.
    write_disposition : str, required
        The write disposition if the table already exists. Default is WRITE_APPEND
    time_partitioning : dict, optional
        Time partition definition for the output table. For example: {"type": "DAY", "field": "dt"}
    cluster_fields : list, optional.
        List of fields to be clustered. For example ['field1', 'field2']
    file_format : str, optional json, csv, parquet
        File format. Default: json
    ignore_unknown_values : bool, Optional
        Indicates if BigQuery should allow
        extra values that are not represented in the table schema.
        If true, the extra values are ignored. If false, records with extra columns
        are treated as bad records, and if there are too many bad records, an
        invalid error is returned in the job result.
    allow_quoted_newlines : bool, Optional
        Whether to allow quoted newlines (true) or not (false).
    allow_jagged_rows : bool, optional
        The missing values are treated as nulls. If false, records with missing trailing
        columns are treated as bad records, and if there are too many bad records, an
        invalid error is returned in the job result. Only applicable to CSV, ignored
        for other formats.
    shard_data: bool, optional
        shard query into multiple files. This will result in a prefix being added. Default is False
    delete_files_after_import: bool, optional
        whether to delete files after import, default: False
    partition : str, optional
        The partition to load the data into, in YYY-MM-DD format
    """

    template_fields = [
        "sql",
        "gs_path",
        "destination_table_id",
        "bucket",
        "destination_project_id",
        "list_processes_to_run",
        "partition",
    ]

    def __init__(
        self,
        mssql_conn_id,
        gcp_conn_id,
        list_processes_to_run,
        sql,
        bucket,
        gs_path: str,
        destination_project_id,
        destination_table_id,
        table_schema=None,
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_APPEND",
        file_format="json",
        time_partitioning=None,
        cluster_fields=None,
        ignore_unknown_values=True,
        allow_quoted_newlines=True,
        allow_jagged_rows=True,
        shard_data=True,
        delete_files_after_import=False,
        partition=None,
        *args,
        **kwargs,
    ):
        super(MSSQLToBigQueryOperator, self).__init__(*args, **kwargs)
        self.mssql_conn_id = mssql_conn_id
        self.gcp_conn_id = gcp_conn_id
        self.sql = sql
        self.bucket = bucket
        self.gs_path = gs_path
        self.table_schema = table_schema
        self.destination_project_id = destination_project_id
        self.destination_table_id = destination_table_id
        self.write_disposition = write_disposition
        self.create_disposition = create_disposition
        self.time_partitioning = time_partitioning
        self.cluster_fields = cluster_fields
        self.list_processes_to_run = list_processes_to_run
        self.file_format = file_format
        self.ignore_unknown_values = ignore_unknown_values
        self.allow_quoted_newlines = allow_quoted_newlines
        self.allow_jagged_rows = allow_jagged_rows
        self.shard_data = shard_data
        self.delete_files_after_import = delete_files_after_import
        self.partition = partition
        self.task_id = kwargs.get("task_id")

    def execute(self, context):
        serialize_process_list = json.loads(str(self.list_processes_to_run))
        if self.table_schema:
            autodetect = False
            schema_fields = general.generate_bq_schema(self.table_schema)
        else:
            autodetect = True
            schema_fields = None
        gs_file = general.GSFile(gs_path=self.gs_path, sharded=self.shard_data)
        if self.file_format == "json":
            source_format = NEWLINE_DELIMITED_JSON
        else:
            source_format = CSV
        if serialize_process_list == [] or self.task_id in serialize_process_list:
            try:
                self.log.info(
                    "Executing transfer task {tsk} to file {fl}".format(
                        tsk=self.task_id, fl=gs_file.full_name
                    )
                )
                self.log.info("bucket: {b}; ".format(b=self.bucket))
                # Execute MSSQLToGCSOperator to transfer data to GCS
                MSSQLToGCSOperator(
                    task_id="{}_mssql_to_gcs".format(self.task_id),
                    mssql_conn_id=self.mssql_conn_id,
                    gcp_conn_id=self.gcp_conn_id,
                    sql=self.sql,
                    bucket=self.bucket,
                    filename=gs_file.full_name,
                    schema=schema_fields,
                    dag=self.dag,
                    export_format="JSON",
                ).execute(context)

                self.log.info(
                    "The file {} has been exported to GCS".format(gs_file.full_name)
                )

                # Execute GCSToBigQueryOperator to load data from GCS to BigQuery
                GCSToBigQueryOperator(
                    task_id="{}_gcs_to_bq".format(self.task_id),
                    bucket=self.bucket,
                    source_objects=gs_file.gs_source,
                    destination_project_dataset_table=general.gen_bq_dataset_table(
                        project_id=self.destination_project_id,
                        destination_table_id=self.destination_table_id,
                        partition=self.partition,
                    ),
                    schema_fields=schema_fields,
                    write_disposition=self.write_disposition,
                    source_format=source_format,
                    create_disposition=self.create_disposition,
                    ignore_unknown_values=self.ignore_unknown_values,
                    allow_quoted_newlines=self.allow_quoted_newlines,
                    allow_jagged_rows=self.allow_jagged_rows,
                    dag=self.dag,
                    gcp_conn_id=self.gcp_conn_id,
                    autodetect=autodetect,
                ).execute(context)
            except Exception as ex:
                self.log.error(
                    "Could not load data from MSSQL {tsk}: {ex}".format(
                        tsk=self.task_id, ex=ex
                    )
                )
                raise
            if self.delete_files_after_import:
                try:
                    GCSDeleteObjectsOperator(
                        bucket_name=self.bucket,
                        prefix="gs://" + gs_file.path,
                        dag=self.dag,
                        google_cloud_storage_conn_id=self.gcp_conn_id,
                    ).execute(context)
                except Exception as ex:
                    self.logger.error(
                        "Could not delete GS files in {}".format("gs://" + gs_file.path)
                    )
            return True


class MySQLToBigQueryOperator(BaseOperator):
    """
    Handles query transfers from a MySQ database to a Google Cloud Storage bucket, then to Google BigQuery.

    Paramters
    ---------
    mysql_conn_id : str, required
        The source MySQL connection id.
    gcp_conn_id : str, required
        The destination Google Cloud connection id.
    list_processes_to_run: str, required
        List of processes to run. By default, all processes are run. In template variable use ["process1", "process2"]
    sql : str, required
        The SQL query to execute on the MySQL database.
    bucket : str
        The intermediate Google Cloud Storage bucket where the data should be written.
    gs_path : str, required.
        The path to store GS intermediate files 'data/customers'
    table_schema : dict, optional
        expected table data schema dictionary
    destination_project_id : str, required
        The destination Google BigQuery project id.
    destination_table_id : str, required
        The destination Google BigQuery table id (including dataset name). The dataset must already exist.
        dataset.table_name
    create_disposition : str, required
        The create disposition if the table doesn't exist. Default is CREATE_IF_NEEDED.
    write_disposition : str, required
        The write disposition if the table already exists. Default is WRITE_APPEND
    time_partitioning : dict, optional
        Time partition definition for the output table. For example: {"type": "DAY", "field": "dt"}
    cluster_fields : list, optional.
        List of fields to be clustered. For example ['field1', 'field2']
    file_format : str, optional json, csv, parquet
        File format. Default: json
    ignore_unknown_values : bool, Optional
        Indicates if BigQuery should allow
        extra values that are not represented in the table schema.
        If true, the extra values are ignored. If false, records with extra columns
        are treated as bad records, and if there are too many bad records, an
        invalid error is returned in the job result.
    allow_quoted_newlines : bool, Optional
        Whether to allow quoted newlines (true) or not (false).
    allow_jagged_rows : bool, optional
        The missing values are treated as nulls. If false, records with missing trailing
        columns are treated as bad records, and if there are too many bad records, an
        invalid error is returned in the job result. Only applicable to CSV, ignored
        for other formats.
    shard_data: bool, optional
        shard query into multiple files. This will result in a prefix being added. Default is False
    delete_files_after_import: bool, optional
        whether to delete files after import, default: False
    ensure_utc: bool, optional
        Ensure TIMESTAMP columns exported as UTC. If set to False,
        TIMESTAMP columns will be exported using the MySQL server’s default timezone.
    partition : str, optional
        The partition to load the data into, in YYY-MM-DD format
    """

    template_fields = [
        "sql",
        "gs_path",
        "destination_table_id",
        "bucket",
        "destination_project_id",
        "list_processes_to_run",
        "partition",
    ]

    def __init__(
        self,
        mysql_conn_id,
        gcp_conn_id,
        list_processes_to_run,
        sql,
        bucket,
        gs_path: str,
        destination_project_id,
        destination_table_id,
        table_schema: dict = None,
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_APPEND",
        file_format="json",
        time_partitioning=None,
        cluster_fields=None,
        ignore_unknown_values=True,
        allow_quoted_newlines=True,
        allow_jagged_rows=True,
        shard_data=True,
        delete_files_after_import=False,
        ensure_utc=True,
        partition=None,
        *args,
        **kwargs,
    ):
        super(MySQLToBigQueryOperator, self).__init__(*args, **kwargs)
        self.mysql_conn_id = mysql_conn_id
        self.gcp_conn_id = gcp_conn_id
        self.sql = sql
        self.bucket = bucket
        self.gs_path = gs_path
        self.table_schema = table_schema
        self.destination_project_id = destination_project_id
        self.destination_table_id = destination_table_id
        self.write_disposition = write_disposition
        self.create_disposition = create_disposition
        self.time_partitioning = time_partitioning
        self.cluster_fields = cluster_fields
        self.list_processes_to_run = list_processes_to_run
        self.file_format = file_format
        self.ignore_unknown_values = ignore_unknown_values
        self.allow_quoted_newlines = allow_quoted_newlines
        self.allow_jagged_rows = allow_jagged_rows
        self.shard_data = shard_data
        self.delete_files_after_import = delete_files_after_import
        self.ensure_utc = ensure_utc
        self.partition = partition
        self.task_id = kwargs.get("task_id")

    def execute(self, context):
        serialize_process_list = json.loads(str(self.list_processes_to_run))
        destination_dataset_table = general.gen_bq_dataset_table(
            project_id=self.destination_project_id,
            destination_table_id=self.destination_table_id,
            partition=self.partition,
        )
        if self.table_schema:
            autodetect = False
            schema_fields = self.table_schema
        else:
            autodetect = True
            schema_fields = None
        gs_file = general.GSFile(gs_path=self.gs_path, sharded=self.shard_data)
        if self.file_format == "json":
            source_format = NEWLINE_DELIMITED_JSON
        else:
            source_format = CSV
        if serialize_process_list == [] or self.task_id in serialize_process_list:
            try:
                self.log.info(
                    "Executing transfer task {tsk} to file {fl}".format(
                        tsk=self.task_id, fl=gs_file.name
                    )
                )
                self.log.info(
                    "bucket: {b}; file: {f}".format(b=self.bucket, f=gs_file.full_name)
                )
                # Execute MySQLToGCSOperator to transfer data to GCS
                MySQLToGCSOperator(
                    task_id="{}_mysql_to_gcs".format(self.task_id),
                    mysql_conn_id=self.mysql_conn_id,
                    gcp_conn_id=self.gcp_conn_id,
                    sql=self.sql,
                    bucket=self.bucket,
                    filename=gs_file.full_name,
                    schema=schema_fields,
                    dag=self.dag,
                    ensure_utc=self.ensure_utc,
                    export_format="JSON",
                ).execute(context)

                self.log.info(
                    "The file {} has been exported to GCS".format(gs_file.full_name)
                )

                # Execute GCSToBigQueryOperator to load data from GCS to BigQuery
                GCSToBigQueryOperator(
                    task_id="{}_gcs_to_bq".format(self.task_id),
                    bucket=self.bucket,
                    source_objects=gs_file.gs_source,
                    destination_project_dataset_table=destination_dataset_table,
                    schema_fields=schema_fields,
                    write_disposition=self.write_disposition,
                    source_format=source_format,
                    create_disposition=self.create_disposition,
                    ignore_unknown_values=self.ignore_unknown_values,
                    allow_quoted_newlines=self.allow_quoted_newlines,
                    allow_jagged_rows=self.allow_jagged_rows,
                    dag=self.dag,
                    gcp_conn_id=self.gcp_conn_id,
                    autodetect=autodetect,
                ).execute(context)
            except Exception as ex:
                self.log.error(
                    "Could not load data from MySQL {tsk}: {ex}".format(
                        tsk=self.task_id, ex=ex
                    )
                )
                raise
            if self.delete_files_after_import:
                try:
                    GCSDeleteObjectsOperator(
                        bucket_name=self.bucket,
                        prefix="gs://" + gs_file.path,
                        dag=self.dag,
                        google_cloud_storage_conn_id=self.gcp_conn_id,
                    ).execute(context)
                except Exception as ex:
                    self.logger.error(
                        "Could not delete GS files in {}".format("gs://" + gs_file.path)
                    )
            return True


class LoadQueryToTable(BaseOperator):
    """Handles executing the queries into partition
    ``destination_project_id``, ``destination_table_id``, ``query``
    are templated variable for this operator.


    Parameters
    ----------
    bq_conn_id: str, required
        The connection id for big query
    destination_project_id: str, required
        The output project id
    query: str, required
        The sql query that appends to the output table
    destination_table_id: str, required
        The name of the output table for each entity
    write_disposition: str, required
        Write disposition argument for query result destination table
    time_partitioning: dict, optional
        Time partition definition for the output table
    cluster_fields: list, optional
        List of fields to be clustered
    create_disposition: str, optional
        Create disposition argument for query result destination table
    partition: str, optional
        The partition to load the data into, in YYY-MM-DD format
    """

    template_fields = [
        "destination_project_id",
        "destination_table_id",
        "query",
        "partition",
    ]

    def __init__(
        self,
        bq_conn_id,
        query,
        destination_project_id,
        destination_table_id,
        write_disposition,
        time_partitioning=None,
        cluster_fields=None,
        create_disposition="CREATE_IF_NEEDED",
        partition=None,
        *args,
        **kwargs,
    ):
        super(LoadQueryToTable, self).__init__(*args, **kwargs)
        self.bq_conn_id = bq_conn_id
        self.destination_project_id = destination_project_id
        self.query = query
        self.destination_table_id = destination_table_id
        self.write_disposition = write_disposition
        self.create_disposition = create_disposition
        self.time_partitioning = time_partitioning
        self.cluster_fields = cluster_fields
        self.partition = partition

    def execute(self, context):
        bq_hook = BigQueryHook(bigquery_conn_id=self.bq_conn_id, use_legacy_sql=False)
        conn = bq_hook.get_conn()
        cursor = conn.cursor()
        try:
            self.log.info("Executing create query")

            destination_dataset_table = general.gen_bq_dataset_table(
                project_id=self.destination_project_id,
                destination_table_id=self.destination_table_id,
                partition=self.partition,
            )

            cursor.run_query(
                sql=self.query,
                destination_dataset_table=destination_dataset_table,
                create_disposition=self.create_disposition,
                write_disposition=self.write_disposition,
                allow_large_results=True,
                use_legacy_sql=False,
                time_partitioning=self.time_partitioning,
                cluster_fields=self.cluster_fields,
            )
            self.log.info(
                "Succesfully loaded data for {} partition".format(
                    destination_dataset_table
                )
            )
            return True
        except Exception:
            self.log.error(
                "Could not load data for {} partition".format(destination_dataset_table)
            )
            raise


class RunQuery(BaseOperator):
    """Handles retrieving Firehose automated feeds from Sauron.

    This operator drops the temporary output table

    ``query`` is a templated variable for this operator

    Parameters
    ----------
    bq_conn_id: str, required
        The connection id for big query
    query: str, required
        Query to run

    """

    template_fields = [
        "query",
    ]

    def __init__(self, bq_conn_id, query, *args, **kwargs):
        super(RunQuery, self).__init__(*args, **kwargs)
        self.bq_conn_id = bq_conn_id
        self.query = query

    def execute(self, context):
        bq_hook = BigQueryHook(bigquery_conn_id=self.bq_conn_id, use_legacy_sql=False)
        conn = bq_hook.get_conn()
        cursor = conn.cursor()

        try:
            self.log.info("Executing query")
            self.log.info("The query is: \n {}".format(self.query))
            cursor.run_query(sql=self.query, use_legacy_sql=False)
            self.log.info("Succesfully executed query")
        except Exception as ex:
            self.log.error("Could not qun the query: {}".format(ex))
