# The purpose of this package is to create custom operators for Google Cloud Platform
import logging
from datetime import datetime, timedelta
import time
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airmelt_airflow_operators import general
from google.cloud import bigquery

NEWLINE_DELIMITED_JSON = "NEWLINE_DELIMITED_JSON"
CSV = "CSV"


class BigQuery(object):
    def __init__(self, bq_project_id: str, credentials_path=None, gcp_conn_id=None):
        """
        BigQuery class
        bq_project_id: Name of BQ project
        credentials_path: Path to the credentials file
        """
        self.logger = logging.getLogger(__name__)
        self.bq_project_id = bq_project_id
        if not credentials_path and gcp_conn_id:
            self.logger.info("Using BQ connection id: {}".format(gcp_conn_id))
            bq_hook = BigQueryHook(gcp_conn_id=gcp_conn_id, use_legacy_sql=False)
            self.bq_client = bq_hook.get_client(project_id=self.bq_project_id)
        elif credentials_path:
            self.logger.info("Using credentials path: {}".format(credentials_path))
            self.bq_client = bigquery.Client.from_service_account_json(
                credentials_path, project=self.bq_project_id
            )
        else:
            raise ValueError("Either credentials_path or gcp_conn_id must be provided")

    def create_table(
        self,
        dataset_name,
        table_name,
        schema,
        partition_col_name=None,
        expirtion_days=None,
        restart=False,
    ):
        """
        dataset_name: Name of the dataset
        table_name: Name of the table
        schema: Schema of the table
        partition_col_name: Name of the partition column
        expirtion_days: Number of days to expire the partition
        restart: If True, delete the table and recreate it
        """
        # Get the dataset and table reference
        table_ref = self.bq_client.dataset(dataset_name).table(table_name)

        if restart:
            self.bq_client.delete_table(table_ref, not_found_ok=True)

        # Set the table schema
        if expirtion_days:
            expiration_ms = 86400000 * expirtion_days
        else:
            expiration_ms = None
        # Create the table object
        table_obj = bigquery.Table(table_ref, schema=schema)
        if partition_col_name:
            table_obj.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partition_col_name,
                expiration_ms=expiration_ms,
            )
        # Create the table
        self.bq_client.create_table(table_obj, exists_ok=True)
        return self.bq_client.get_table(table_ref)

    def insert_rows(self, table, rows_to_insert):
        return self.bq_client.insert_rows(table, rows_to_insert)

    def run_query(self, query, use_legacy_sql=False):
        """
        The method runs sql query
        """
        job_config = bigquery.QueryJobConfig()
        job_config.use_legacy_sql = use_legacy_sql
        query_job = self.bq_client.query(query, job_config=job_config)
        return query_job.result()


class InsertRowsOperator(BaseOperator):
    """
    Handles inserting rows into a BigQuery table. The input is a list of dictionaries.

    Parameters
    ----------
    gcp_conn_id: str, required
        The connection id for big query
    destination_project_id: str, required, templated variable
        The output project id
    destination_table : str, required
        The destination Google BigQuery table.
    destination_dataset : str, required
        The destination Google BigQuery dataset.
    task_input: list, required, templated variable
        The list of dictionaries to insert into the table
    restart_table: bool, optional, default: False
        If True, delete the table and recreate it
    """

    template_fields = [
        "destination_dataset",
        "destination_table",
        "destination_project_id",
        "task_input",
    ]

    def __init__(
        self,
        gcp_conn_id,
        destination_project_id,
        destination_dataset,
        destination_table,
        table_schema,
        task_input,
        restart_table=False,
        *args,
        **kwargs,
    ):
        super(InsertRowsOperator, self).__init__(*args, **kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.destination_project_id = destination_project_id
        self.destination_table = destination_table
        self.destination_dataset = destination_dataset
        self.table_schema = table_schema
        self.task_input = task_input
        self.restart_table = restart_table

    def execute(self, context):
        # initialize BigQuery client
        client = BigQuery(self.destination_project_id, gcp_conn_id=self.gcp_conn_id)
        self.log.info(self.task_input)

        # create staging table if it doesn't exist, skip if it does
        table = client.create_table(
            dataset_name=self.destination_dataset,
            table_name=self.destination_table,
            schema=self.table_schema,
            restart=self.restart_table,
        )

        errors = client.insert_rows(table, self.task_input)
        if errors:
            self.log.error(f"Error inserting rows: {errors}")
            raise Exception(f"Error inserting rows: {errors}")
        return True


class LoadQueryToTable(BaseOperator):
    """Handles executing the queries into partition
    ``destination_project_id``, ``destination_table_id``, ``query``
    are templated variable for this operator.


    Parameters
    ----------
    gcp_conn_id: str, required
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
        gcp_conn_id,
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
        self.gcp_conn_id = gcp_conn_id
        self.destination_project_id = destination_project_id
        self.query = query
        self.destination_table_id = destination_table_id
        self.write_disposition = write_disposition
        self.create_disposition = create_disposition
        self.time_partitioning = time_partitioning
        self.cluster_fields = cluster_fields
        self.partition = partition

    def execute(self, context):
        bq_hook = BigQueryHook(bigquery_conn_id=self.gcp_conn_id, use_legacy_sql=False)
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
    gcp_conn_id: str, required
        The connection id for big query
    query: str, required
        Query to run

    """

    template_fields = [
        "query",
    ]

    def __init__(self, gcp_conn_id, query, *args, **kwargs):
        super(RunQuery, self).__init__(*args, **kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.query = query

    def execute(self, context):
        bq_hook = BigQueryHook(gcp_conn_id=self.gcp_conn_id, use_legacy_sql=False)
        conn = bq_hook.get_conn()
        cursor = conn.cursor()

        try:
            self.log.info("Executing query")
            self.log.info("The query is: \n {}".format(self.query))
            cursor.execute(self.query)
            self.log.info("Succesfully executed query")
        except Exception as ex:
            self.log.error("Could not qun the query: {}".format(ex))
        result = cursor.fetchall()
        return result


class WaitForValueBigQueryOperator(BaseOperator):
    """
    Custom Airflow operator that runs a BigQuery query until the specified condition is met.

    Parameters

    sql: str, required
        The SQL query to run
    gcp_conn_id: str, required
        The connection id for big query
    timeout: int, optional
        The time in SECONDS to wait for the condition to be met
    retry_delay: int, optional
        The time in SECONDS to wait between retries
    desired_value: int, optional
        The desired value to be returned by the query
    """

    template_fields = ("sql",)
    ui_color = "#4ea4b8"

    def __init__(
        self,
        sql,
        gcp_conn_id,
        timeout=60,
        desired_value=1,
        retry_delay=10,
        *args,
        **kwargs,
    ):
        super(WaitForValueBigQueryOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.gcp_conn_id = gcp_conn_id
        self.timeout = timeout
        self.desired_value = desired_value
        self.retry_delay = retry_delay

    def execute(self, context):
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=self.timeout)
        success = False

        self.log.info("The query is: \n {}".format(self.sql))

        while datetime.now() < end_time:
            # Run the query using BigQueryHook
            bq_hook = BigQueryHook(gcp_conn_id=self.gcp_conn_id, use_legacy_sql=False)
            conn = bq_hook.get_conn()
            cursor = conn.cursor()
            cursor.execute(self.sql)
            result = cursor.fetchone()

            self.log.info("The result is: \n {}".format(result))

            if result and result[0] == self.desired_value:
                success = True
                break

            self.log.info(
                "Waiting for the desired condition. Retrying in {} seconds...".format(
                    self.retry_delay
                )
            )
            time.sleep(self.retry_delay)

        if success:
            self.log.info("Query returned the desired value.")
        else:
            self.log.error("Timeout reached. Query did not return the desired value.")

        return success
