# Description #
The airflow_operators repository is a collection of custom Apache Airflow operators designed to streamline and automate data transfer workflows in Google Cloud Platform.
These custom operators enhance the capabilities of Airflow by offering specialized functionality for extracting data from Relational databses, S3 storage and others.
The repository's primary focus is to provide a cohesive set of tools to simplify the ETL (Extract, Transform, Load) process for data engineers and analysts.

# Installation #

`pip install --ignore-installed git+ssh://git@github.com/semeonb/airmelt_airflow.git`

# Contents #

## google_cloud_platform.py ##
This package contains operators to load, manipulate and extract data in Google BigQuery, Google Cloud Storage and other Google Cloud services


# Usage # 
Users can integrate the custom operators from this repository into their Airflow DAGs to seamlessly orchestrate data movement from MSSQL to BigQuery. 
By leveraging the operators' capabilities, data engineers can automate ETL workflows, 
enhance data analytics pipelines, and make informed decisions based on fresh, integrated data in Google BigQuery.

# Contributions #
Contributions from the open-source community are welcome. 
Users can extend the functionality of existing operators or propose new operators 
that align with the goals of simplifying data integration and transformation tasks within the context of Apache Airflow.

