import setuptools


setuptools.setup(
    name="airmelt_airflow",
    version="0.0.3",
    author="Semeon Balagula @ Airmelt",
    description="Airflow Utilities",
    packages=["airmelt_airflow_operators"],
    install_requires=[
        "mysqlclient==2.2.0",
        "apache-airflow==2.7.0",
        "apache-airflow-providers-common-sql==1.7.0",
        "apache-airflow-providers-ftp==3.5.0",
        "apache-airflow-providers-google==10.6.0",
        "apache-airflow-providers-http==4.5.0",
        "apache-airflow-providers-imap==3.3.0",
        "apache-airflow-providers-microsoft-mssql==3.4.2",
        "apache-airflow-providers-sqlite==3.4.3",
        "apache-airflow-providers-mysql==5.2.1",
    ],
)
