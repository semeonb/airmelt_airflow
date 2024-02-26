import setuptools


setuptools.setup(
    name="airmelt_airflow",
    version="0.3.5",
    author="Semeon Balagula @ Airmelt",
    description="Airflow Utilities",
    packages=["airmelt_airflow_operators"],
    install_requires=[
        "urllib3==1.26.18",
        "mysqlclient==2.2.0",
        "SQLAlchemy==1.4.49",
        "Flask-SQLAlchemy==2.5.1",
        "Flask-Session==0.5.0",
        "apache-airflow==2.8.1",
        "pendulum==2.1.2",
        "apache-airflow-providers-ftp==3.5.0",
        "apache-airflow-providers-google==10.6.0",
        "apache-airflow-providers-amazon==8.13.0",
        "boto3==1.28.0",
        "apache-airflow-providers-http==4.5.0",
        "apache-airflow-providers-imap==3.3.0",
        "apache-airflow-providers-ssh==3.10.0",
        "astronomer-cosmos==1.3.2",
        "pyYaml==6.0.1",
        "connexion==2.14.2",
        "Werkzeug==2.2.3",
        "pyodbc==5.0.1",
        "paramiko==3.4.0",
        "pydantic==2.3.0",
    ],
)
