import setuptools


setuptools.setup(
    name="airmelt_airflow",
    version="0.2.9",
    author="Semeon Balagula @ Airmelt",
    description="Airflow Utilities",
    packages=["airmelt_airflow_operators"],
    install_requires=[
        "urllib3==1.26.18",
        "mysqlclient==2.2.0",
        "apache-airflow==2.7.1",
        "pendulum==2.1.2",
        "apache-airflow-providers-ftp==3.5.0",
        "apache-airflow-providers-google==10.6.0",
        "apache-airflow-providers-amazon==8.13.0",
        "boto3==1.28.0",
        "apache-airflow-providers-http==4.5.0",
        "apache-airflow-providers-imap==3.3.0",
        "astronomer-cosmos==1.2.5",
        "pyYaml==6.0.1",
        "connexion==2.14.2",
        "Werkzeug==2.2.3",
    ],
)
