from distutils.core import setup

setup(
    name='vivian_airflow_extensions',
    version='0.2',
    packages=[
        'vivian_airflow_extensions',
        'vivian_airflow_extensions.hooks',
        'vivian_airflow_extensions.operators',
        'vivian_airflow_extensions.sensors',
    ],
    install_requires=['apache-airflow']
)
