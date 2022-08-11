import os

from setuptools import find_packages, setup


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read().strip()


setup(
    name="execute_any_operator",
    version=read("VERSION.txt"),
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "click",
        "apache-airflow-providers-cncf-kubernetes",
        "apache-airflow-providers-apache-hdfs",
        "apache-airflow-providers-apache-hive",
        "apache-airflow-providers-amazon",
    ],
    entry_points={
        "console_scripts": [
            "execute-any-operator = execute_any_operator.entrypoint:cli",
        ],
    },
)
