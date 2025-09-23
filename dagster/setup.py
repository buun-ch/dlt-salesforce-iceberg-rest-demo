from setuptools import find_packages, setup

setup(
    name="dlt_salesforce",
    packages=find_packages(exclude=["dlt_salesforce_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
