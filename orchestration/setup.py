from setuptools import find_packages, setup

setup(
    name="orchestration",
    packages=find_packages(exclude=["orchestration_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-airbyte"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
