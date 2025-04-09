from setuptools import find_packages, setup

setup(
    name="project_supinfo",
    packages=find_packages(exclude=["project_supinfo_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
