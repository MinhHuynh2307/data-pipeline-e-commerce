from setuptools import find_packages, setup

setup(
    name="e_commerce",
    packages=find_packages(exclude=["e_commerce_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
