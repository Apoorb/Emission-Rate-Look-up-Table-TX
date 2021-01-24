from setuptools import find_packages, setup

setup(
    name="src",
    packages=find_packages(),
    version="0.1.0",
    description="Batch process MOVES output files to get condensed emission rate look-up tables by district, year, road type, speed, and pollutants.",
    author="TTI-HMP",
    license="MIT",
)
