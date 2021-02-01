from setuptools import find_packages, setup

setup(
    name="ttierlt",
    packages=find_packages(),
    version="0.1.0",
    description="Batch process MOVES output files to get condensed emission rate look-up tables by district, year, road type, speed, and pollutants.",
    author="TTI-HMP",
    license="MIT",
    install_requires=[
        "click",
        "Sphinx",
        "coverage",
        "awscli",
        "flake8",
        "python-dotenv>=0.5.1",
        "pandas",
        "numpy",
        "mariadb",
        "sqlalchemy",
        "mysql-connector",
        "black",
        "openpyxl",
        "plotly",
        "dash",
        "pytest",
        "scipy",
    ],
    classifiers=[
        "Development Status :: 3 - Alpha",
        # Chose either "3 - Alpha", "4 - Beta" or "5 - Production/Stable" as the current state of your package
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)
