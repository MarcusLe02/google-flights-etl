# Google Flights ETL Project

## Overview

This repository contains the source code and documentation for the Google Flight ETL project. The project involves daily crawling of flight data using Selenium, storing the data in MySQL, processing it with Apache Spark, storing in HDFS as a data lake, and warehousing the data with Hive. The entire environment is containerized and deployed using Docker.

## System Architecture

![System Architecture](https://github.com/MarcusLe02/google-flights-etl/blob/main/google-flights-pipeline.png)

## Project Structure

## Components

- **Selenium**: Web scraping tool used for extracting flight data from Google Flights.
- **MySQL**: Relational database used for storing raw flight data.
- **Apache Spark**: Distributed data processing engine for data transformation.
- **HDFS**: Distributed file system used as a data lake for storing processed data.
- **Hive**: Data warehousing tool for querying and analyzing structured data.

## Prerequisites

- Docker installed on your machine.
- Python and necessary libraries for Selenium web scraping.

## Getting Started

1. Clone the repository:

    ```bash
    git clone https://github.com/your-username/google_flight_etl.git
    cd google_flight_etl
    ```

2. Build and run the Docker containers:

    ```bash
    cd mysql-hadoop-spark-hive
    docker-compose up -d
    ```

3. Execute the data crawling process:

    ```bash
    python flight_selenium.py
    ```

4. Execute the Hadoop ingestion process:

    ```bash
    docker exec -it namenode bash
    spark-submit --master spark://spark-master:7077 --py-files pyspark-jobs/ingestion.py
    ```

4. Execute the Hive transformation process:

    ```bash
    docker exec -it namenode bash
    spark-submit --master spark://spark-master:7077 --py-files pyspark-jobs/transformation.py
    ```

## Acknowledgments

This project is inspired by the Data Lake & Warehousing demo by Mr. Canh Tran (Data Guy Story). The architecture design, ingestion, and transformation scripts in Spark (Scala) were outlined in a video available [here](https://www.youtube.com/watch?v=Kpl35Q6G4uw&t=770s). I adapted the scripts to use PySpark for implementation.

## Contact

For questions or support, please contact [dong.lenam.2002@gmail.com].

