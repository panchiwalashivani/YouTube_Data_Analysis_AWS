# YouTube_Data_Analysis_AWS

## Project Overview:
This project is dedicated to managing, processing, and analyzing YouTube video data efficiently and securely. By leveraging AWS services and Python technologies, we aim to streamline the data pipeline from ingestion to reporting, enabling insightful analysis of video categories and trending metrics.

## Project Goals:
Data Ingestion: Develop robust mechanisms to ingest data from diverse sources, ensuring seamless integration into our analytics pipeline.
ETL System: Implement a comprehensive Extract, Transform, Load (ETL) system to preprocess raw data into a structured format conducive to analysis.
Data Lake Establishment: Create a centralized data repository utilizing Amazon S3, allowing scalable storage and easy accessibility for analysis.
Scalability: Architect the system to scale effortlessly with the growing volume of data, ensuring consistent performance and reliability.
Cloud Integration: Utilize AWS services including AWS Glue, AWS Lambda, and AWS Athena to harness the power of the cloud for data processing and analysis.
Reporting and Visualization: Develop intuitive dashboards using Amazon QuickSight, empowering stakeholders to derive actionable insights from the data.

## Services Utilized:
Amazon S3: Object storage service providing durability, scalability, and security for our data lake.
AWS Glue: Serverless data integration service for automated ETL workflows and schema discovery.
AWS Lambda: Compute service for executing code in response to events, facilitating serverless data processing.
AWS Athena: Interactive query service enabling ad-hoc querying of data stored in Amazon S3 without the need for infrastructure management.
Amazon QuickSight: Cloud-native business intelligence service for interactive visualization and insightful reporting.

## Dataset:
I utilize the YouTube New Dataset available on Kaggle, comprising daily statistics of popular YouTube videos across various regions. The dataset includes essential metrics such as views, likes, dislikes, comments, and category information, enabling comprehensive analysis of video trends.
https://www.kaggle.com/datasets/datasnaek/youtube-new

## Project Implementation:
The project's implementation involves designing and deploying scalable data pipelines using AWS Glue for ETL, storing data in Amazon S3, and performing ad-hoc queries with AWS Athena. Python and PySpark are utilized for data processing tasks, ensuring efficient transformation and analysis. Finally, insights are visualized and reported using Amazon QuickSight, facilitating data-driven decision-making.
