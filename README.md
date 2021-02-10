# apache-beam-demo
Apache Beam with BigQuery Demo.

This example generates sample user data and injects it into big query using Beam Direct Runner.

## Requirements 
It is necessary to create the following on GCP or Firebase:
* Google Cloud Project
* Google Bucket that is used as a temporary folder
* Big Query Instance (use the sandbox for example)
* In Big Query, create a Dataset and a User DB with the right schema
* Create a service account with required write accesss rights on big query and google bucket

## Run the application
* Build the application by running **mvn package**
* Run the application using **java -jar** command
