# Custom Ingestion File Watcher Architecture

The below instructions are meant to show configuration for AWS Lambda functions in order to create interactions between IDO / AWS.

1. Create a new AWS Lambda Function using Python
2. Inside of the lambda function home page, add a trigger for the given action that you would like 
3. Role: You will need to configure a role for correct permissions. It will need different permissions depending on your use case - the below has some that are common
    1. EC2 Access
    2. S3 Read Only
    3. RDS Access
    4. Cloudwatch Write
4. VPC config
    1. You will need access to the VPC associated subnets
    2. Will need security group for your given use
5. Enable Monitoring
6. Code
    1. Libraries: Json, urllib.parse, boto3, requests
    2. If you are running queries you will need to conigure the below
       3. Create an [rds client](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds-data.html)
       4. Depending on wanted functionality, configure a sql string
       5. Configure your [execute statement](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds-data.html#RDSDataService.Client.execute_statement)
7. Hit any API's that are needed using the requests lib
8. test / use cloudwatch to debug.
