# **DataOps Excel Plugin**
This code allows you to customize and ingest a dataframe from an excel spreadsheet. This enables you to pull out different information
from different sheets and ignore unimportant miscellaneous cells.

Steps
1. Create a new workbook in your databricks environment. This can be in any location and with any name. Copy the code from dataops-excel.scala to your notebook.
2. Set up [Custom Ingestion](https://intellio.gitbook.io/dataops/user-manual/sdk/custom-ingestion) and connect the sessionIngestion to your source
    1. Configure a custom Connection
    2. Configure the custom source following the directions, set the notebook path to what you specified in step 1.
    3. When setting up a cluster configuration, There are two options:
        1. Creating a singular cluster: Not recommended but can be used for single file testing
        2. Configuring a pool: Is supported in more instances and scales effectively with IDO configurations. See below instructions *Configuing a Pool*   
3. Configure parameters in the source settings for getting excel data. See below *Configuring parameters*
4. Mount the s3 bucket to the DBFS. see below *Mounting S3 Bucket*
5. Pull data and verify wanted functionality


### Configuring a Pool
1. In your Databricks environment select "Compute" -> "Pools"
2. Create Pool
3. Configure the pool
    1. Name - Excel Ingestion
    2. Min Idle - 0     * Note - if you need compute fast, you can increase this number to have warmed up instances
    3. Idle instance termination - {By case decision, 30 is a good amount}
    4. Enable autoscaling local storage
    5. Choose your instance type. See [here](https://aws.amazon.com/ec2/instance-types/) for helping decide what you should use.
    6. Runtime - 7.3 LTS
    7. Availability Zone - Same as where IDO is hosted
    8. On demand/spot composition - All Spot
    9. Max Spot Price - 100   * Note - can be whatever you want if you want more strict cost savings

### Configuring Parameters
There are two parameters you need to set for custom ingestion. The first is related to the pool you will be linking to, and the second will to feed the script.
1. Custom Cluster Params
```
{
"libraries":[
      {"jar":"{intellio dataops-sdk jar location}"},
      {"jar":"{intellio sparky jar location},
      {"maven":{"coordinates":{ most up to date spark excel library}}}
   ],
"new_cluster":
   {"spark_conf":
      {"spark.sql.legacy.timeParserPolicy":"LEGACY",
      "spark.hadoop.fs.s3a.experimental.input.fadvise":"sequential",
      "spark.sql.legacy.avro.datetimeRebaseModeInRead":"CORRECTED",
      "spark.sql.legacy.avro.datetimeRebaseModeInWrite":"CORRECTED"
   },
"num_workers":{can scale to what you need, can default to 2},
"spark_version":"7.3.x-scala2.12",
"aws_attributes": {specify the aws instance profile arn},
"instance_pool_id":"{ID of the pool instance}"}
}
```
1. Finding the pool instance number
    1. Navigate to the pool configuration tab within your Databricks location, at the end following the last '/', there will be an id collection of numbers and letters. That is the id to copy into that location.

2. Custom Parameters
```
{ 
"folderPath": "testing/test/madeUpFolder" //Folder location relative to mounted AWS bucket (REQUIRED)  
"DataRows":"F99", //select up the top left cell that your tabular data works with  (REQUIRED)  
"SheetName":"MySheet1", // sheetname (REQUIRED)  
"FileKey": "name", //key in the filename that shows that that file should be ingested (REQUIRED)
"ShouldArchive":"true", // if you would like your file to be archived once it has been ingested (DEFAULT = FALSE)
"ShouldInferDataTypes": "true" // if you would like datetypes inferred from the file, if not, everything comes in as a string (DEFAULT = FALSE)
"AddColorColumns":"false" // if you would like to include data about cell coloring (DEFAULT = FALSE)
"SourceName": "{source name from ido}" // source that is ingesting the data (REQUIRED) 
"SourceEnvironment": "[dev|prod|test|...]" // Specify whatever environment you are ingesting for (REQUIRED) 
"HeaderRows": // (OPTIONAL)  
  [  
     {"key_reference":"A1","value":"B1"}, //Use key_reference when using a cell on the sheet as the header  
     {"key_custom":"customerName","value":"E1"}, //use key custom when giving the header a custom input  
     ...  
  ]  
}  
```

### Mounting S3 Bucket
You will need to mount your aws bucket to the databricks filesystem. You do this using the mount command from the [filesystem](https://docs.databricks.com/data/data-sources/aws/amazon-s3.html).

### Assumptions and Notes
1. Each source can only load from one sheet
2. The DataRows Column selected is the top left of the tabular data and it will ingest any data below that
3. 'Archive' archives 2 files into the archive bucket: The original file and the dataframe ingested. Both are timestmpaed
4. Hidden Cells are still read in
5. Whatever is shown to the user is what is copied to the dataframe, all formatting included (unless you infer the schema).
6. For Merged data - it reads the upper left hand cell as what it should be reported within.
