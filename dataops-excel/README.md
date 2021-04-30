# **DataOps Excel Plugin**
This code allows you to customize a dataframe from an excel spreadsheet. This enables you to pull out different information
from different sheets and ignore inimportant miscellaneous cells.

Steps
1. Set up [Custom Ingestion](https://intellio.gitbook.io/dataops/configuring-the-data-integration-process/custom-ingestion) and connect the sessionIngestion to your source
2. Copy the code in dataops-excel.scala into a databricks notebook
3. Ensure you have the following libraries
    * https://github.com/crealytics/spark-excel (can be installed with Maven)
    * intellio dataops sdk (will be at the toplevel of the datalake location)
   
4. Configure parameters in the source settings for getting excel data
5. Run the ingestion
6. Verify

{
"DataRows":"A4", //select up the top left cell that your tabular data works with  (REQUIRED)  
"SheetName":"Sheet1", // sheetname (REQUIRED)  
"FileKey": "name", //key in the filename that shows that that file should be ingested (REQUIRED)
"FolderName": "ingestableFiles", //The lowest level folder to ingest files from, if its at the bucket level, specify the bucket name (REQUIRED)  
"ShouldArchive":"true" // if you would like your file to be archived once it has been ingested (DEFAULT = FALSE)  
"HeaderRows": // (OPTIONAL)  
    [  
        {"key_reference":"A1","value":"B1"}, //Use key_reference when using a cell on the sheet as the header  
        {"key_custom":"customerName","value":"E1"}, //use key custom when giving the header a custom input  
        ...  
    ]  
}  

Assumptions and Notes
1. Each source can only load from one sheet
2. The DataRows Column selected is the top left of the tabular data and it will ingest any data below that
3. Archive archives file with a timestamp in an 'archive' folder in the same directory
4. Hidden Cells are still read in
5. Whatever is shown to the user is what is copied to the dataframe, all formatting included
6. Merged data - it reads the upper left hand cell 