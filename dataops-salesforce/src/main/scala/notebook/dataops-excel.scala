import org.apache.spark.sql.{DataFrame,SparkSession}
import com.crealytics.spark.excel._
import com.wmp.intellio.dataops.sdk._
import org.apache.spark.sql.functions._

// Create spark session and link session
val spark = SparkSession.builder().getOrCreate()
val session = new IngestionSession("Sandbox","CustomExcelIngestion")

// Substantiate custom parameters
val fileLocation = getParameter("FileLocation")
val shouldTranspose = getParameter("Metadata-ShouldTranspose")
val dataRows = getParameter("Data-Rows")
val dataHasHeaders = getParameter("Data-HasHeaders")
val sheetName = getParameter("SheetName")
val metadataRows = getParameter("Metadata-Rows")
val shouldArchive = getParameter("ShouldArchive") 

def getParameter(paramName: String) : String = {
  val incomingParam = (session.customParameters \ paramName).asOpt[String]
  if(incomingParam == None){
    session.log("Parameter not substantiated: " + paramName)
  }
  incomingParam.get
}

// Ingest the Excel Sheet
def ingestDf(): DataFrame = {
  //Start the Session
  session.log("Starting Ingestion")
  
  // Read in the Tabular Data 
  val df = spark.read
    .format("com.crealytics.spark.excel")
    .option("header", dataHasHeaders)
    .option("dataAddress", "'" + sheetName + "'!" + dataRows)
    .load(fileLocation)

  //Get the FileName and add the file metadata
  val fileName = fileLocation.split("/")(fileLocation.split("/").size - 1)
  val dfWithMeta = df.withColumn("FileName", lit(fileName)).withColumn("FileName&Sheet", lit(fileName + "/" + sheetName))
  
  val returnedDataSet =  metadataRows match {
    case "" => dfWithMeta
    case _: (String) => HandleMetaData(dfWithMeta, fileName)
  }
  
  //Write out to archive
  if (shouldArchive.toBoolean){
    returnedDataSet.write
      .format("com.crealytics.spark.excel")
      .option("header", dataHasHeaders)
      .option("dataAddress", "'" + sheetName + "'!" + dataRows)
      .save(constructExport(fileLocation.split("/"),fileName))
  }
  return returnedDataSet
}

//Export file to archive folder with timestamp
def constructExport(fileLocation: Array[String], fileName: String): String = {
  fileLocation.head match {
    case `fileName` => "archive/" + fileName.split("\\.")(0) + "_" + sheetName + "_" + java.time.LocalDate.now + "_" + java.time.LocalTime.now + "." + fileName.split("\\.")(1)
    case _ => fileLocation.head + "/" + constructExport(fileLocation.tail, fileName)
  } 
}

//Handle the metadata
def HandleMetaData(dfWithMeta: DataFrame, fileName: String): DataFrame = {
  
  val metadataSelections = metadataRows.split(",")
  val metadataDFFirst = getMetaData(metadataSelections(0), fileName, "1")
  metadataSelections.drop(1)

  //Get all of the mremaining metadata DF's
  val metadataDFs = metadataSelections.map(row =>getMetaData(row, fileName))

  //Join them together into one MetaData Frame
  val MetaData = JoinThem(metadataDFs,metadataDFFirst,metadataDFs.size-1).drop("FileName").drop("FileName1")
  return AddMetaColumns(dfWithMeta, MetaData.columns.toList.size - 1, MetaData)
}

//add the columns to the dataframe
def AddMetaColumns(allData: DataFrame, count: Int, MetaData: DataFrame): DataFrame = {
  if(count == - 1) allData
  else AddMetaColumns(allData.withColumn(MetaData.columns.toList(count), lit(MetaData.select(MetaData.columns.toList(count)).collect()(0)(0))), count-1, MetaData)
}

//Join the dataframes
def JoinThem(dfs: Array[DataFrame], allData: DataFrame, currDF: Int) : DataFrame = {
  if(currDF == 0) allData
  else JoinThem(dfs , allData.join(dfs(currDF), allData("FileName1") === dfs(currDF)("FileName"), "inner"), currDF - 1)
}

//Get the meta data and pivot it if needed
def getMetaData(rows: String, fileName: String, added_ending:String = ""): DataFrame = {
  val metadataDF = spark.read
    .format("com.crealytics.spark.excel")
    .option("header", "false")
    .option("dataAddress", "'" + sheetName + "'!" + rows)
    .load(fileLocation)
  if(shouldTranspose.toBoolean) {
    return metadataDF.groupBy().pivot("_c0").agg(first("_c1")).withColumn("FileName" + added_ending, lit(fileName))
  } else {
    return metadataDF.withColumn("FileName" + added_ending, lit(fileName))
  }
}

session.ingest(ingestDf)