import org.apache.spark.sql.{DataFrame,SparkSession}
import com.crealytics.spark.excel._
import com.wmp.intellio.dataops.sdk._
import org.apache.spark.sql.functions._

// Create spark session and link session
val spark = SparkSession.builder().getOrCreate()
val session = new IngestionSession("Sandbox","CustomExcelIngestion")

// Substantiate custom parameters - all will error if they havent been passed in - error codes needed
val fileLocation = (session.customParameters \ "FileLocation").asOpt[String].get
val dataRows = (session.customParameters \ "Data-Rows").asOpt[String].get
val dataHasHeaders = (session.customParameters \ "Data-HasHeaders").asOpt[String].get
val sheetName = (session.customParameters \ "SheetName").asOpt[String].get
val metadataRows = (session.customParameters \ "Metadata-Rows").asOpt[String].get
val shouldTranspose = (session.customParameters \ "Metadata-ShouldTranspose").asOpt[String].get

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
  //val fileLocLastElem = fileLocation.split("/").size - 1
  val fileName = fileLocation.split("/")(fileLocation.split("/").size - 1)
  val dfWithMeta = df.withColumn("FileName", lit(fileName)).withColumn("FileName&Sheet", lit(fileName + "/" + sheetName))

  return metadataRows match {
    case "" => dfWithMeta
    case _: (String) => HandleMetaData(dfWithMeta, fileName)
  }
}

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

def AddMetaColumns(allData: DataFrame, count: Int, MetaData: DataFrame): DataFrame = {
  if(count == - 1) allData
  else AddMetaColumns(allData.withColumn(MetaData.columns.toList(count), lit(MetaData.select(MetaData.columns.toList(count)).collect()(0)(0))), count-1, MetaData)
}

def JoinThem(dfs: Array[DataFrame], allData: DataFrame, currDF: Int) : DataFrame = {
  if(currDF == 0) allData
  else JoinThem(dfs , allData.join(dfs(currDF), allData("FileName1") === dfs(currDF)("FileName"), "inner"), currDF - 1)
}

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