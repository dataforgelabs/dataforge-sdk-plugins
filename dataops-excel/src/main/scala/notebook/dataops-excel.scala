// Import the neccesary libraries
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.crealytics.spark.excel._
import com.wmp.intellio.dataops.sdk._
import org.apache.spark.sql.functions._
import play.api.libs.json._
import java.io.File
import scala.util.matching._
 
// Create spark session
val spark = SparkSession.builder().getOrCreate()
val session = new IngestionSession()
 
// Substantiate custom parameters coming in from IDO
val folderPath = getStringParameter("FolderPath")
val dataRows = getStringParameter("DataRows")
val sheetName = getStringParameter("SheetName")
val shouldArchive = getStringParameter("ShouldArchive")
val shouldInferDataTypes = getStringParameter("ShouldInferDataTypes")
val fileKey = getStringParameter("FileKey")
val addColorColumns = getStringParameter("AddColorColumns")
val sourceName = getStringParameter("SourceName")
val sourceEnvironment = getStringParameter("SourceEnvironment")
val headerDataRows = (session.customParameters \ "HeaderRows").asOpt[Vector[JsValue]]

// Get all files in a list from the location
val filesFound = dbutils.fs.ls(s"/mnt/{mounted location on databricks file system}/$folderPath").map((_.path)).toList.filter(x => x.contains(fileKey))

// Get the parameters that are strings coming in and default them or error
def getStringParameter(paramName: String) : String = {
  val incomingParam = (session.customParameters \ paramName).asOpt[String]
  
  //If its header information, it can be left Empty
  if(incomingParam == None && (paramName == "ShouldArchive" || paramName == "AddColorColumns" || paramName == "ShouldInferDataTypes")) {
    return "false"
  }
  else if (incomingParam == None){
    session.log("Parameter not substantiated: " + paramName)
  }
  
  return incomingParam.get
}
 
// Send back empty dataframe 
def returnEmpty(): DataFrame = {
  val schema = StructType(Array(StructField("FileName", StringType, true,Metadata.empty)))
  return spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
}

//Main ingestion point
def FilesToIngest(): Unit = {
  // catch in case of edge case 
  if(filesFound.isEmpty){
    return session.ingest(returnEmpty)
  }
  else{
    //Get the first session
    val firstFileDf = ingestDf(filesFound.head)
    session.ingest( () => firstFileDf )
    
    //archive the first session
    if (shouldArchive.toBoolean){
      archive(firstFileDf,filesFound.head)
    }
    
    //ingest and archive any others with the specified environment and source name
    for((fileLocation) <- filesFound.tail){
      val session = new IngestionSession(sourceEnvironment, sourceName)
      val df = ingestDf(fileLocation)
      session.ingest(() => df)
      
      if (shouldArchive.toBoolean){
        archive(df,fileLocation)
      }
    }
  }
}

//Archive the data sources
def archive(returnedDataSet: DataFrame, fileLocation: String): Any = {
  val fileName = fileLocation.split("/")(fileLocation.split("/").size - 1)
  val exportTimeStamped = constructExport(fileLocation.split("/"),fileName)
  val exportRawData = constructMove(fileLocation.split("/"),fileName)
  
  returnedDataSet.write
    .format("com.crealytics.spark.excel")
    .option("header", "true")
    .option("dataAddress", "'" + sheetName + "'!" + dataRows)
    .save(exportTimeStamped)
    
    dbutils.fs.mv(fileLocation, exportRawData)
}
 
// Ingest the Excel Sheet
def ingestDf(fileLocation: String): (DataFrame) = {
  //Start the Session
  session.log("Starting Ingestion")
  
  // Read in the Tabular Data 
  val df = spark.read
    .format("com.crealytics.spark.excel")
    .option("header", "true")
    .option("addColorColumns", addColorColumns)
    .option("usePlainNumberFormat", shouldInferDataTypes)
    .option("inferSchema", shouldInferDataTypes)
    .option("dataAddress", "'" + sheetName + "'!" + dataRows)
    .load(fileLocation)
 
  //Get the FileName and add the file header
  val fileName = fileLocation.split("/")(fileLocation.split("/").size - 1)
  val dfWithHeader = df.withColumn("FileName", lit(fileName)).withColumn("FileName&Sheet", lit(fileName + "/" + sheetName))
  
  val returnedDataSet = getFinalDataSet(headerDataRows, dfWithHeader,fileLocation)
 
  return (returnedDataSet)
}
 
//If the header data is none then return the current DF, else add on the headers
def getFinalDataSet(HeaderData: AnyRef, dfWithHeader: DataFrame, fileLocation: String) : DataFrame = {
   HeaderData match {
    case None => dfWithHeader
    case _ => HandleHeaderData(dfWithHeader,fileLocation)
  }
}
 
//New header Specs
def HandleHeaderData(dfWithHeader: DataFrame, fileLocation: String) : DataFrame = {
  val headerMap = scala.collection.mutable.Map[String,String]()
  val mappedValues = headerDataRows.get.map(x => getValues(x,headerMap,fileLocation))
  //attach the headers
  val headerSeq = "*" :: headerMap.map{case(k,v) => "'" + v + "' as " + k }.toList
  val completeDF = dfWithHeader.selectExpr(headerSeq:_*)
  return completeDF
}
 
//get the values into the map
def getValues(Item: JsValue, headerMap: scala.collection.mutable.Map[String, String], fileLocation: String){
  val value = (Item \ "value").as[String]
  if((Item \ "key_custom").asOpt[String] != None){
    headerMap.put((Item \ "key_custom").as[String], lookupCell(value, fileLocation))
  }else if ((Item \ "key_reference").asOpt[String] != None){
    headerMap.put(lookupCell((Item \ "key_reference").as[String], fileLocation), lookupCell(value, fileLocation))
  }else{
    session.log("Header Cell Mapping not correct")
  }
}
 
def lookupCell(cell: String, fileLocation: String) : String = {
  val headerDataDF = spark.read
    .format("com.crealytics.spark.excel")
    .option("header", "false")
    .option("addColorColumns", addColorColumns)
    .option("usePlainNumberFormat", shouldInferDataTypes)
    .option("inferSchema", shouldInferDataTypes)
    .option("dataAddress", "'" + sheetName + "'!" + cell + ":" + cell)
    .load(fileLocation)
  return headerDataDF.first().getString(0)
}

def constructMove(fileLocation: Array[String], fileName: String): String = {
  fileLocation.head match {
    case `fileName` => "archive/" + fileName.split("\\.")(0) + "_SupplierRaw_" + sheetName + "_" + java.time.LocalDate.now + "_" + java.time.LocalTime.now + "." + fileName.split("\\.")(1)
    case _ => fileLocation.head + "/" + constructMove(fileLocation.tail, fileName)
  } 
}
 
//Export file to archive folder with timestamp
def constructExport(fileLocation: Array[String], fileName: String): String = {
  fileLocation.head match {
    case `fileName` => "archive/" + fileName.split("\\.")(0) + "_" + sheetName + "_" + java.time.LocalDate.now + "_" + java.time.LocalTime.now + "." + fileName.split("\\.")(1)
    case _ => fileLocation.head + "/" + constructExport(fileLocation.tail, fileName)
  } 
}
 
FilesToIngest()
