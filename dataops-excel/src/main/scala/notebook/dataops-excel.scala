import org.apache.spark.sql.{DataFrame,SparkSession}
import com.crealytics.spark.excel._
import com.wmp.intellio.dataops.sdk._
import org.apache.spark.sql.functions._
import play.api.libs.json._

// Create spark session and link session
val spark = SparkSession.builder().getOrCreate()
val session = new IngestionSession("Sandbox","CustomExcelIngestion")

// Substantiate custom parameters
val fileLocation = getStringParameter("FileLocation")
val dataRows = getStringParameter("DataRows")
val sheetName = getStringParameter("SheetName")
val shouldArchive = getStringParameter("ShouldArchive") 
val headerDataRows = (session.customParameters \ "HeaderRows").asOpt[Vector[JsValue]]

def getStringParameter(paramName: String) : String = {
  val incomingParam = (session.customParameters \ paramName).asOpt[String]
  //If its header information, it can be left Empty
  if(incomingParam == None && paramName == "ShouldArchive") return ""
  else if (incomingParam == None){
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
    .option("header", "true")
    .option("dataAddress", "'" + sheetName + "'!" + dataRows)
    .load(fileLocation)

  //Get the FileName and add the file header
  val fileName = fileLocation.split("/")(fileLocation.split("/").size - 1)
  val dfWithHeader = df.withColumn("FileName", lit(fileName)).withColumn("FileName&Sheet", lit(fileName + "/" + sheetName))
  
  val returnedDataSet = getFinalDataSet(headerDataRows, dfWithHeader)
 
  //Write out to archive
  if (shouldArchive != "" && shouldArchive.toBoolean){
    returnedDataSet.write
      .format("com.crealytics.spark.excel")
      .option("header", "true")
      .option("dataAddress", "'" + sheetName + "'!" + dataRows)
      .save(constructExport(fileLocation.split("/"),fileName))
  }
  return returnedDataSet
}

def getFinalDataSet(HeaderData: AnyRef, dfWithHeader: DataFrame) : DataFrame = {
   HeaderData match {
    case None => dfWithHeader
    case _ => HandleHeaderData2(dfWithHeader)
  }
}

//New header Stuff
def HandleHeaderData2(dfWithHeader: DataFrame) : DataFrame = {
  val headerMap = scala.collection.mutable.Map[String,String]()
  val mappedValues = headerDataRows.get.map(x => getValues(x,headerMap))
  //attach the headers
  val headerSeq = "*" :: headerMap.map{case(k,v) => "'" + v + "' as " + k }.toList
  val completeDF = dfWithHeader.selectExpr(headerSeq:_*)
  return completeDF
}

def getValues(Item: JsValue,headerMap: scala.collection.mutable.Map[String, String]){
  val value = (Item \ "value").as[String]
  if((Item \ "key_custom").asOpt[String] != None){
    headerMap.put((Item \ "key_custom").as[String], lookupCell(value))
  }else if ((Item \ "key_reference").asOpt[String] != None){
    headerMap.put(lookupCell((Item \ "key_reference").as[String]), lookupCell(value))
  }else{
    session.log("Header Cell Mapping not correct")
  }
}

def lookupCell(cell: String) : String = {
  val headerDataDF = spark.read
    .format("com.crealytics.spark.excel")
    .option("header", "false")
    .option("dataAddress", "'" + sheetName + "'!" + cell + ":" + cell)
    .load(fileLocation)
  return headerDataDF.first().getString(0)
}

//Export file to archive folder with timestamp
def constructExport(fileLocation: Array[String], fileName: String): String = {
  fileLocation.head match {
    case `fileName` => "archive/" + fileName.split("\\.")(0) + "_" + sheetName + "_" + java.time.LocalDate.now + "_" + java.time.LocalTime.now + "." + fileName.split("\\.")(1)
    case _ => fileLocation.head + "/" + constructExport(fileLocation.tail, fileName)
  } 
}

session.ingest(ingestDf)