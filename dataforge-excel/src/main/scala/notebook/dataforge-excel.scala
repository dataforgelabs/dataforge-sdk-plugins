import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.crealytics.spark.excel._
import com.dataforgelabs.sdk_
import org.apache.spark.sql.functions._
import play.api.libs.json._
import java.io.File
import scala.util.matching._

// Create spark session and link session
val spark = SparkSession.builder().getOrCreate()
val session = new IngestionSession("Sandbox","CustomExcelIngestion")

// Substantiate custom parameters
val folderName = getStringParameter("FolderName")
val dataRows = getStringParameter("DataRows")
val sheetName = getStringParameter("SheetName")
val shouldArchive = getStringParameter("ShouldArchive")
val fileKey = getStringParameter("FileKey")
val headerDataRows = (session.customParameters \ "HeaderRows").asOpt[Vector[JsValue]]
val filesFound = dbutils.fs.ls(s"/mnt/$folderName").map((_.path)).toList.filter(x => x.contains(fileKey) && x.contains(folderName))

def getStringParameter(paramName: String) : String = {
  val incomingParam = (session.customParameters \ paramName).asOpt[String]
  //If its header information, it can be left Empty
  if(incomingParam == None && paramName == "ShouldArchive") return ""
  else if (incomingParam == None){
    session.log("Parameter not substantiated: " + paramName)
  }
  incomingParam.get
}

def ingestAllDFs(): DataFrame = {
  filesFound.isEmpty match {
    case true => returnEmpty() 
    case false => FilesToIngest()
  }
}

// Send back empty dataframe 
def returnEmpty(): DataFrame = {
  val schema = StructType(Array(StructField("FileName", StringType, true,Metadata.empty)))
  return spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
}

def FilesToIngest(): DataFrame = {
  val DFs = filesFound.map(x => ingestDf(x))
  return UnionDFs(DFs.head, DFs.tail)
}

def UnionDFs(df: DataFrame, DFs: List[DataFrame]): DataFrame = DFs match {
  case Nil => df
  case x :: xs => UnionDFs(df.union(x),xs)
}

// Ingest the Excel Sheet
def ingestDf(fileLocation: String): DataFrame = {
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
  
  val returnedDataSet = getFinalDataSet(headerDataRows, dfWithHeader,fileLocation)
 
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

//If the header data is none then return the current DF, else add on the headers
def getFinalDataSet(HeaderData: AnyRef, dfWithHeader: DataFrame, fileLocation: String) : DataFrame = {
   HeaderData match {
    case None => dfWithHeader
    case _ => HandleHeaderData(dfWithHeader,fileLocation)
  }
}

//New header Stuff
def HandleHeaderData(dfWithHeader: DataFrame, fileLocation: String) : DataFrame = {
  val headerMap = scala.collection.mutable.Map[String,String]()
  val mappedValues = headerDataRows.get.map(x => getValues(x,headerMap,fileLocation))
  //attach the headers
  val headerSeq = "*" :: headerMap.map{case(k,v) => "'" + v + "' as " + k }.toList
  val completeDF = dfWithHeader.selectExpr(headerSeq:_*)
  return completeDF
}

//get the values into the map
def getValues(Item: JsValue,headerMap: scala.collection.mutable.Map[String, String], fileLocation: String){
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

session.ingest(ingestAllDFs)