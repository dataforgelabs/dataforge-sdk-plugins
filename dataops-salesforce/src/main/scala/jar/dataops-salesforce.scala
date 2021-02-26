package jar

import com.springml.spark.salesforce.Utils._
import com.wmp.intellio.dataops.sdk._
import org.apache.spark.sql.{DataFrame, SparkSession}

object dataopsSalesforce {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().getOrCreate()

    val session = new IngestionSession()

    case class SalesforceFieldSchema(
                                      name: String,
                                      dataType: String,
                                      precision: Int,
                                      scale: Int
                                    )

    def typeMapper(salesforceFieldSchema: SalesforceFieldSchema): String = {
      salesforceFieldSchema.dataType match {
        case "boolean" => "boolean"
        case "checkbox" => "boolean"

        case "int" => "int"

        case "double" => "double"

        case "long" => "long"

        case "decimal" => "decimal(" + salesforceFieldSchema.precision + "," + salesforceFieldSchema.scale + ")"
        case "currency" => "decimal(" + salesforceFieldSchema.precision + "," + salesforceFieldSchema.scale + ")"
        case "percent" => "decimal(" + salesforceFieldSchema.precision + "," + salesforceFieldSchema.scale + ")"
        case "number" => "decimal(" + salesforceFieldSchema.precision + "," + salesforceFieldSchema.scale + ")"

        case "date" => "timestamp"
        case "datetime" => "timestamp"
        case "time" => "timestamp"

        case _ => "string"
      }
    }

    def ingestDf(): DataFrame = {


      val conn = createConnection(
        (session.getSecureConnectionParameters \ "username").as[String],
        (session.getSecureConnectionParameters \ "password").as[String],
        "https://login.salesforce.com",
        "37.0"
      )

      session.log("Connected to Salesforce API", "I")

      val sfObject = (session.customParameters \ "sfObject").as[String]
      val soqlWhereCondition = (session.customParameters \ "soqlWhereClause").asOpt[String]

      val sobjectResults = conn.describeSObject(sfObject)
      val originalFields = sobjectResults.getFields

      session.log("Original fields list: " + originalFields.map(field => field.getName).mkString("Array(", ", ", ")"), "I")

      val fields = originalFields.filter(field => (field.getType.toString != "address") && (field.getType.toString != "location"))

      val fieldProperties = fields.map(field => SalesforceFieldSchema(field.getName, field.getType.toString, field.getPrecision, field.getScale))

      session.log("Field list with compound types removed: " + originalFields.map(field => field.getName).mkString("Array(", ", ", ")"), "I")


      val soqlSelectStatement = fields.foldLeft("") {
        (query, field) => query + field.getName + ","
      }.dropRight(1)

      val soqlQuery = "SELECT " + soqlSelectStatement + " FROM " + sfObject + (if (soqlWhereCondition.nonEmpty) " " + soqlWhereCondition.get else "")

      session.log("Running SOQL query: " + soqlQuery, "I")

      val sfDF = spark.read.
        format("com.springml.spark.salesforce").
        option("username", (session.getSecureConnectionParameters \ "username").as[String]).
        option("password", (session.getSecureConnectionParameters \ "password").as[String]).
        option("soql", soqlQuery).load()


      val sparkSelectExpression = fieldProperties.foldLeft(Seq[String]()) {
        (selectStatements, field) => selectStatements ++ Seq("cast(" + field.name + " as " + typeMapper(field) + ")")
      }


      session.log("Running Select Expression against untyped DataFrame: " + sparkSelectExpression, "I")

      sfDF.selectExpr(sparkSelectExpression: _*)

    }

    session.ingest(ingestDf)
  }
}
