package com.apple.ist.gbi.fraud.ares


import org.apache.spark.sql._
import org.apache.spark.sql.types.{MapType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.udf
import org.json4s.jackson.JsonMethods.parse
import org.json4s._
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.functions._

import scala.util.control.Breaks.{break, breakable}

case class Test1 (
                   alias: Column, // e.g. store_name
                   contextKey: Column, // e.g. data.store.name
                   contextType: String, // Simple, Array or Map
                   substringLength: Column,
                   lobTypeCode: Column, // live or val
                   columnType: Column, // request, workflow or others
                   aggregation: Column, // e.g. sum, count etc. or NA
                   wildcard: Boolean // true if key contains [%] or [0-9]
                 )


object MyHelloWorld {

  val EndsWithSemiColonPattern = """(.*?)(;*)$""".r

  def main (args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("MyHelloWorld")
      .master("local[2]")
      .getOrCreate()

    //val s1=filterByCharBytes("Value of Test",4)

   // val str1="java.util.concurrent.TimeoutException: did not receive response within 20 MILLISECONDS\\tat com.apple.athena.util.UpdateableFuture.get(UpdateableFuture.java:31)\\tat com.google.common.util.concurrent.Future123"

    //codePoints

    //print(str1.substring(0,200))


   // println("Value of bytecode"+s1)


    val sampleJSONFile = "src/main/resources/data/sample_json_2"

    val workflowsInnerSchema = (new StructType).add("exceptions", StringType, true)
    val workflowsSchema = (new StructType).add("live", workflowsInnerSchema, true)
     // val customSchema = (new StructType).add("workflows", workflowsSchema)


    //print("Value of ---->"+jsonToMapF(rawData))

    val customSchema = StructType(
      Array(
        StructField("uuid", StringType, true),
        StructField("source", StringType, true),
        StructField("id", StringType, true),
        StructField("workflows", workflowsSchema, true)
      )
    )

   /* val jsonRDD = spark.sparkContext.wholeTextFiles(sampleJSONFile).map(x => x._2)

    jsonRDD.foreach(print)*/

    val rawDF  = spark.read
//      .format("text")
      //.schema(customSchema)
//      .option("multiline",true)
//      .option("mode", "DROPMALFORMED")
//      .load(Seq(sampleJSONFile): _*)
      .text(sampleJSONFile)


   val raw2Txt= rawDF.withColumn("filterTxt", get_json_object(col("value"), "$.data.svc" ))

    raw2Txt.select(col("value").alias("Test")).show(false)

    val FilterTxtLob = Map("idms_iforgot" -> Map("filterTxt" -> "$.data.SVC", "filterTxtList" -> Seq("authentication:processResetPasswordByCR", "authentication:processGenerateConfirmCode", "authentication:processGenerateEmailConfirmCode")))

    //val Keys= FilterTxtLob(0)

    for((k,v)<-FilterTxtLob){
      print("Value of key",k)

      val symb =v.getClass
      println("Value of val type",symb)
      print("Value of txt",v("filterTxt"))
      print("Value of filter txt list",v("filterTxtList"))
      val raw1=rawDF.filter(get_json_object(col("value"),v("filterTxt").toString ).isin(v("filterTxtList").asInstanceOf[Seq[String]]: _*))

      raw1.show(false)
    }


    raw2Txt.printSchema()
    raw2Txt.select(col("filterTxt")).show(false)

    val newDF=rawDF.withColumn("root", from_json(col("value"), customSchema))
    //rawDF.withColumn("test",jsonToMap(lit("workflows"))).show(false)
    newDF.select("root.workflows.live.exceptions").show(false)

   val df1= newDF.withColumn("Test",jsonToMap(col("root.workflows.live.exceptions")))

    df1.select("Test").show(false)

    val alias = "Test"
    val contextKey = "workflows.live.exceptions"
    val contextType = "simple"
    val substringLength = 0
    val lobTypeCd = "live"
    val aggregationTxt = "NA"
    val colType = "request"

    val r = Test1(
      lit(alias),
      lit(contextKey),
      contextType,
      lit (substringLength),
      lit (lobTypeCd),
      lit(colType),
      lit (aggregationTxt),
      wildcard = false
    )


    //removeBackslash()
  spark.stop()
  }


  def trimSuffixes(str: String, suffixChar: Char): String = {
    var trimmedStr = ""
    val strLength = str.length
    breakable {
      for (i <- (0 until strLength).reverse) {
        if (str(i) != suffixChar) {
          trimmedStr = str.substring(0, i+1)
          break
        }
      }
    }
    trimmedStr
  }


  val jsonToMap1 = udf{(json: String) =>
    implicit val formats = org.json4s.DefaultFormats
    try {
      val mapper = new ObjectMapper
      mapper.registerModule(DefaultScalaModule)
      mapper.readValue(json, classOf[Map[String, String]])
    } catch {
      case ex: Exception => Map[String, String]()
    }
  }

  def jsonToMapF (str: String):Map[String,String] ={
    val mapper = new ObjectMapper
    mapper.registerModule(DefaultScalaModule)
    mapper.readValue(str, classOf[Map[String, String]])
  }

  def removeBackslashF: String => String = _.replaceAll("\\", "")

  val jsonToMap = udf{(json: String) =>
    implicit val formats = org.json4s.DefaultFormats
    try {
      parse(json).extract[Map[String, String]]
    } catch {
      case ex: Exception => Map[String, String]()
    }
  }


  def filterByCharBytes(input: String, byteLimit: Int, substrLength:Int = 100000): String = {
    var length = 0
    val codePointsArray = (0 to input.codePointCount(0, input.length)).map(c => input.offsetByCodePoints(0, c)).sliding(2).map(w => input.substring(w.head, w.last)).toArray

    val sb = new StringBuilder

    breakable {
      for (i <- 0 until codePointsArray.length) {
        val str = codePointsArray(i)
        val count = str.getBytes(java.nio.charset.StandardCharsets.UTF_8).length
        //      println(s" $str --> $count")
        if (count < byteLimit) {
          sb.append(str)
          length += 1
        }
        if (length > substrLength - 1) {
          break
        }
      }
    }
    sb.toString
  }

  def makeJsonArrayFormat(str: String) = {
    implicit val formats = org.json4s.DefaultFormats
    val doubleQuote = """""""
    val arr = str.replaceAll(doubleQuote, "").stripSuffix("]").stripPrefix("[").split(",")
    val doubleQuotedString = arr.map(word => doubleQuote + word + doubleQuote).mkString(",")
    s"[$doubleQuotedString]"
  }

}
//|-- exceptions: string (nullable = true)
