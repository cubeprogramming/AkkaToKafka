package no.sysco

import spray.json._
import DefaultJsonProtocol._
import scala.collection.breakOut

class CsvToJson private (val headerLine :String) {

  //#Part of the default constructor
  val jsonKeys = headerLine.split(",")
  //jsonKeys.foreach(value => println(value))
  //#Part of the default constructor

  private[this] var jsValuesMap: List[Map[String, JsValue]] = List.empty[Map[String,JsValue]]


  def convertToJson(tokens: Array[String]) = synchronized {
    val jsonAst = Some(tokens.toJson)
    //println(jsonAst.map(_.prettyPrint))

    jsonAst
  }

  def convertToJson(tokens: Map[String,String]) = synchronized {
    val jsonAst = Some(tokens.toJson)
    //println(jsonAst.map(_.prettyPrint))

    jsonAst
  }

  def convertToJson(line: String) = synchronized {
    Some(getJsonMap(line).toJson)
  }

  def convertToMap(keys: Array[String]) = synchronized {
    keys.map(key => (key,""  ) ).toMap
  }

  def getJsonArray(line: String) = synchronized {
    jsValuesMap = addJsonMapToList(line)
    Some(jsValuesMap.toJson)
  }

  private def getJsonMap(line: String) = {
    val jsonValues = line.split(",")
    //jsonValues.foreach(value => println(value))

    //line => json => map
    val jsonKeyValues: Map[String,String] = (jsonKeys zip jsonValues)(breakOut)
    jsonKeyValues.toJson.asJsObject.fields
  }

  private def addJsonMapToList(line: String) = {

    val jsonMapStructure = List(getJsonMap(line))

    if (jsValuesMap.isEmpty) jsonMapStructure else jsValuesMap ++ jsonMapStructure

  }

}

object CsvToJson {
  def apply(headerLine :String): CsvToJson = new CsvToJson(headerLine)
}
