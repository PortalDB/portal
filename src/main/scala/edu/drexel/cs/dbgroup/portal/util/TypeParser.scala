package edu.drexel.cs.dbgroup.portal.util

import org.apache.spark.sql.types._
import java.time.LocalDate

object TypeParser {
  val stringPat = "(?i)string".r
  val floatPat = "(?i)float".r
  val intPat = "(?i)(?:int|integer)".r
  val shortPat = "(?i)smallint".r
  val doublePat = "(?i)double".r
  val longPat = "(?i)(?:bigint|long)".r
  val boolPat = "(?i)boolean".r
  val datePat = "(?i)date".r
  
  def parseType(input: String): DataType = {
    input match {
      case stringPat(_*) => StringType
      case floatPat(_*) => FloatType
      case intPat(_*) => IntegerType
      case shortPat(_*) => ShortType
      case doublePat(_*) => DoubleType
      case longPat(_*) => LongType
      case boolPat(_*) => BooleanType
      case datePat(_*) => DateType
    }
  }

  def parseValueByType(input: String, typ: DataType): Any = {
    typ match {
      case StringType => input
      case FloatType => input.toFloat
      case IntegerType => input.toInt
      case ShortType => input.toShort
      case DoubleType => input.toDouble
      case LongType => input.toLong
      case BooleanType => input.toBoolean
      case DateType => LocalDate.parse(input)
    }
  }
}
