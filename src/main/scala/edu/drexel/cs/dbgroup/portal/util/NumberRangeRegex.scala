//generate a file globbing regex for an inclusive range of two ints.
package edu.drexel.cs.dbgroup.portal.util

object NumberRangeRegex {

  def getFirstDigitAndRest(num:String): (String,String) = {
    if (num.length > 1)
      (num.head.toString, num.tail)
    else if (num.length == 1)
      (num.head.toString, "")
    else
      ("","")
  }

  def generateHead(first:String, zeros:String, rest:String, bound:String): String = {
    var parts:List[String] = List()
    generateToBound(rest, bound).split(',').foreach {x =>
      parts = parts :+ (first + zeros + x)
    }
    parts.mkString(",")
  }

  def stripLeftRepeatedDigit(num:String, digit:String): (String,String) = {
    for ((ch, i) <- num.zipWithIndex) {
      if (ch != digit)
        return (digit * i, num.drop(i))
    }
    (num, "")
  }

  def generateToBound(num:String, bound:String): String = {
    if (num == "")
      return ""
    val noRangeExit:String = if (bound == "lower") "0" else "9"
    if (num.length == 1 && num.toInt == noRangeExit.toInt)
      return noRangeExit
    if (num.length == 1 && num.toInt >= 0 && num.toInt < 10)
      if (bound == "lower")
        return "[0-" + num + "]"
      else
        return "[" + num + "-9]"

    val (firstDigit, rest) = getFirstDigitAndRest(num)
    val (repeated, rest2) = stripLeftRepeatedDigit(rest, noRangeExit)
    val head = generateHead(firstDigit, repeated, rest2, bound)
    var tail = ""
    if (bound == "lower") {
      if (firstDigit.toInt > 1) {
        tail = "[0-" + (firstDigit.toInt - 1) + "]"
        tail += "[0-9]" * (num.length - 1)
      } else if (firstDigit.toInt == 1) {
        tail = "0" + ("[0-9]" * (num.length - 1))
      }
    } else {
      if (firstDigit.toInt < 8) {
        tail = "[" + (firstDigit.toInt + 1) + "-9]"
        tail += "[0-9]" * (num.length - 1)
      } else if (firstDigit.toInt == 8) {
        tail = "9" + "[0-9]" * (num.length - 1)
      }
    }
    if (tail != "")
      head + "," + tail
    else
      head
  }

  def extractCommon(min: String, max: String): (String, String, String, String, String) = {
    var fdrMin = getFirstDigitAndRest(min)
    var fdrMax = getFirstDigitAndRest(max)
    var common = ""
    while (fdrMin._1 == fdrMax._1 && fdrMin._1 != "") {
      common += fdrMin._1
      fdrMin = getFirstDigitAndRest(fdrMin._2)
      fdrMax = getFirstDigitAndRest(fdrMax._2)
    }
    (common, fdrMin._1, fdrMin._2, fdrMax._1, fdrMax._2)
  }

  def generateForSameLenNr(min: String, max: String): String = {
    val (common, fdMin, restMin, fdMax, restMax) = extractCommon(min, max)
    var starting, ending = ""
    if (restMin != "")
      starting = generateToBound(restMin, "upper").split(',').map(x => common + fdMin + x).mkString(",")
    else
      starting = common + fdMin

    if (restMax != "")
      ending = generateToBound(restMax, "lower").split(',').map(x => common + fdMax + x).mkString(",")
    else
      ending = common + fdMax

    if (fdMin != "" && fdMax != "" && (fdMin.toInt + 1) > (fdMax.toInt -1))
      return starting + "," + ending

    var middle: String = ""
    if (fdMin != "" && fdMax != "" && (fdMin.toInt + 1 ) == (fdMax.toInt - 1))
      middle = common + (fdMin.toInt + 1)
    else if (fdMin != "" && fdMax != "")
      middle = common + "[" + (fdMin.toInt + 1) + "-" + (fdMax.toInt - 1) + "]"
    else
      middle = common

    middle += "[0-9]" * restMin.length

    starting + "," + middle + "," + ending
  }

  def generateRegex(minIn: Int, maxIn: Int): String = {
    if (minIn == maxIn)
      return minIn.toString

    val min: String = minIn.toString
    val max: String = maxIn.toString

    val nrDigMin = min.length
    val nrDigMax = max.length
    if (nrDigMin != nrDigMax) {
      var middleParts = List[String]()
      var i = nrDigMin
      while (i > nrDigMax) {
        middleParts = middleParts :+ "[1-9]" + ("[0-9]" * i)
        i += 1
      }
      val middle = middleParts.mkString(",")
      val starting = generateToBound(min, "upper")
      val ending = generateForSameLenNr("1" + "0" * (max.length - 1), max)
      if (middle != "")
        starting + "," + middle + "," + ending
      else
        starting + "," + ending
    } else
      generateForSameLenNr(min, max)
  }

  def main(args: Array[String]) {
    println(generateRegex(args(0).toInt, args(1).toInt))
  }

}

