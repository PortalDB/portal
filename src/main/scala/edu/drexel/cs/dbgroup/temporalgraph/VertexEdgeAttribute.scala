package edu.drexel.cs.dbgroup.temporalgraph

import org.apache.spark.sql.types.DataType

/** 
  Inspired by InternalRow code of SparkSQL.
  Not meant to be mutable since Graphs are not mutable.
*/
class VertexEdgeAttribute(values: Array[Any]) {
  /** No-arg constructor for serialization. */ 
  protected def this() = this(null)

  def numFields: Int = values.length

  def genericGet(ordinal: Int) = values(ordinal)
  private def getAs[T](ordinal: Int) = genericGet(ordinal).asInstanceOf[T]
  def isNullAt(ordinal: Int): Boolean = getAs[AnyRef](ordinal) eq null
  def get(ordinal: Int, dataType: DataType): AnyRef = getAs(ordinal)
  def getBoolean(ordinal: Int): Boolean = getAs(ordinal)
  def getByte(ordinal: Int): Byte = getAs(ordinal)
  def getShort(ordinal: Int): Short = getAs(ordinal)
  def getInt(ordinal: Int): Int = getAs(ordinal)
  def getLong(ordinal: Int): Long = getAs(ordinal)
  def getFloat(ordinal: Int): Float = getAs(ordinal)
  def getDouble(ordinal: Int): Double = getAs(ordinal)
  def getString(ordinal: Int): String = getAs(ordinal)
  def getBinary(ordinal: Int): Array[Byte] = getAs(ordinal)

  override def toString: String = {
    if (numFields == 0) {
      "[empty]"
    } else {
      val sb = new StringBuilder
      sb.append("[")
      sb.append(genericGet(0))
      val len = numFields
      var i = 1
      while (i < len) {
        sb.append(",")
        sb.append(genericGet(i))
        i += 1
      }
      sb.append("]")
      sb.toString()
    }
  }

  override def equals(o: Any): Boolean = {
    if (!o.isInstanceOf[VertexEdgeAttribute]) {
      return false
    }

    val other = o.asInstanceOf[VertexEdgeAttribute]
    if (other eq null) {
      return false
    }

    val len = numFields
    if (len != other.numFields) {
      return false
    }

    var i = 0
    while (i < len) {
      if (isNullAt(i) != other.isNullAt(i)) {
        return false
      }
      if (!isNullAt(i)) {
        val o1 = genericGet(i)
        val o2 = other.genericGet(i)
        o1 match {
          case f1: Float if java.lang.Float.isNaN(f1) =>
            if (!o2.isInstanceOf[Float] || ! java.lang.Float.isNaN(o2.asInstanceOf[Float])) {
              return false
            }
          case d1: Double if java.lang.Double.isNaN(d1) =>
            if (!o2.isInstanceOf[Double] || ! java.lang.Double.isNaN(o2.asInstanceOf[Double])) {
              return false
            }
          case _ => if (o1 != o2) {
            return false
          }
        }
      }
      i += 1
    }
    true
  }

  override def hashCode: Int = {
    var result: Int = 37
    var i = 0
    val len = numFields
    while (i < len) {
      val update: Int =
        if (isNullAt(i)) {
          0
        } else {
          genericGet(i) match {
            case b: Boolean => if (b) 0 else 1
            case b: Byte => b.toInt
            case s: Short => s.toInt
            case i: Int => i
            case l: Long => (l ^ (l >>> 32)).toInt
            case f: Float => java.lang.Float.floatToIntBits(f)
            case d: Double =>
              val b = java.lang.Double.doubleToLongBits(d)
              (b ^ (b >>> 32)).toInt
            case other => other.hashCode()
          }
        }
      result = 37 * result + update
      i += 1
    }
    result
  }

}
