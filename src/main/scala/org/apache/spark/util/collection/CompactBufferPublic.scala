package org.apache.spark.util.collection

import scala.reflect.ClassTag

class CompactBufferPublic[T: ClassTag] extends CompactBuffer[T] {

  override def += (value: T): CompactBufferPublic[T] = super.+=(value).asInstanceOf[CompactBufferPublic[T]]
  override def ++= (values: TraversableOnce[T]): CompactBufferPublic[T] = super.++=(values).asInstanceOf[CompactBufferPublic[T]]
}

object CompactBufferPublic {
  def apply[T: ClassTag](): CompactBufferPublic[T] = new CompactBufferPublic[T]

  def apply[T: ClassTag](value: T): CompactBufferPublic[T] = {
    val buf = new CompactBufferPublic[T]
    buf += value
  }
}

