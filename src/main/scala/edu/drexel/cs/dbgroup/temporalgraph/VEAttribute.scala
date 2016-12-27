package edu.drexel.cs.dbgroup.temporalgraph

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import scala.collection.Iterable


class VEAttribute extends VertexEdgeAttribute {
  private val set: Object2ObjectOpenHashMap[String, Any] = new Object2ObjectOpenHashMap[String, Any]()

  def size: Int = {
    return set.size
  }

  def keys: Iterable[String] = {
    return set.keySet.iterator.asInstanceOf[Iterable[String]]
  }

  def apply(key: String): Any = {
    return set.get(key)
  }

  def exists(key: String): Boolean = {
    return set.containsKey(key)
  }

  override def ++(other: VertexEdgeAttribute): Unit = {
    other.keys.foreach(x => set.put(x, apply(x)));
  }

  def add(key: String, value: Any): Unit =  {
    set.put(key, value)
  }

  def drop(key: String):Unit = {
    set.remove(key)
  }

}
