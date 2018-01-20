package edu.drexel.cs.dbgroup.portal

/** 
  Property model.
  Key-value pairs with set semantics.
  Mutable.
*/
trait VertexEdgeAttribute extends Serializable {
    /**
    * How many entries, with set semantics, which does not include duplicates.
    */
  def size: Int
  /**
    * Return all the keys. If a key appears multiple times,
    * it is returned multiple times.
    */
  def keys: Iterable[String]
  /**
    * Return the value associated with the key.
    */
  def apply(key: String): Any
  /**
    * Returns true if there there exists a value associated with the key.
    */
  def exists(key: String): Boolean

  //this collection is mutable, so the instance will be modified and the function does not return anything.
  /**
    * Return a new collection with all the entries from both. No deduplication.
    */
  def ++(other: VertexEdgeAttribute): Unit
  /**
    * Add a key-value pair. If this key already exists, 
    * the value is overridden.
    */
  def add(key: String, value: Any): Unit
  /**
    * Drop the key-value pair with this key.
    */
  def drop(key: String): Unit

}

object VertexEdgeAttribute {
  def empty: VertexEdgeAttribute = new EmptyVEAttribute
}

class EmptyVEAttribute extends VertexEdgeAttribute {
  override def size: Int = 0
  override def keys: Iterable[String] = List[String]()
  override def apply(key: String): Any = null
  override def exists(key: String): Boolean = false
  override def ++(other: VertexEdgeAttribute): Unit = {}
  override def add(key: String, value: Any): Unit = {}
  override def drop(key: String): Unit = {}
}
