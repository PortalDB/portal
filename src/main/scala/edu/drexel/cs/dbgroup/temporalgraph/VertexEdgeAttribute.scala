package edu.drexel.cs.dbgroup.temporalgraph

/** 
  Property model.
  Key-value pairs with bag semantics.
  Not meant to be mutable since Graphs are not mutable.
*/
//TODO: it may be more efficient to have a mutable version because of memory churn
trait VertexEdgeAttribute extends Serializable {
  /**
    * How many entries, with bag semantics, which includes duplicates.
    */
  def size: Int
  /**
    * Return all the keys. If a key appears multiple times,
    * it is returned multiple times.
    */
  def keys: Iterable[String]
  /**
    * Return all the values associated with the same key.
    * The order is arbitrary.
    */
  def apply(key: String): Iterable[Any]
  /**
    * Returns true if there is at least one value
    * associated with this key.
    */
  def exists(key: String): Boolean

  //this collection is immutable, so returning a new one for any modification
  /**
    * Return a new collection with all the entries from both. No deduplication.
    */
  def ++(other: VertexEdgeAttribute): VertexEdgeAttribute
  /**
    * Add a key-value pair. If this key already exists, 
    * nothing is overwritten/destroyed. Both are kept.
    */
  def add(key: String, value: Any): VertexEdgeAttribute
  /**
    * Drop all key-value pairs with this key.
    */
  def drop(key: String): VertexEdgeAttribute

}

object VertexEdgeAttribute {
  def empty: VertexEdgeAttribute = new EmptyVEAttribute
}

class EmptyVEAttribute extends VertexEdgeAttribute {
  override def size: Int = 0
  override def keys: Iterable[String] = List[String]()
  override def apply(key: String): Iterable[Any] = List[Any]()
  override def exists(key: String): Boolean = false
  override def ++(other: VertexEdgeAttribute): VertexEdgeAttribute = other
  override def add(key: String, value: Any): VertexEdgeAttribute = throw new UnsupportedOperationException("cannot add to permanently empty VE attribute")
  override def drop(key: String): VertexEdgeAttribute = this
}
