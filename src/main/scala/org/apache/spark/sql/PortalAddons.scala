package org.apache.spark.sql

import org.apache.spark.sql.catalyst.TableIdentifier

object ModifierWorkaround {
  def makeTableIdentifier(tableName: String): TableIdentifier = TableIdentifier(tableName)

}
