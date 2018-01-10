package edu.drexel.cs.dbgroup.portal.portal

import org.apache.spark.sql.catalyst.trees.TreeNode

//this is just like org.apache.spark.sql.AnalysisException
//which we cannot use because it is package protected
class PortalException(message: String, line:Option[Int] = None, startPosition: Option[Int] = None) extends Exception with Serializable {
  def withPosition(line: Option[Int], startPosition: Option[Int]): PortalException = {
    val newException = new PortalException(message, line, startPosition)
    newException.setStackTrace(getStackTrace)
    newException
  }

  override def getMessage: String = {
    val lineAnnotation = line.map(l => s" line $l").getOrElse("")
    val positionAnnotation = startPosition.map(p => s" pos $p").getOrElse("")
    s"$message;$lineAnnotation$positionAnnotation"
  }

  implicit class PortalAnalysisErrorAt(t: TreeNode[_]) {
    def failAnalysis(msg: String): Nothing = {
      throw new PortalException(msg, t.origin.line, t.origin.startPosition)
    }
  }
}
