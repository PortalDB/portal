package edu.drexel.cs.dbgroup.temporalgraph.plans.logical

import java.time.LocalDate

import org.apache.spark.sql.catalyst.plans.logical.{UnaryNode,LogicalPlan}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{StructType,Metadata,StructField}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAlias

import edu.drexel.cs.dbgroup.temporalgraph._
import edu.drexel.cs.dbgroup.temporalgraph.portal.PortalException

case class TGroup(resolution: Resolution, vertexSemantics: AggregateSemantics.Value, edgeSemantics: AggregateSemantics.Value, vertexAggregations: Seq[NamedExpression], edgeAggregations: Seq[NamedExpression], child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = catalog

  protected lazy val catalog: Seq[Attribute] = {
    val vatrs: Seq[StructField] = vertexAggregations.map(f => 
      f match {
        case UnresolvedAlias(child) => StructField(child.prettyName, child.dataType, child.nullable, Metadata.empty)
        case ne: NamedExpression => StructField(ne.name, ne.dataType, ne.nullable, Metadata.empty)
        case other => 
          println("not a named expression or unresolved alias...")
          throw new PortalException("argh")
      }
    )
    val eatrs: Seq[StructField] = edgeAggregations.map(f => 
      f match {
        case UnresolvedAlias(child) => StructField(child.prettyName, child.dataType, child.nullable, Metadata.empty)
        case ne: NamedExpression => StructField(ne.name, ne.dataType, ne.nullable, Metadata.empty)
        case other => 
          println("not a named expression or unresolved alias...")
          throw new PortalException("argh")
      }
    )
    Seq(AttributeReference("V", StructType(vatrs), false, Metadata.empty)(), AttributeReference("E", StructType(eatrs), false, Metadata.empty)())

  }
}

case class TemporalSelect(start: LocalDate, end: LocalDate, child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
}

//analytics sequences include all projections, not just analytics
//because we need to remember the order of attributes
case class VertexAnalytics(vertexAnalytics: Seq[NamedExpression], child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = {
    val vatrs: Seq[StructField] = vertexAnalytics.map(_.toAttribute).map(f => StructField(f.name, f.dataType, f.nullable, f.metadata))
    val eatrs: StructType = child.schema("E").dataType match {
      case st: StructType => st
    }
    Seq(AttributeReference("V", StructType(vatrs), false, Metadata.empty)(), AttributeReference("E", eatrs, false, Metadata.empty)())
  }
}

case class EdgeAnalytics(edgeAnalytics: Seq[NamedExpression], child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = {
    val eatrs: Seq[StructField] = edgeAnalytics.map(_.toAttribute).map(f => StructField(f.name, f.dataType, f.nullable, f.metadata))
    val vatrs: StructType = child.schema("V").dataType match {
      case st: StructType => st
    }
    Seq(AttributeReference("V", vatrs, false, Metadata.empty)(), AttributeReference("E", StructType(eatrs), false, Metadata.empty)())
  }
}

//empty seq means project all out except ids
//the key is just for pretty-print at the moment
case class ProjectVertices(key: String, vertexAttrs: Seq[NamedExpression], child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = {
    val vatrs: Seq[StructField] = vertexAttrs.map(_.toAttribute).map(f => StructField(f.name, f.dataType, f.nullable, f.metadata))
    val eatrs: StructType = child.schema("E").dataType match {
      case st: StructType => st
    }
    Seq(AttributeReference("V", StructType(vatrs), false, Metadata.empty)(), AttributeReference("E", eatrs, false, Metadata.empty)())
  }
}

//the key is just for pretty-print at the moment
case class ProjectEdges(key: String, edgeAttrs: Seq[NamedExpression], child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = {
    val eatrs: Seq[StructField] = edgeAttrs.map(_.toAttribute).map(f => StructField(f.name, f.dataType, f.nullable, f.metadata))
    val vatrs: StructType = child.schema("V").dataType match {
      case st: StructType => st
    }
    Seq(AttributeReference("V", vatrs, false, Metadata.empty)(), AttributeReference("E", StructType(eatrs), false, Metadata.empty)())
  }

}
