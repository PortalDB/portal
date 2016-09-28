package edu.drexel.cs.dbgroup.temporalgraph.portal

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry,SimpleFunctionRegistry}
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{ExpressionInfo,Expression,ExpressionDescription}

import edu.drexel.cs.dbgroup.temporalgraph.expressions._

//TODO: we may want to switch away from SimpleFunctionRegistry
//if we want to explicitly distinguish between aggregation and
//analytics functions and be able to do lookup by type

object PortalFunctionRegistry {
  val expressions: Map[String, (ExpressionInfo, FunctionBuilder)] = Map(
    expression[PageRank]("pagerank"),
    expression[ConnectedComponents]("components"),
    expression[Any]("any"),
    expression[First]("first"),
    expression[Last]("last"),
    expression[Sum]("sum"),
    expression[Max]("max")
  )

  val builtin: FunctionRegistry = {
    val fr = new SimpleFunctionRegistry
    expressions.foreach { case (name, (info, builder)) => fr.registerFunction(name, info, builder) }
    fr
  }

  // straight up copied over from FunctionRegistry object
  // TODO: can we do this without copying a function over?
  def expression[T <: Expression](name: String)
      (implicit tag: ClassTag[T]): (String, (ExpressionInfo, FunctionBuilder)) = {

    // See if we can find a constructor that accepts Seq[Expression]
    val varargCtor = Try(tag.runtimeClass.getDeclaredConstructor(classOf[Seq[_]])).toOption
    val builder = (expressions: Seq[Expression]) => {
      if (varargCtor.isDefined) {
        // If there is an apply method that accepts Seq[Expression], use that one.
        Try(varargCtor.get.newInstance(expressions).asInstanceOf[Expression]) match {
          case Success(e) => e
          case Failure(e) => throw new PortalException(e.getMessage)
        }
      } else {
        // Otherwise, find an ctor method that matches the number of arguments, and use that.
        val params = Seq.fill(expressions.size)(classOf[Expression])
        val f = Try(tag.runtimeClass.getDeclaredConstructor(params : _*)) match {
          case Success(e) =>
            e
          case Failure(e) =>
            throw new PortalException(s"Invalid number of arguments for function $name")
        }
        Try(f.newInstance(expressions : _*).asInstanceOf[Expression]) match {
          case Success(e) => e
          case Failure(e) => throw new PortalException(e.getMessage)
        }
      }
    }

    val clazz = tag.runtimeClass
    val df = clazz.getAnnotation(classOf[ExpressionDescription])
    if (df != null) {
      (name,
        (new ExpressionInfo(clazz.getCanonicalName, name, df.usage(), df.extended()),
        builder))
    } else {
      (name, (new ExpressionInfo(clazz.getCanonicalName, name), builder))
    }
  }

}
