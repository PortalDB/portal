package edu.drexel.cs.dbgroup.temporalgraph.evaluation

import scala.util.parsing.combinator.PackratParsers
import scala.util.parsing.combinator.syntactical.StandardTokenParsers
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel._

import java.time.LocalDate

import org.apache.spark.graphx.Edge

import edu.drexel.cs.dbgroup.temporalgraph._
import edu.drexel.cs.dbgroup.temporalgraph.util.{LinearTrendEstimate, GraphLoader}

object PortalParser extends StandardTokenParsers with PackratParsers {
  lexical.reserved += ("select", "from", "union", "intersection", "min", "max", "sum", "any", "all", "exists", "directed", "undirected", "vertices", "edges", "group", "by", "with", "return", "compute", "pagerank", "degree", "components", "count", "id", "attr", "trend", "list", "ave", "year", "month", "day", "changes", "start", "end", "where", "and", "length", "value", "spaths", "months", "days", "years",
    //these are for debugging and testing
    "materialize")
  lexical.delimiters ++= List("-", "=", ".", "<", ">", "(", ")", "+", ",")
  var strategy = PartitionStrategyType.None
  var width = 8
  def setStrategy(str: PartitionStrategyType.Value):Unit = strategy = str
  def setRunWidth(rw: Int):Unit = width = rw

  def parse(input: String) = {
    println("parsing query " + input)
    val tokens = (new PackratReader(new lexical.Scanner(input)))
    val res = phrase(query)(tokens)
    res match {
      case Success(result, _) => Interpreter.parseQuery(result)
      case e: NoSuccess => throw new IllegalArgumentException("illformed query, message: " + e.toString)
    }
  }

  lazy val query: PackratParser[Query] = ( expr ~ "return" ~ entity ~ opt(attrStr) ^^ { case g ~ _ ~ e ~ Some(astr) => Return(g, e, astr) 
    case g ~ _ ~ e ~ _ => Return(g, e, Attr()) }
    | expr ~ "materialize" ^^ { case g ~ _ => Materialize(g)}
  )

  lazy val expr: PackratParser[Expression] = ( select ~ "union" ~ "with" ~ function ~ function ~ select ^^ { case g1 ~ _ ~ _ ~ func1 ~ func2 ~ g2 => Union(g1, g2, func1, func2)}
    | select ~ "intersection" ~ "with" ~ function ~ function ~ select ^^ { case g1 ~ _ ~ _ ~ func1 ~ func2 ~ g2 => Intersect(g1, g2, func1, func2)}
    | select ^^ { case sel => PlainSelect(sel)}
  )

  lazy val entity = ( "vertices" ^^^ Vertices()
    | "edges" ^^^ Edges()
  )

  lazy val attrStr = ( "." <~ "count" ^^^ Count()
    | "." <~ "id" ^^^ Id()
    | "." <~ "attr" ^^^ Attr()
  )

  lazy val select: PackratParser[Select] = (
    "select" ~> "from" ~> graph ~ compute ^^ { case g ~ cmp => new SCompute(g, cmp) }
      | "select" ~> "from" ~> graph ~ where ^^ { case g ~ w => new SWhere(g, w) }
      | "select" ~> "from" ~> graph ~ groupby ^^ { case g ~ gpb => new SGroupBy(g, gpb) }
      | "select" ~> "from" ~> graph ^^ { case g => new Select(g) }
  )

  lazy val compute = ( "compute" ~> "pagerank" ~> dir ~ doubleLit ~ doubleLit ~ numericLit ^^ { case dir ~ tol ~ reset ~ numIter => Pagerank(dir, tol, reset, numIter)}
    | "compute" ~> "degree" ^^^ Degrees()
    | "compute" ~> "components" ^^^ ConnectedComponents()
    | "compute" ~> "spaths" ~> dir ~ landmarks ^^ { case dir ~ lds => ShortestPaths(dir, lds) }
  )

  lazy val landmarks: PackratParser[Seq[String]] = repsep(numericLit, ",")

  lazy val where = ("where" ~> datecond ~ opt("and" ~> datecond) ^^ { 
    case datec ~ Some(datec2) => new TWhere(datec, datec2)
    case datec ~ _ => new TWhere(datec)
  }
    | "where" ~> attr ~ operation ~ literal ^^ { case a ~ o ~ l => SubWhere(a, o, l) }
  )

  //TODO: add vgroupby
  //TODO: add group by size
  lazy val groupby = ("group" ~> "by" ~> numericLit ~ period ~ "vertices" ~ semantics ~ function ~ "edges" ~ semantics ~ function ^^ { case num ~ per ~ _ ~ vsem ~ vfunc ~ _ ~ esem ~ efunc => new GroupBy(num, per, vsem, vfunc, esem, efunc)})

  lazy val graph = ("(" ~> select <~ ")" ^^ { case s => Nested(s) }
    | stringLit ^^ { case s => DataSet(s) }
  )

  lazy val literal: Parser[String] =
    ( numericLiteral
    | stringLit)

  lazy val numericLiteral: Parser[String] =
    ( integral  
    | sign.? ~ unsignedFloat ^^ { case s ~ f => s + f }
    )

  lazy val unsignedFloat: Parser[String] =
    ( "." ~> numericLit ^^ { u => "0." + u }
      | numericLit ~ "." ~ numericLit ^^ { case n1 ~ n2 => n1 + "." + n2 }
      | numericLit
    )

  lazy val integral: Parser[String] =
    sign.? ~ numericLit ^^ { case s ~ n => s.getOrElse("") + n }

  lazy val sign: Parser[String] = ("+" | "-")

  lazy val attr = ("length" | "value")

  lazy val operation: Parser[String] = (">" | "<" | "=")

  lazy val semantics = ( "all" ^^^ Universal()
    | "exists" ^^^ Existential()
  )

  lazy val doubleLit = ( numericLit ~ "." ~ numericLit ^^ { case num1 ~ _ ~ num2 => (num1 + "." + num2).toDouble} )

  lazy val datecond = ("start" ~> "=" ~> date ^^ { StartDate(_)}
    | "end" ~> "=" ~> date ^^ { EndDate(_)})

  lazy val year = ( numericLit ^^ { _.toInt })
  lazy val other = ( numericLit ^^ { _.toInt })

  lazy val date = ( year ~ ("-" ~> other) ~ ("-" ~> other) ^^ { case y ~ mo ~ d => new Date(y,mo,d) }
  )

  lazy val period = ("year" ^^^ Years()
    | "years" ^^^ Years()
    | "month" ^^^ Months()
    | "months" ^^^ Months()
    | "days" ^^^ Days()
    | "day" ^^^ Days()
    | "changes" ^^^ Changes()
  )

  lazy val dir = ( "directed" ^^^ Directed()
            | "undirected" ^^^ Undirected()
  )

  //TODO: add first/last
  lazy val function = ( "min" ^^^ MinFunc()
                 | "max" ^^^ MaxFunc()
                 | "sum" ^^^ SumFunc()
                 | "any" ^^^ AnyFunc()
                 | "trend" ^^^ TrendFunc()
                 | "list" ^^^ ListFunc()
                 | "ave" ^^^ AverageFunc()
  )


}

object Interpreter {
  var argNum:Int = 1

  def parseQuery(q: Query) {
    q match {
      case Materialize(graph) =>
        val intRes = parseExpr(graph)
        val materializeStart = System.currentTimeMillis()
        intRes.materialize
        val materializeEnd = System.currentTimeMillis()
        val total = materializeEnd - materializeStart
        println(f"Materialize Runtime: $total%dms ($argNum%d)")
        argNum += 1
      case Return(graph, entity, attr) =>
        val intRes = parseExpr(graph)
        val countStart = System.currentTimeMillis()
        var op:String = ""
        entity match {
          case v: Vertices =>
            attr match {
              case c: Count =>
                val count = intRes.vertices.count
                println("Total vertex count: " + count)
                op = "Return"
              case i: Id =>
                println("Vertices:\n" + intRes.vertices.keys.collect.mkString(","))
                op = "Ids"
              case a: Attr =>
                println("Vertices with attributes:\n" + intRes.vertices.collect.mkString("\n"))
                op = "Return"
            }
          case e: Edges =>
            attr match {
              case c: Count =>
                val count = intRes.edges.count
                println("Total edges count: " + count)
                op = "Return"
              case i: Id =>
                println("Edges:\n" + intRes.edges.map{ case (k,v) => k}.collect.mkString(","))
                op = "Return"
              case a: Attr =>
                println("Edges with attributes:\n" + intRes.edges.collect.mkString("\n"))
                op = "Return"
            }
        }
        val countEnd = System.currentTimeMillis()
        val total = countEnd - countStart
        println(f"$op Runtime: $total%dms ($argNum%d)")
        argNum += 1
    }
  }

  def parseExpr(expr: Expression): TGraphNoSchema[Any, Any] = {
    expr match {
      case PlainSelect(gr) => {
        parseSelect(gr)
      }
      case Union(g1, g2, vfunc, efunc) => {
        val gr1 = parseSelect(g1)
        val gr2 = parseSelect(g2)
        val countStart = System.currentTimeMillis()

        val fun1 = (s1:Any, s2:Any) => {
          s1 match {
            case st: String => vfunc match {
              case su: SumFunc => st + s2.toString
              case mi: MinFunc => if (st.length() > s2.toString.length()) s2.toString else st
              case ma: MaxFunc => if (st.length() < s2.toString.length()) s2.toString else s1
              case an: AnyFunc => st
              case _ => throw new IllegalArgumentException("only sum/min/max/any are supported for the union operation")
            }
            case in: Int => vfunc match {
              case su: SumFunc => in + s2.asInstanceOf[Int]
              case mi: MinFunc => math.min(in, s2.asInstanceOf[Int])
              case ma: MaxFunc => math.max(in, s2.asInstanceOf[Int])
              case an: AnyFunc => in
              case _ => throw new IllegalArgumentException("only sum/min/max/any are supported for the union operation")
            }
            case du: Double => vfunc match {
              case su: SumFunc => du + s2.asInstanceOf[Double]
              case mi: MinFunc => math.min(du, s2.asInstanceOf[Double])
              case ma: MaxFunc => math.max(du, s2.asInstanceOf[Double])
              case an: AnyFunc => du
              case _ => throw new IllegalArgumentException("only sum/min/max/any are supported for the union operation")
            }
            case null => null
            case _ => throw new IllegalArgumentException("unsupported data type " + s1.getClass + " in union")
          }
        }
        val fun2 = (s1:Any, s2:Any) => {
          s1 match {
            case st: String => efunc match {
              case su: SumFunc => st + s2.toString
              case mi: MinFunc => if (st.length() > s2.toString.length()) s2.toString else st
              case ma: MaxFunc => if (st.length() < s2.toString.length()) s2.toString else s1
              case an: AnyFunc => st
              case _ => throw new IllegalArgumentException("only sum/min/max/any are supported for the union operation")
            }
            case in: Int => efunc match {
              case su: SumFunc => in + s2.asInstanceOf[Int]
              case mi: MinFunc => math.min(in, s2.asInstanceOf[Int])
              case ma: MaxFunc => math.max(in, s2.asInstanceOf[Int])
              case an: AnyFunc => in
              case _ => throw new IllegalArgumentException("only sum/min/max/any are supported for the union operation")
            }
            case du: Double => efunc match {
              case su: SumFunc => du + s2.asInstanceOf[Double]
              case mi: MinFunc => math.min(du, s2.asInstanceOf[Double])
              case ma: MaxFunc => math.max(du, s2.asInstanceOf[Double])
              case an: AnyFunc => du
              case _ => throw new IllegalArgumentException("only sum/min/max/any are supported for the union operation")
            }
            case null => null
            case _ => throw new IllegalArgumentException("unsupported data type " + s1.getClass + " in union")
          }
        }

        val res = gr1.union(gr2, fun1, fun2).partitionBy(PortalParser.strategy, PortalParser.width).asInstanceOf[TGraphNoSchema[Any,Any]]
        val countEnd = System.currentTimeMillis()
        val total = countEnd - countStart
        println(f"Union Runtime: $total%dms ($argNum%d)")
        argNum += 1
        res
      }
      case Intersect(g1, g2, vfunc, efunc) => {
        val gr1 = parseSelect(g1)
        val gr2 = parseSelect(g2)
        val countStart = System.currentTimeMillis()

        val fun1 = (s1:Any, s2:Any) => {
          s1 match {
            case st: String => vfunc match {
              case su: SumFunc => st + s2.toString
              case mi: MinFunc => if (st.length() > s2.toString.length()) s2.toString else st
              case ma: MaxFunc => if (st.length() < s2.toString.length()) s2.toString else s1
              case an: AnyFunc => st
              case _ => throw new IllegalArgumentException("only sum/min/max/any are supported for the intersection operation")
            }
            case in: Int => vfunc match {
              case su: SumFunc => in + s2.asInstanceOf[Int]
              case mi: MinFunc => math.min(in, s2.asInstanceOf[Int])
              case ma: MaxFunc => math.max(in, s2.asInstanceOf[Int])
              case an: AnyFunc => in
              case _ => throw new IllegalArgumentException("only sum/min/max/any are supported for the intersection operation")
            }
            case du: Double => vfunc match {
              case su: SumFunc => du + s2.asInstanceOf[Double]
              case mi: MinFunc => math.min(du, s2.asInstanceOf[Double])
              case ma: MaxFunc => math.max(du, s2.asInstanceOf[Double])
              case an: AnyFunc => du
              case _ => throw new IllegalArgumentException("only sum/min/max/any are supported for the intersection operation")
            }
            case null => null
            case _ => throw new IllegalArgumentException("unsupported data type " + s1.getClass + " in intersection")
          }
        }
        val fun2 = (s1:Any, s2:Any) => {
          s1 match {
            case st: String => efunc match {
              case su: SumFunc => st + s2.toString
              case mi: MinFunc => if (st.length() > s2.toString.length()) s2.toString else st
              case ma: MaxFunc => if (st.length() < s2.toString.length()) s2.toString else s1
              case an: AnyFunc => st
              case _ => throw new IllegalArgumentException("only sum/min/max/any are supported for the intersection operation")
            }
            case in: Int => efunc match {
              case su: SumFunc => in + s2.asInstanceOf[Int]
              case mi: MinFunc => math.min(in, s2.asInstanceOf[Int])
              case ma: MaxFunc => math.max(in, s2.asInstanceOf[Int])
              case an: AnyFunc => in
              case _ => throw new IllegalArgumentException("only sum/min/max/any are supported for the intersection operation")
            }
            case du: Double => efunc match {
              case su: SumFunc => du + s2.asInstanceOf[Double]
              case mi: MinFunc => math.min(du, s2.asInstanceOf[Double])
              case ma: MaxFunc => math.max(du, s2.asInstanceOf[Double])
              case an: AnyFunc => du
              case _ => throw new IllegalArgumentException("only sum/min/max/any are supported for the intersection operation")
            }
            case null => null
            case _ => throw new IllegalArgumentException("unsupported data type " + s1.getClass + " in intersection")
          }
        }

        val res = gr1.intersection(gr2, fun1, fun2).partitionBy(PortalParser.strategy, PortalParser.width).asInstanceOf[TGraphNoSchema[Any,Any]]
        val countEnd = System.currentTimeMillis()
        val total = countEnd - countStart
        println(f"Intersection Runtime: $total%dms ($argNum%d)")
        argNum += 1
        res
      }
    }
  }

  def parseSelect(sel: Select): TGraphNoSchema[Any,Any] = {
    sel match {
      case SCompute(g, cmp) => {
        val gr = parseGraph(g)
        compute(gr, cmp)
      }
      case SWhere(g, w) => {
        w match {
          case t: TWhere => {
            val gr = parseGraph(g)
            val opStart = System.currentTimeMillis()
            val res = gr.slice(Interval(t.start, t.end)).partitionBy(PortalParser.strategy, PortalParser.width).asInstanceOf[TGraphNoSchema[Any,Any]]
            val opEnd = System.currentTimeMillis()
            val total = opEnd - opStart
            println(f"Slice Runtime: $total%dms ($argNum%d)")
            argNum += 1
            res
          }
          case s: SubWhere => {
            val vp = (vid: Long, attrs: (Interval, Any)) => {
              s.compute(attrs._2)
            }
            val gr = parseGraph(g)
            val opStart = System.currentTimeMillis()
            val res = gr.select(vpred = vp).partitionBy(PortalParser.strategy, PortalParser.width).asInstanceOf[TGraphNoSchema[Any,Any]]
            val opEnd = System.currentTimeMillis()
            val total = opEnd - opStart
            println(f"Subgraph Runtime: $total%dms ($argNum%d)")
            argNum += 1
            res
          }
        }
      }
      case SGroupBy(g, gbp) => {
        val gr = parseGraph(g)
        val opStart = System.currentTimeMillis()
        val spec = gbp.per match {
          case Changes() => ChangeSpec(gbp.num.toInt)
          case _ => TimeSpec(Resolution.from(gbp.period))
        }
        val fun1 = (s1:Any, s2:Any) => {
          s1 match {
            case st: String => gbp.vfun match {
              case su: SumFunc => st + s2.toString
              case mi: MinFunc => if (st.length() > s2.toString.length()) s2.toString else st
              case ma: MaxFunc => if (st.length() < s2.toString.length()) s2.toString else s1
              case an: AnyFunc => st
              case _ => throw new IllegalArgumentException("unsupported function " + gbp.vfun + " on string attribute of vertices")
            }
            case in: Int => gbp.vfun match {
              case su: SumFunc => in + s2.asInstanceOf[Int]
              case mi: MinFunc => math.min(in, s2.asInstanceOf[Int])
              case ma: MaxFunc => math.max(in, s2.asInstanceOf[Int])
              case an: AnyFunc => in
              case _ => throw new IllegalArgumentException("unsupported function " + gbp.vfun + " on int attribute of vertices")
            }
            case du: Double => gbp.vfun match {
              case su: SumFunc => du + s2.asInstanceOf[Double]
              case mi: MinFunc => math.min(du, s2.asInstanceOf[Double])
              case ma: MaxFunc => math.max(du, s2.asInstanceOf[Double])
              case an: AnyFunc => du
              case _ => throw new IllegalArgumentException("unsupported function " + gbp.vfun + " on double attribute of vertices")
            }
            case lt: List[Any] => gbp.vfun match {
              case su: SumFunc => lt ++ s2.asInstanceOf[List[Any]]
              case lf: ListFunc => lt ++ s2.asInstanceOf[List[Any]]
              case an: AnyFunc => lt
              case _ => throw new IllegalArgumentException("only sum/list/any functions are supported with lists")
            }
            case mp: Map[Any,Any] => gbp.vfun match {
              case su: SumFunc => mp ++ s2.asInstanceOf[Map[Any,Any]]
              case td: TrendFunc => mp ++ s2.asInstanceOf[Map[Any,Any]]
              case _ => throw new IllegalArgumentException("only sum and trend functions are supported with maps")
            }
            case tu: Tuple2[Double, Int] => gbp.vfun match {
              case ave: AverageFunc => (tu._1 + s2.asInstanceOf[Tuple2[Double,Int]]._1, tu._2 + s2.asInstanceOf[Tuple2[Double,Int]]._2)
              case an: AnyFunc => tu
              case _ => throw new IllegalArgumentException("only average/any functions are supported with Tuples")
            }
            case null => null
            case _ => throw new IllegalArgumentException("unsupported vertex attribute type in aggregation")
          }
        }
        val fun2 = (s1:Any, s2:Any) => {
          s1 match {
            case st: String => gbp.efun match {
              case su: SumFunc => st + s2.toString
              case mi: MinFunc => if (st.length() > s2.toString.length()) s2.toString else st
              case ma: MaxFunc => if (st.length() < s2.toString.length()) s2.toString else s1
              case an: AnyFunc => st
              case _ => throw new IllegalArgumentException("unsupported function " + gbp.efun + " on string attribute of vertices")
            }
            case in: Int => gbp.efun match {
              case su: SumFunc => in + s2.asInstanceOf[Int]
              case mi: MinFunc => math.min(in, s2.asInstanceOf[Int])
              case ma: MaxFunc => math.max(in, s2.asInstanceOf[Int])
              case an: AnyFunc => in
               case _ => throw new IllegalArgumentException("unsupported function " + gbp.efun + " on int attribute of vertices")           
            }
            case du: Double => gbp.efun match {
              case su: SumFunc => du + s2.asInstanceOf[Double]
              case mi: MinFunc => math.min(du, s2.asInstanceOf[Double])
              case ma: MaxFunc => math.max(du, s2.asInstanceOf[Double])
              case an: AnyFunc => du
              case _ => throw new IllegalArgumentException("unsupported function " + gbp.efun + " on double attribute of vertices")
            }
            case null => null
          }
        }

        val mpd: TGraphNoSchema[Any,Any] = gbp.vfun match {
          case td: TrendFunc => gr.mapVertices((vid, intv, attr) => Map(intv -> attr), Map[Interval,Double]())
          case lt: ListFunc => gr.mapVertices((vid, intv, attr) => List(attr), List[Any]())
          case ave: AverageFunc => gr.mapVertices((vid, intv, attr) => (attr, 1), (0, 1))
          case _ => gr
        }

        val agg = mpd.aggregate(spec, gbp.vsem.value, gbp.esem.value, fun1, fun2)().partitionBy(PortalParser.strategy, PortalParser.width).asInstanceOf[TGraphNoSchema[Any,Any]]

        val res: TGraphNoSchema[Any,Any] = gbp.vfun match {
          case td: TrendFunc => agg.mapVertices((vid, intv, attr) => LinearTrendEstimate.calculateSlopeFromIntervals(attr.asInstanceOf[Map[Interval,Double]]), 0.0)
          case ave: AverageFunc => agg.mapVertices((vid, intv, attr) => {val tp = attr.asInstanceOf[Tuple2[Double,Int]]; tp._1 / tp._2}, 0.0)
          case _ => agg
        }
        val opEnd = System.currentTimeMillis()
        val total = opEnd - opStart
        println(f"Aggregation Runtime: $total%dms ($argNum%d)")
        argNum += 1
        res
      }
      case s: Select => parseGraph(s.graph)
    }
  }

  def parseGraph(g: Graph): TGraphNoSchema[Any,Any] = {
    g match {
      case DataSet(nm) => load(nm)
      case Nested(sel) => parseSelect(sel)
    }
  }

  def load(name: String): TGraphNoSchema[Any,Any] = {
    val selStart = System.currentTimeMillis()
    val res = if (name.endsWith("structure")) {
      GraphLoader.loadStructureOnlyParquet(PortalShell.uri + "/" + name.dropRight("structure".length)).asInstanceOf[TGraphNoSchema[Any,Any]]
    } else
      GraphLoader.loadDataParquet(PortalShell.uri + "/" + name)
    if (PortalShell.warmStart)
      res.materialize
    val selEnd = System.currentTimeMillis()
    val total = selEnd - selStart
    println(f"Load Runtime: $total%dms ($argNum%d)")
    argNum += 1
    res
  }

  def compute(gr: TGraphNoSchema[Any,Any], com: Compute): TGraphNoSchema[Any,Any] = {
    com match {
      case Pagerank(dir, tol, res, numIter) => {
        val prStart = System.currentTimeMillis()
        val result = gr.pageRank(dir.value, tol, res, numIter.toInt).asInstanceOf[TGraphNoSchema[Any,Any]]
        val prEnd = System.currentTimeMillis()
        val total = prEnd - prStart
        println(f"PageRank Runtime: $total%dms ($argNum%d)")
        argNum += 1
        result
      }
      case Degrees() => {
        throw new UnsupportedOperationException("degree not currently supported")
//        val degStart = System.currentTimeMillis()
//        val result = gr.degree()
//        val degEnd = System.currentTimeMillis()
//        val total = degEnd - degStart
//        println(f"Degree Runtime: $total%dms ($argNum%d)")
//        argNum += 1
//        result
      }
      case ConnectedComponents() => {
        val conStart = System.currentTimeMillis()

        val result = gr.connectedComponents().asInstanceOf[TGraphNoSchema[Any,Any]]
        val conEnd = System.currentTimeMillis()
        val total = conEnd - conStart
        println(f"ConnectedComponents Runtime: $total%dms ($argNum%d)")
        argNum += 1
        result
      }
      case ShortestPaths(dir, ids) => {
        val spStart = System.currentTimeMillis()

        val result = gr.shortestPaths(dir.value, ids.map(_.toLong)).asInstanceOf[TGraphNoSchema[Any,Any]]
        val spEnd = System.currentTimeMillis()
        val total = spEnd - spStart
        println(f"ShortestPaths Runtime: $total%dms ($argNum%d)")
        argNum += 1
        result
      }
    }
  }
}

sealed abstract class Query
case class Return(graph: Expression, ent: Entity, attr: AttrStr) extends Query
case class Materialize(graph: Expression) extends Query

sealed abstract class Expression
case class PlainSelect(sel: Select) extends Expression
case class Union(graph1: Select, graph2: Select, vfunc: Function, efunc: Function) extends Expression
case class Intersect(graph1: Select, graph2: Select, vfunc: Function, efunc: Function) extends Expression

sealed abstract class Entity
case class Vertices() extends Entity
case class Edges() extends Entity

sealed abstract class AttrStr
case class Count() extends AttrStr
case class Id() extends AttrStr
case class Attr() extends AttrStr

class Select(data: Graph) extends Serializable {
  val graph: Graph = data
}
case class SCompute(data: Graph, com: Compute) extends Select(data)
case class SWhere(data: Graph, w: Where) extends Select(data)
case class SGroupBy(data: Graph, g: GroupBy) extends Select(data)

sealed abstract class Graph
case class DataSet(name: String) extends Graph
case class Nested(sel: Select) extends Graph

sealed abstract class Compute
case class Pagerank(dir: Direction, tol: Double, reset: Double, numIter: String) extends Compute
case class Degrees() extends Compute
case class ConnectedComponents() extends Compute
case class ShortestPaths(dir: Direction, ids: Seq[String]) extends Compute

sealed abstract class Where
case class TWhere(datec: Datecond) extends Where {
  var start: LocalDate = datec match { 
    case StartDate(dt) => dt.value
    case _ => LocalDate.MIN
  }
  var end: LocalDate = datec match {
    case EndDate(dt) => dt.value
    case _ => LocalDate.MAX
  }

  def this(date1: Datecond, date2: Datecond) = {
    this(date1)
    date2 match {
      case StartDate(dt) => start = dt.value
      case EndDate(dt) => end = dt.value
    }
  }
}
case class SubWhere(attr: String, op: String, va: String) extends Where {
  //this is ugly but it'll do for now
  def compute(in: Any): Boolean = {
    if (in == null) return false
    attr match {
      case "length" => op match {
        case "=" => in.toString.length == va.toInt
        case ">" => in.toString.length > va.toInt
        case "<" => in.toString.length < va.toInt
      }
      //TODO: add handling for > and <
      case "value" => op match {
        case "=" => in.toString == va
      }
    }
  }
}

sealed abstract class Datecond
case class StartDate(datet: Date) extends Datecond
case class EndDate(datet: Date) extends Datecond

class Date(y: Int, m: Int, d: Int) {
  val year:Int = y
  val month:Int = m
  val day:Int = d
  def value():LocalDate = LocalDate.of(year, month, day)
}

case class GroupBy(num: String, per: Period, vsem: Semantics, vfun: Function, esem: Semantics, efun: Function) {
  val period = "P" + num + per.value
}

sealed abstract class Period {
  val value:String
}
case class Months() extends Period {
  val value = "M"
}
case class Years() extends Period {
  val value = "Y"
}
case class Days() extends Period {
  val value = "D"
}
case class Changes() extends Period {
  val value = "C"
}

sealed abstract class Direction {
  def value: Boolean
}
case class Directed() extends Direction {
  def value() = true
}
case class Undirected() extends Direction {
  def value() = false
}

sealed abstract class Function extends Serializable
case class MaxFunc() extends Function
case class MinFunc() extends Function
case class SumFunc() extends Function
case class AnyFunc() extends Function
case class TrendFunc() extends Function
case class AverageFunc() extends Function
case class ListFunc() extends Function

sealed abstract class Semantics {
  def value: Quantification
}

case class Universal() extends Semantics {
  def value() = Always()
}
case class Existential() extends Semantics {
  def value() = Exists()
}



