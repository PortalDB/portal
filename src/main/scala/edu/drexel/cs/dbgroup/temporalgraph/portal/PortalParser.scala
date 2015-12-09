package edu.drexel.cs.dbgroup.temporalgraph.portal

import scala.language.implicitConversions
import scala.util.parsing.combinator.PackratParsers
import scala.util.parsing.combinator.syntactical.StandardTokenParsers
import scala.util.parsing.combinator.lexical.StdLexical
import scala.util.parsing.input.CharArrayReader.EofCh
import scala.util.matching.Regex
import java.time.LocalDate

import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import edu.drexel.cs.dbgroup.temporalgraph._
import edu.drexel.cs.dbgroup.temporalgraph.plan.{LoadGraph,LoadGraphWithSchema}

object PortalParser extends StandardTokenParsers with PackratParsers {
  //lexical.reserved += ("tselect", "from", "union", "intersection", "min", "max", "sum", "any", "universal", "existential", "directed", "undirected", "vertices", "edges", "group", "by", "with", "return", "compute", "pagerank", "components", "count", "id", "attr", "trend", "year", "month", "day", "start", "end", "where", "and",
    //these are for debugging and testing
//    "materialize")
  //lexical.delimiters ++= List("-", "=", ".")

  protected case class Keyword(str: String) {
    def normalize: String = lexical.normalizeKeyword(str)
    def parser: Parser[String] = normalize
  }

  protected implicit def asParser(k: Keyword): Parser[String] = k.parser

  import lexical.Identifier
  implicit def regexToParser(regex: Regex): Parser[String] = acceptMatch(
    s"identifier matching regex ${regex}",
    { case Identifier(str) if regex.unapplySeq(str).isDefined => str }
  )

  protected val TSELECT = Keyword("TSELECT")
  protected val FROM = Keyword("FROM")
  protected val ALL = Keyword("ALL")
  protected val ANY = Keyword("ANY")
  protected val V = Keyword("V")
  protected val E = Keyword("E")
  protected val HDFS = Keyword("HDFS")
  protected val FILE = Keyword("FILE")
  //TODO: ADD MORE HERE

  protected lazy val reservedWords: Seq[String] = 
    this
      .getClass
      .getMethods
      .filter(_.getReturnType == classOf[Keyword])
      .map(_.invoke(this).asInstanceOf[Keyword].normalize)

  override val lexical = new PortalLexical

  def parse(input: String): LogicalPlan = {
    initLexical
    val tokens = (new PackratReader(new lexical.Scanner(input)))
    val res = phrase(start)(tokens)
    res match {
      case Success(result, _) => result
      case e: NoSuccess => throw new IllegalArgumentException("illformed query, message: " + e.toString)
    }
  }

  protected lazy val initLexical: Unit = lexical.initialize(reservedWords)

  protected def start: Parser[LogicalPlan] = query

  protected lazy val query: Parser[LogicalPlan] = tselect

  protected lazy val tselect: Parser[LogicalPlan] = (
    TSELECT ~> V ~> ";" ~> E ~> FROM ~> path ^^ {
      case dataset => LoadGraph(dataset)
    }
    | TSELECT ~> graphspec ~ (FROM ~> path) ^^ {
      case spec ~ dataset => LoadGraphWithSchema(spec, dataset)
    }
  )

  //poorman's url parse
  protected lazy val path: Parser[String] = {
    protocol ~ "://" ~ repsep(stringLit, "/") ^^ {
      case prot ~ _ ~ dirs => prot + "://" + dirs.mkString("/")
    }
  }

  protected lazy val protocol: Parser[String] = (
    HDFS | FILE
  )

  protected lazy val graphspec: Parser[GraphSpec] = (
    (V ~> "[" ~> repsep(field, ",") <~ "]") ~ (E ~> "[" ~> repsep(field, ",") <~ "]") ^^ {
      case vfields ~ efields => new GraphSpec(vfields, efields)
    }
  )

  protected lazy val field: Parser[StructField] = (
    //FIXME: vid is not nullable and this makes every field nullable
    ident ~ ":" ~ dataType ^^ { case fieldName ~ _ ~ typ => StructField(fieldName, typ) }
  )

  protected lazy val dataType: Parser[DataType] = primitiveType

  protected lazy val primitiveType: Parser[DataType] = (
    "(?i)string".r ^^^ StringType |
    "(?i)float".r ^^^ FloatType |
    "(?i)(?:int|integer)".r ^^^ IntegerType |
    "(?i)tinyint".r ^^^ ByteType |
    "(?i)smallint".r ^^^ ShortType |
    "(?i)double".r ^^^ DoubleType |
    "(?i)(?:bigint|long)".r ^^^ LongType |
    "(?i)binary".r ^^^ BinaryType |
    "(?i)boolean".r ^^^ BooleanType |
    "(?i)decimal".r ^^^ DecimalType.USER_DEFAULT |
    "(?i)date".r ^^^ DateType |
    "(?i)timestamp".r ^^^ TimestampType
  )

/*
  protected lazy val graphspec: Parser[GraphSpec] = {
    V ~ opt(attrspec) ~ ";" ~ E ~ opt(attrspec) ^^ {
      //TODO
    }
  }

  protected lazy val modifer: Parser[AggregateSemantics.Value] = {
    ALL ^^^ AggregateSemantics.Universal
    ANY ^^ AggregateSemantics.Existential
  }

  protected lazy val attrspec: Parser[Seq[StructField]] = {
    "[" ~> repsep(attr, ",") <~ "]"
  }

  protected lazy val attr: Parser[StructField] = {

  }
 */
/*
  lazy val query: PackratParser[Query] = ( expr ~ "return" ~ entity ~ opt(attrStr) ^^ { case g ~ _ ~ e ~ Some(astr) => Return(g, e, astr) 
    case g ~ _ ~ e ~ _ => Return(g, e, Attr()) }
    | expr ~ "materialize" ^^ { case g ~ _ => Materialize(g)}
  )

  lazy val expr: PackratParser[Expression] = ( select ~ "union" ~ "with" ~ semantics ~ function ~ select ^^ { case g1 ~ _ ~ _ ~ sem ~ func ~ g2 => Union(g1, g2, sem, func)}
    | select ~ "intersection" ~ "with" ~ semantics ~ function ~ select ^^ { case g1 ~ _ ~ _ ~ sem ~ func ~ g2 => Intersect(g1, g2, sem, func)}
    | select ^^ { case sel => PlainSelect(sel)}
  )

  lazy val entity = ( "vertices" ^^^ Vertices()
    | "edges" ^^^ Edges()
  )

  lazy val attrStr = ( "." <~ "count" ^^^ Count()
    | "." <~ "id" ^^^ Id()
    | "." <~ "attr" ^^^ Attr()
    | "." <~ "trend" ^^^ Trend()
  )

  lazy val select: PackratParser[Select] = ("select" ~> opt(compute) ~ "from" ~ stringLit ~ opt(where) ~ opt(groupby) ^^ {
    case Some(cmp) ~ _ ~ dataset ~ Some(rng) ~ Some(grp) => new Select(cmp, dataset, rng, grp)
    case _ ~ dataset ~ Some(rng) ~ Some(grp) => new Select(dataset, rng, grp)
    case Some(cmp) ~ _ ~ dataset ~ _ ~ Some(grp) => new Select(cmp, dataset, grp)
    case Some(cmp) ~ _ ~ dataset ~ Some(rng) ~ _ => new Select(cmp, dataset, rng)
    case Some(cmp) ~ _ ~ dataset ~ _ ~ _ => new Select(cmp, dataset)
    case _ ~ dataset ~ Some(rng) ~ _ => new Select(dataset, rng)
    case _ ~ dataset ~ _ ~ Some(grp) => new Select(dataset, grp)
    case _ ~ dataset ~ _ ~ _ => new Select(dataset)
  }
  )

  lazy val semantics = ( "universal" ^^^ Universal()
    | "existential" ^^^ Existential()
  )

  lazy val compute = ( "compute" ~> "pagerank" ~> dir ~ doubleLit ~ doubleLit ~ numericLit ^^ { case dir ~ tol ~ reset ~ numIter => Pagerank(dir, tol, reset, numIter)}
  )

  lazy val doubleLit = ( numericLit ~ "." ~ numericLit ^^ { case num1 ~ _ ~ num2 => (num1 + "." + num2).toDouble} )

  lazy val where = ("where" ~> datecond ~ opt("and" ~> datecond) ^^ { 
    case datec ~ Some(datec2) => new Where(datec, datec2)
    case datec ~ _ => new Where(datec)
  })

  lazy val datecond = ("start" ~> "=" ~> date ^^ { StartDate(_)}
    | "end" ~> "=" ~> date ^^ { EndDate(_)})

  lazy val year = ( numericLit ^^ { _.toInt })
  lazy val other = ( numericLit ^^ { _.toInt })

  lazy val date = ( year ~ ("-" ~> other) ~ ("-" ~> other) ^^ { case y ~ mo ~ d => new Date(y,mo,d) }
  )

  lazy val groupby = ("group" ~> "by" ~> numericLit ~ period ~ "with" ~ semantics ~ function ^^ { case num ~ per ~ _ ~ sem ~ func => new GroupBy(num, per, sem, func)})

  lazy val period = ("year" ^^^ Years()
    | "month" ^^^ Months()
    | "day" ^^^ Days()
  )

  lazy val dir = ( "directed" ^^^ Directed()
            | "undirected" ^^^ Undirected()
  )

  lazy val function = ( "min" ^^^ MinFunc()
                 | "max" ^^^ MaxFunc()
                 | "sum" ^^^ SumFunc()
                 | "any" ^^^ AnyFunc()
  )
 */

}

class PortalLexical extends StdLexical {
  case class FloatLit(chars: String) extends Token {
    override def toString: String = chars
  }

  /* This is a work around to support the lazy setting */
  def initialize(keywords: Seq[String]): Unit = {
    reserved.clear()
    reserved ++= keywords
  }

  /* Normal the keyword string */
  def normalizeKeyword(str: String): String = str.toLowerCase

  delimiters += (
    "@", "*", "+", "-", "<", "=", "<>", "!=", "<=", ">=", ">", "/", "(", ")",
    ",", ";", "%", "{", "}", ":", "[", "]", ".", "&", "|", "^", "~", "<=>"
  )

  protected override def processIdent(name: String) = {
    val token = normalizeKeyword(name)
    if (reserved contains token) Keyword(token) else Identifier(name)
  }

  override lazy val token: Parser[Token] =
    ( identChar ~ (identChar | digit).* ^^
      { case first ~ rest => processIdent((first :: rest).mkString) }
    | digit.* ~ identChar ~ (identChar | digit).* ^^
      { case first ~ middle ~ rest => processIdent((first ++ (middle :: rest)).mkString) }
    | rep1(digit) ~ ('.' ~> digit.*).? ^^ {
        case i ~ None => NumericLit(i.mkString)
        case i ~ Some(d) => FloatLit(i.mkString + "." + d.mkString)
      }
    | '\'' ~> chrExcept('\'', '\n', EofCh).* <~ '\'' ^^
      { case chars => StringLit(chars mkString "") }
    | '"' ~> chrExcept('"', '\n', EofCh).* <~ '"' ^^
      { case chars => StringLit(chars mkString "") }
    | '`' ~> chrExcept('`', '\n', EofCh).* <~ '`' ^^
      { case chars => Identifier(chars mkString "") }
    | EofCh ^^^ EOF
    | '\'' ~> failure("unclosed string literal")
    | '"' ~> failure("unclosed string literal")
    | delim
    | failure("illegal character")
    )

  override def identChar: Parser[Elem] = letter | elem('_')

  override def whitespace: Parser[Any] =
    ( whitespaceChar
    | '/' ~ '*' ~ comment
    | '/' ~ '/' ~ chrExcept(EofCh, '\n').*
    | '#' ~ chrExcept(EofCh, '\n').*
    | '-' ~ '-' ~ chrExcept(EofCh, '\n').*
    | '/' ~ '*' ~ failure("unclosed comment")
    ).*
}


/*
object Interpreter {
  var argNum:Int = 1

  def parseQuery(q: Query) {
    q match {
      case Materialize(graph) =>
        val intRes = parseExpr(graph) match {
          case Left(x) => x
          case Right(x) => x
        }
        val materializeStart = System.currentTimeMillis()
        intRes.materialize
        val materializeEnd = System.currentTimeMillis()
        val total = materializeEnd - materializeStart
        println(f"Materialize Runtime: $total%dms ($argNum%d)")
        argNum += 1
      case Return(graph, entity, attr) =>
        val intRes = parseExpr(graph) match {
          case Left(x) => x
          case Right(x) => x
        }
        val countStart = System.currentTimeMillis()
        var op:String = ""
        entity match {
          case v: Vertices =>
            attr match {
              case c: Count =>
                val count = intRes.vertices.count
                println("Total vertex count: " + count)
                op = "Count"
              case i: Id =>
                println("Vertices:\n" + intRes.vertices.keys.collect.mkString(","))
                op = "Ids"
              case a: Attr =>
                println("Vertices with attributes:\n" + intRes.vertices.collect.mkString("\n"))
                op = "Attrs"
              case t: Trend =>
                val intervals = intRes.getTemporalSequence
                val trendy: TemporalGraph[Double,Double] = intRes match {
                  case te: TemporalGraph[Double,Double] => te
                  case _ => throw new IllegalArgumentException("trying to get trend on a non-analytic")
                }
                println("Vertices with trend:")
                println(trendy.vertices.collect
                  .map(x => (x._1, x._2.map(y => (intervals.indexOf(y._1),y._2))))
                  .map(x => (x._1, LinearTrendEstimate.calculateSlope(x._2)))
                  mkString("\n"))
                op = "Trend"
            }
          case e: Edges =>
            attr match {
              case c: Count =>
                val count = intRes.edges.count
                println("Total edges count: " + count)
                op = "Count"
              case i: Id =>
                println("Edges:\n" + intRes.edges.map(e => (e.srcId, e.dstId)).collect.mkString(","))
                op = "Ids"
              case a: Attr =>
                println("Edges with attributes:\n" + intRes.edges.collect.mkString("\n"))
                op = "Attrs"
              case t: Trend =>
                println("TODO")
                op = "Trend"
            }
        }
        val countEnd = System.currentTimeMillis()
        val total = countEnd - countStart
        println(f"$op Runtime: $total%dms ($argNum%d)")
        argNum += 1
    }
  }

  def parseExpr(expr: Expression): Either[TemporalGraph[String,Int], TemporalGraph[Double,Double]] = {
    expr match {
      case PlainSelect(gr) => {
        parseSelect(gr)
      }
      case Union(g1, g2, sem, func) => {
        val gr1 = parseSelect(g1)
        val gr2 = parseSelect(g2)
        if (gr1.isLeft && gr2.isRight ||
          gr1.isRight && gr2.isLeft) {
          throw new IllegalArgumentException("two graphs are not structurally union-compatible")
        }
        val countStart = System.currentTimeMillis()

        def fun1(s1:String, s2:String): String = {
          func match {
            case su: SumFunc => s1 + s2
            case mi: MinFunc => if (s1.length() > s2.length()) s2 else s1
            case ma: MaxFunc => if (s1.length() < s2.length()) s2 else s1
            case an: AnyFunc => s1
          }
        }
        def fun2(in1:Int, in2:Int): Int = {
          func match {
            case su: SumFunc => in1 + in2
            case mi: MinFunc => math.min(in1, in2)
            case ma: MaxFunc => math.max(in1, in2)
            case an: AnyFunc => in1
          }
        }
        def fun3(d1:Double, d2:Double): Double = {
          func match {
            case su: SumFunc => d1 + d2
            case mi: MinFunc => math.min(d1, d2)
            case ma: MaxFunc => math.max(d1, d2)
            case an: AnyFunc => d1
          }
        }

        if (gr1.isLeft) {
          val res = gr1.left.get.union(gr2.left.get, sem.value, fun1, fun2)
          val countEnd = System.currentTimeMillis()
          val total = countEnd - countStart
          println(f"Union Runtime: $total%dms ($argNum%d)")
          argNum += 1
          Left(res)
        } else {
          val res = gr1.right.get.union(gr2.right.get, sem.value, fun3, fun3)
          val countEnd = System.currentTimeMillis()
          val total = countEnd - countStart
          println(f"Union Runtime: $total%dms ($argNum%d)")
          argNum += 1
          Right(res)
        }
      }
      case Intersect(g1, g2, sem, func) => {
        val gr1 = parseSelect(g1)
        val gr2 = parseSelect(g2)
        if (gr1.isLeft && gr2.isRight ||
          gr1.isRight && gr2.isLeft) {
          throw new IllegalArgumentException("two graphs are not structurally union-compatible")
        }
        val countStart = System.currentTimeMillis()

        def fun1(s1:String, s2:String): String = {
          func match {
            case su: SumFunc => s1 + s2
            case mi: MinFunc => if (s1.length() > s2.length()) s2 else s1
            case ma: MaxFunc => if (s1.length() < s2.length()) s2 else s1
            case an: AnyFunc => s1
          }
        }
        def fun2(in1:Int, in2:Int): Int = {
          func match {
            case su: SumFunc => in1 + in2
            case mi: MinFunc => math.min(in1, in2)
            case ma: MaxFunc => math.max(in1, in2)
            case an: AnyFunc => in1
          }
        }
        def fun3(d1:Double, d2:Double): Double = {
          func match {
            case su: SumFunc => d1 + d2
            case mi: MinFunc => math.min(d1, d2)
            case ma: MaxFunc => math.max(d1, d2)
            case an: AnyFunc => d1
          }
        }

        if (gr1.isLeft) {
          val res = gr1.left.get.intersect(gr2.left.get, sem.value, fun1, fun2)
          val countEnd = System.currentTimeMillis()
          val total = countEnd - countStart
          println(f"Intersection Runtime: $total%dms ($argNum%d)")
          argNum += 1
          Left(res)
        } else {
          val res = gr1.right.get.intersect(gr2.right.get, sem.value, fun3, fun3)
          val countEnd = System.currentTimeMillis()
          val total = countEnd - countStart
          println(f"Intersection Runtime: $total%dms ($argNum%d)")
          argNum += 1
          Right(res)
        }
      }
    }
  }

  def parseSelect(sel: Select): Either[TemporalGraph[String,Int], TemporalGraph[Double,Double]] = {
    val selStart = System.currentTimeMillis()
    var res: TemporalGraph[String, Int] = GraphLoader.loadData(sel.dataset, sel.start,sel.end).persist()
    //FIXME: this is a hack for the experiment
    res.materialize
    val selEnd = System.currentTimeMillis()
    val total = selEnd - selStart
    println(f"Select Runtime: $total%dms ($argNum%d)")
    argNum += 1

    //if there is both group and compute, group comes first
    if (sel.doGroupby) {
      val aggStart = System.currentTimeMillis()
      val func:Function = sel.groupClause.func

      def fun1(s1:String, s2:String): String = {
        func match {
          case su: SumFunc => s1 + s2
          case mi: MinFunc => if (s1.length() > s2.length()) s2 else s1
          case ma: MaxFunc => if (s1.length() < s2.length()) s2 else s1
          case an: AnyFunc => s1
        }
      }
      def fun2(in1:Int, in2:Int): Int = {
        func match {
          case su: SumFunc => in1 + in2
          case mi: MinFunc => math.min(in1, in2)
          case ma: MaxFunc => math.max(in1, in2)
          case an: AnyFunc => in1
        }
      }

      val semant:AggregateSemantics.Value = sel.groupClause.semantics.value
      res = res.aggregate(Resolution.from(sel.groupClause.period), semant, fun1, fun2)
      val aggEnd = System.currentTimeMillis()
      val total = aggEnd - aggStart
      println(f"Aggregation Runtime: $total%dms ($argNum%d)")
      argNum += 1
    }
    if (sel.doCompute)
      Right(compute(res, sel.compute))
    else
      Left(res)
  }

  def compute(gr: TemporalGraph[String,Int], com: Compute): TemporalGraph[Double,Double] = {
    com match {
      case Pagerank(dir, tol, res, numIter) => {
        val prStart = System.currentTimeMillis()
        val result = gr.pageRank(dir.value, tol, res, numIter.toInt)
        val prEnd = System.currentTimeMillis()
        val total = prEnd - prStart
        println(f"PageRank Runtime: $total%dms ($argNum%d)")
        argNum += 1
        result
      }
        /*
         case ConnectedComponents() => {
         val conStart = System.currentTimeMillis()
         val result = gr.connectedComponents()
        val conEnd = System.currentTimeMillis()
        val total = conEnd - conStart
        println(f"ConnectedComponents Runtime: $total%dms ($argNum%d)")
        argNum += 1
        result
         }
         */
    }
  }
}

sealed abstract class Query
case class Return(graph: Expression, ent: Entity, attr: AttrStr) extends Query
case class Materialize(graph: Expression) extends Query

sealed abstract class Expression
case class PlainSelect(sel: Select) extends Expression
case class Union(graph1: Select, graph2: Select, sem: Semantics, func: Function) extends Expression
case class Intersect(graph1: Select, graph2: Select, sem: Semantics, func: Function) extends Expression

sealed abstract class Entity
case class Vertices extends Entity
case class Edges extends Entity

sealed abstract class AttrStr
case class Count extends AttrStr
case class Id extends AttrStr
case class Attr extends AttrStr
case class Trend extends AttrStr

class Select(data: String) {
  val dataset: String = data
  var doGroupby = false
  var doCompute = false
  var compute: Compute = null
  var start = LocalDate.MIN
  var end = LocalDate.MAX
  var groupClause: GroupBy = null

  def this(data: String, grp: GroupBy) = {
    this(data)
    doGroupby = true
    groupClause = grp
  }

  def this(data: String, rng: Where) = {
    this(data)
    start = rng.start
    end = rng.end
  }

  def this(cm: Compute, data: String) = {
    this(data)
    doCompute = true
    compute = cm
  }

  def this(cm: Compute, data: String, rng: Where) = {
    this(data)
    doCompute = true
    compute = cm
    start = rng.start
    end = rng.end
  }

  def this(cm: Compute, data: String, grp: GroupBy) = {
    this(data)
    doGroupby = true
    groupClause = grp
    doCompute = true
    compute = cm
  }

  def this(data: String, rng: Where, grp: GroupBy) = {
    this(data)
    doGroupby = true
    groupClause = grp
    start = rng.start
    end = rng.end
  }

  def this(cm: Compute, data: String, rng: Where, grp: GroupBy) = {
    this(data)
    doCompute = true
    compute = cm
    start = rng.start
    end = rng.end
    doGroupby = true
    groupClause = grp
  }
}

sealed abstract class Semantics {
  def value: AggregateSemantics.Value
}

case class Universal extends Semantics {
  def value() = AggregateSemantics.Universal
}
case class Existential extends Semantics {
  def value() = AggregateSemantics.Existential
}

sealed abstract class Compute
case class Pagerank(dir: Direction, tol: Double, reset: Double, numIter: String) extends Compute

class Where(datec: Datecond) {
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

sealed abstract class Datecond
case class StartDate(datet: Date) extends Datecond
case class EndDate(datet: Date) extends Datecond


class Date(y: Int, m: Int, d: Int) {
  val year:Int = y
  val month:Int = m
  val day:Int = d
  def value():LocalDate = LocalDate.of(year, month, day)
}

class GroupBy(num: String, per: Period, sem: Semantics, fun: Function) {
  val period: String = "P" + num + per.value
  val semantics: Semantics = sem
  val func = fun
}

sealed abstract class Period {
  val value:String
}
case class Months extends Period {
  val value = "M"
}
case class Years extends Period {
  val value = "Y"
}
case class Days extends Period {
  val value = "D"
}

sealed abstract class Direction {
  def value: Boolean
}
case class Directed extends Direction {
  def value() = true
}
case class Undirected extends Direction {
  def value() = false
}

sealed abstract class Function extends Serializable
case class MaxFunc extends Function
case class MinFunc extends Function
case class SumFunc extends Function
case class AnyFunc extends Function

 */
