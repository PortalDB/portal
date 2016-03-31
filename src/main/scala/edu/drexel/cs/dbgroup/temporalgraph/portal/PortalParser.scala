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
import org.apache.spark.sql.catalyst.analysis.{UnresolvedFunction,UnresolvedAttribute,UnresolvedAlias}
import org.apache.spark.sql.catalyst.expressions._

import edu.drexel.cs.dbgroup.temporalgraph._
import edu.drexel.cs.dbgroup.temporalgraph.plans.logical._

object PortalParser extends StandardTokenParsers with PackratParsers {
  protected case class Keyword(str: String) {
    def normalize: String = lexical.normalizeKeyword(str)
    def parser: Parser[String] = normalize
  }

  protected implicit def asParser(k: Keyword): Parser[String] = k.parser

  import lexical.Identifier
  implicit def regexToParser(regex: Regex): Parser[String] = acceptMatch(
    s"identifier matching regex ${regex}", {
      case Identifier(str) if regex.unapplySeq(str).isDefined => str
      case lexical.Keyword(str) if regex.unapplySeq(str).isDefined => str
    }
  )

  protected val ALL = Keyword("ALL")
  protected val AND = Keyword("AND")
  protected val ANY = Keyword("ANY")
  protected val AS = Keyword("AS")
  protected val BY = Keyword("BY")
  protected val E = Keyword("E")
  protected val END = Keyword("END")
  protected val FALSE = Keyword("FALSE")
  protected val FROM = Keyword("FROM")
  protected val START = Keyword("START")
  protected val TAND = Keyword("TAND")
  protected val TGROUP = Keyword("TGROUP")
  protected val TOR = Keyword("TOR")
  protected val TRUE = Keyword("TRUE")
  protected val TSELECT = Keyword("TSELECT")
  protected val TWHERE = Keyword("TWHERE")
  protected val V = Keyword("V")
  protected val VID = Keyword("VID")
  protected val VID1 = Keyword("VID1")
  protected val VID2 = Keyword("VID2")
  protected val SIZE = Keyword("SIZE")

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

  protected lazy val query: Parser[LogicalPlan] = tselect | tload

  protected lazy val tload: Parser[LogicalPlan] = (
    TSELECT ~> graphspec ~ (FROM ~> stringLit) ~
      twhere.? ^^ {
        case spec ~ dataset ~ tw =>
          val twh: TWhere = tw.getOrElse(TWhere(StartDate(Date(-999999,1,1))))
          LoadGraphWithSchema(spec, dataset, twh.start, twh.end)
      }
  )

  protected lazy val tselect: Parser[LogicalPlan] =
    TSELECT ~> semantics.? ~ (V ~> vproj.?) ~ ";" ~ 
      semantics.? ~ (E ~> eproj.?) ~
      (FROM ~> tgraph) ~
      twhere.? ~
      tgroup.? ^^ {
        case vsem ~ vproj ~ _ ~ esem ~ eproj ~ id ~ tw ~ tg =>
          val vprojt = vproj.getOrElse(Seq()).map(e => UnresolvedAlias(e.transformDown{
            case u @ UnresolvedAttribute(nameParts) => UnresolvedAttribute("V." + u.name)
            //case a @ Alias(child, nameParts) => Alias(a.child, "V." + a.name)()
          }))
          val eprojt = eproj.getOrElse(Seq()).map(e => UnresolvedAlias(e.transformDown{case u @ UnresolvedAttribute(nameParts) => UnresolvedAttribute("E." + u.name)}))

          val haveJoin: Boolean = id match {
            case tj: TemporalJoin => 
              tj.vertexAggregations = vprojt
              tj.edgeAggregations = eprojt
              true
            case _ => false
          }
          if ((vsem.isDefined || esem.isDefined) && (tg.isEmpty && !haveJoin))
            throw new PortalException("invalid expression: grouping/joining semantics without group or join")
          val twh: TWhere = tw.getOrElse(TWhere(StartDate(Date(-999999,1,1))))
          val sel = tw.map(_ => TemporalSelect(twh.start, twh.end, id)).getOrElse(id)
          //empty vproj or eproj means projecting out
          //not defined vproj or eproj means all attributes kept, with any semantics
          val withTGroup = tg.map(g => TGroup(Resolution.from(g.value), vsem.getOrElse(AggregateSemantics.Any), esem.getOrElse(AggregateSemantics.Any), vprojt, eprojt, sel)).getOrElse(sel)
          val withVProject = if (vproj.isDefined) ProjectVertices("vid", vprojt, withTGroup) else withTGroup
          val withEProject = if (eproj.isDefined) ProjectEdges("vid1, vid2", eprojt, withVProject) else withVProject
          withEProject
      }

  protected lazy val semantics: Parser[AggregateSemantics.Value] = 
    ( ALL ^^^ AggregateSemantics.All
    | ANY ^^^ AggregateSemantics.Any
    )

  protected lazy val joinType: Parser[JoinSemantics.Value] = 
    ( TAND ^^^ JoinSemantics.And
    | TOR ^^^ JoinSemantics.Or
    )

  protected lazy val vproj: Parser[Seq[Expression]] =
    "[" ~> VID ~> ",".? ~> repsep(projection, ",") <~ "]"

  protected lazy val eproj: Parser[Seq[Expression]] =
    "[" ~> VID1 ~> "," ~> VID2 ~> ",".? ~> repsep(projection, ",") <~ "]"

  protected lazy val tgraph: Parser[LogicalPlan] =
    joined | factor

  protected lazy val joined: Parser[LogicalPlan] =
    factor ~ joinType ~ factor ^^ {
      case lhs ~ jt ~ rhs =>
        TemporalJoin(lhs, rhs, jt)
    }

  protected lazy val factor: Parser[LogicalPlan] =
    ( ident ^^ {
      case gIdent => UnresolvedGraph(gIdent)
    }
    | "(" ~> query <~ ")"
    )

  protected lazy val projection: Parser[Expression] =
    expression ~ (AS.? ~> ident.?) ^^ {
      case e ~ a => a.fold(e)(Alias(e, _)())
    }

  protected lazy val expression: Parser[Expression] = termExpression

  //these copied over from SqlParser with some modifications
  protected lazy val termExpression: Parser[Expression] =
    productExpression *
      ( "+" ^^^ { (e1: Expression, e2: Expression) => Add(e1, e2) }
      | "-" ^^^ { (e1: Expression, e2: Expression) => Subtract(e1, e2) }
      )

  protected lazy val productExpression: Parser[Expression] =
    primary *
      ( "*" ^^^ { (e1: Expression, e2: Expression) => Multiply(e1, e2) }
      | "/" ^^^ { (e1: Expression, e2: Expression) => Divide(e1, e2) }
      | "%" ^^^ { (e1: Expression, e2: Expression) => Remainder(e1, e2) }
      | "&" ^^^ { (e1: Expression, e2: Expression) => BitwiseAnd(e1, e2) }
      | "|" ^^^ { (e1: Expression, e2: Expression) => BitwiseOr(e1, e2) }
      | "^" ^^^ { (e1: Expression, e2: Expression) => BitwiseXor(e1, e2) }
      )

  protected lazy val primary: PackratParser[Expression] = 
    ( literal
    | "(" ~> expression <~ ")"
    | function
    | attributeName ^^ UnresolvedAttribute.quoted
    )

  protected lazy val attributeName: Parser[String] = acceptMatch("attribute name", {
    case lexical.Identifier(str) => str
    case lexical.Keyword(str) if !lexical.delimiters.contains(str) => str
  })

  protected lazy val function: Parser[Expression] =
    ( ident ~ ("(" ~> repsep(expression, ",")) <~ ")" ^^
      { case fName ~ exprs => UnresolvedFunction(fName, exprs, isDistinct = false) }
    | ANY ~> "(" ~> repsep(expression, ",") <~ ")" ^^
      { case exprs => UnresolvedFunction("any", exprs, isDistinct = false) }
    )

  protected lazy val literal: Parser[Literal] =
    ( numericLiteral
    | booleanLiteral
    | stringLit ^^ {case s => Literal.create(s, StringType) }
    )

  protected lazy val booleanLiteral: Parser[Literal] =
    ( TRUE ^^^ Literal.create(true, BooleanType)
    | FALSE ^^^ Literal.create(false, BooleanType)
    )

  protected lazy val numericLiteral: Parser[Literal] =
    ( integral  ^^ { case i => Literal(toNarrowestIntegerType(i)) }
    | sign.? ~ unsignedFloat ^^ {
      case s ~ f => Literal(toDecimalOrDouble(s.getOrElse("") + f))
    }
    )

  private def toDecimalOrDouble(value: String): Any = {
    val decimal = BigDecimal(value)
    if (value.contains('E') || value.contains('e')) {
      decimal.doubleValue()
    } else {
      decimal.underlying()
    }
  }

  protected lazy val unsignedFloat: Parser[String] =
    ( "." ~> numericLit ^^ { u => "0." + u }
    | elem("decimal", _.isInstanceOf[lexical.FloatLit]) ^^ (_.chars)
    )

  private def toNarrowestIntegerType(value: String): Any = {
    val bigIntValue = BigDecimal(value)

    bigIntValue match {
      case v if bigIntValue.isValidInt => v.toIntExact
      case v if bigIntValue.isValidLong => v.toLongExact
      case v => v.underlying()
    }
  }

  protected lazy val integral: Parser[String] =
    sign.? ~ numericLit ^^ { case s ~ n => s.getOrElse("") + n }

  protected lazy val sign: Parser[String] = ("+" | "-")

  lazy val twhere: Parser[TWhere] = (TWHERE ~> datecond ~ opt(AND ~> datecond) ^^ { 
    case datec ~ Some(datec2) => new TWhere(datec, datec2)
    case datec ~ _ => new TWhere(datec)
  })

  lazy val tgroup: Parser[Group] = 
    ( TGROUP ~> BY ~> numericLit ~ period ^^ {
      case gn ~ gp => new Group(gn, gp)
    }
    | TGROUP ~> BY ~> SIZE ^^^ new Group()
    )


  protected lazy val datecond: Parser[Datecond] = (START ~> ">=" ~> date ^^ { StartDate(_)}
    | END ~> "<" ~> date ^^ { EndDate(_)})

  protected lazy val graphspec: Parser[PartialGraphSpec] = (
    (V ~> ("[" ~> VID ~> ":" ~> primitiveType ~> "," ~> repsep(field, ",") <~ "]").? <~ ";") ~ (E ~> ("[" ~> VID1 ~> ":" ~> primitiveType ~> "," ~> VID2 ~> ":" ~> primitiveType ~> "," ~> repsep(field, ",") <~ "]").?) ^^ {
      case vfields ~ efields => new PartialGraphSpec(vfields, efields)
    }
  )

  protected lazy val field: Parser[StructField] = (
    ident ~ ":" ~ dataType ^^ { case fieldName ~ _ ~ typ => StructField(fieldName, typ) }
  )

  protected lazy val dataType: Parser[DataType] = primitiveType

  lazy val year = ( numericLit ^^ { _.toInt })
  lazy val other = ( numericLit ^^ { _.toInt })

  lazy val date = ( year ~ ("-" ~> other) ~ ("-" ~> other) ^^ { case y ~ mo ~ d => new Date(y,mo,d) }
  )

  lazy val period: Parser[Period] = 
    ( "year(s)?".r ^^^ Years()
    | "month(s)?".r ^^^ Months()
    | "day(s)?".r ^^^ Days()
  )

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

protected case class Date(y: Int, m: Int, d: Int) {
  def value: LocalDate = LocalDate.of(y,m,d)
}
protected sealed abstract class Datecond
protected case class StartDate(datet: Date) extends Datecond
protected case class EndDate(datet: Date) extends Datecond

protected case class Group(num: String, per: Period) {
  def value(): String = "P" + num + per.value
  //this is a special one for aggregation by size
  def this() = this("0", Years())
}

protected case class TWhere(datec: Datecond) {
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

sealed abstract class Period {
  val value:String
}
case class Months() extends Period {
  override val value = "M"
}
case class Years() extends Period {
  override val value = "Y"
}
case class Days() extends Period {
  override val value = "D"
}

