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
import org.apache.spark.sql.catalyst.analysis.{UnresolvedFunction,UnresolvedAttribute,UnresolvedAlias,UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions._

import edu.drexel.cs.dbgroup.temporalgraph._
import edu.drexel.cs.dbgroup.temporalgraph.{Exists => GExists}
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

  protected val ALWAYS = Keyword("ALWAYS")
  protected val AND = Keyword("AND")
  protected val ANY = Keyword("ANY")
  protected val AS = Keyword("AS")
  protected val BY = Keyword("BY")
  protected val EXISTS = Keyword("EXISTS")
  protected val ESELECT = Keyword("ESELECT")
  protected val EWHERE = Keyword("EWHERE")
  protected val EEXISTS = Keyword("EXISTS")
  protected val FALSE = Keyword("FALSE")
  protected val FROM = Keyword("FROM")
  protected val INTERSECT = Keyword("INTERSECT")
  protected val LIKE = Keyword("LIKE")
  protected val LOAD = Keyword("LOAD")
  protected val MOST = Keyword("MOST")
  protected val NOT = Keyword("NOT")
  protected val OR = Keyword("OR")
  protected val TGROUP = Keyword("TGROUP")
  protected val TRUE = Keyword("TRUE")
  protected val SELECT = Keyword("SELECT")
  protected val UNION = Keyword("UNION")
  protected val VEXISTS = Keyword("VEXISTS")
  protected val VSELECT = Keyword("VSELECT")
  protected val VWHERE = Keyword("VWHERE")

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

  protected lazy val query: Parser[LogicalPlan] = 
    (query_expression | ("(" ~> query_expression <~ ")")) *
    ( UNION ^^^ { (lhs: LogicalPlan, rhs: LogicalPlan) => TemporalUnion(lhs, rhs) }
    | INTERSECT ^^^ { (lhs: LogicalPlan, rhs: LogicalPlan) => TemporalIntersection(lhs, rhs) }
//      query ~ UNION ~ query_expression ^^ { case lhs ~ _ ~ rhs => TemporalUnion(lhs, rhs) }
//    | query ~ INTERSECT ~ query_expression ^^ { case lhs ~ _ ~ rhs => TemporalIntersection(lhs, rhs) }
//    | 
//query_expression
  )

  protected lazy val query_expression: Parser[LogicalPlan] = (
    tload | tselect 
    | ident ^^ { case gIdent => UnresolvedGraph(gIdent) }
  )

  protected lazy val tload: Parser[LogicalPlan] = (
    LOAD ~> FROM ~> stringLit ^^ { case dataset => LoadGraph(dataset) }
  )

  protected lazy val tselect: Parser[LogicalPlan] =
    VSELECT ~> proj ~
    (ESELECT ~> proj) ~
    (FROM ~> tgraph) ~
    (VWHERE ~> expression).? ~
    (EWHERE ~> expression).? ~
    tgroup.? ^^ {
      case vproj ~ eproj ~ id ~ vw ~ ew ~ tg =>
        //if there are analytics in there, that gets pulled out later
        //similar with aggregation functions
        val vprojt = vproj.map(e => UnresolvedAlias(e.transformDown{
          case u @ UnresolvedAttribute(nameParts) => UnresolvedAttribute("V." + u.name)
        }))
        val eprojt = eproj.map(e => UnresolvedAlias(e.transformDown{
          case u @ UnresolvedAttribute(nameParts) => UnresolvedAttribute("E." + u.name)
        }))

        //if there are slice conditions there, that gets pulled out later
        val vwt = vw.getOrElse(UnresolvedStar(None))
        val ewt = ew.getOrElse(UnresolvedStar(None))
        val subg = if (vw.isDefined || ew.isDefined) Subgraph(vwt, ewt, id) else id

        val withTGroup = tg.map(g => Aggregate(g.value, g.vq, g.eq, vprojt, eprojt, subg)).getOrElse(subg)

        val withVMap = if (vproj.isEmpty) withTGroup else VertexMap(vprojt, withTGroup)
        val withEMap = if (eproj.isEmpty) withVMap else EdgeMap(eprojt, withVMap)

        withEMap
    }

  protected lazy val proj: Parser[Seq[Expression]] = (
    "*" ^^^ Seq() 
      | repsep(projection, ",")
  )

  protected lazy val tgraph: Parser[LogicalPlan] =
    ( ident ^^ {
      case gIdent => UnresolvedGraph(gIdent)
    }
    | "(" ~> query <~ ")"
    )

  protected lazy val projection: Parser[Expression] =
    expression ~ (AS.? ~> ident.?) ^^ {
      case e ~ a => a.fold(e)(Alias(e, _)())
    }

  protected lazy val expression: Parser[Expression] = orExpression

  protected lazy val orExpression: Parser[Expression] =
    andExpression * (OR ^^^ { (e1: Expression, e2: Expression) => Or(e1, e2) })

  protected lazy val andExpression: Parser[Expression] =
    notExpression * (AND ^^^ { (e1: Expression, e2: Expression) => And(e1, e2) })

  protected lazy val notExpression: Parser[Expression] =
    NOT.? ~ comparisonExpression ^^ { case maybeNot ~ e => maybeNot.map(_ => Not(e)).getOrElse(e) }

  protected lazy val comparisonExpression: Parser[Expression] =
    ( termExpression ~ ("="  ~> termExpression) ^^ { case e1 ~ e2 => EqualTo(e1, e2) }
    | termExpression ~ ("<"  ~> termExpression) ^^ { case e1 ~ e2 => LessThan(e1, e2) }
    | termExpression ~ ("<=" ~> termExpression) ^^ { case e1 ~ e2 => LessThanOrEqual(e1, e2) }
    | termExpression ~ (">"  ~> termExpression) ^^ { case e1 ~ e2 => GreaterThan(e1, e2) }
    | termExpression ~ (">=" ~> termExpression) ^^ { case e1 ~ e2 => GreaterThanOrEqual(e1, e2) }
    | termExpression ~ ("!=" ~> termExpression) ^^ { case e1 ~ e2 => Not(EqualTo(e1, e2)) }
    | termExpression ~ ("<>" ~> termExpression) ^^ { case e1 ~ e2 => Not(EqualTo(e1, e2)) }
    | termExpression ~ ("<=>" ~> termExpression) ^^ { case e1 ~ e2 => EqualNullSafe(e1, e2) }
    | termExpression ~ (LIKE   ~> termExpression) ^^ { case e1 ~ e2 => Like(e1, e2) }
    | termExpression ~ (NOT ~ LIKE ~> termExpression) ^^ { case e1 ~ e2 => Not(Like(e1, e2)) }
    | termExpression
    )

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

  lazy val tgroup: Parser[Group] = 
    ( TGROUP ~> BY ~> numericLit ~ period ~ (VEXISTS ~> quantifier).? ~ (EEXISTS ~> quantifier).? ^^ {
      case gn ~ gp ~ vq ~ eq => 
        val vquant = vq.getOrElse(GExists())
        val equant = eq.getOrElse(GExists())
        new Group(gn, gp, vquant, equant)
    }
    )

  lazy val period: Parser[Period] = 
    ( "year(s)?".r ^^^ Years()
    | "month(s)?".r ^^^ Months()
    | "day(s)?".r ^^^ Days()
    | "change(s)?".r ^^^ Changes()
  )

  lazy val quantifier: Parser[Quantification] =
    ( ALWAYS ^^^ Always()
    | EXISTS ^^^ GExists()
    | MOST ^^^ Most()
    | ">" ~> numericLit ^^ { case nu => AtLeast(nu.toDouble) }
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

protected case class Group(num: String, per: Period, vq: Quantification, eq: Quantification) {
  def value(): WindowSpecification = {
    per match {
      case Changes() => ChangeSpec(num.toInt)
      case _ => TimeSpec(Resolution.from("P" + num + per.value))
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
case class Changes() extends Period {
  override val value = "C"
}
