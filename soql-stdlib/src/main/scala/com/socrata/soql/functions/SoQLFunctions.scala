package com.socrata.soql.functions

import com.socrata.soql.types._
import com.socrata.soql.ast.SpecialFunctions
import com.socrata.soql.environment.FunctionName
import java.lang.reflect.Modifier

sealed abstract class SoQLFunctions

object SoQLFunctions {
  private val log = org.slf4j.LoggerFactory.getLogger(classOf[SoQLFunctions])

  private val Universe = SoQLType.typesByName.values.toSeq.sortBy(_.name)

  private val Ordered = Seq[SoQLType](
    SoQLText,
    SoQLNumber,
    SoQLDouble,
    SoQLMoney,
    SoQLBoolean,
    SoQLFixedTimestamp,
    SoQLFloatingTimestamp,
    SoQLDate,
    SoQLTime,
    SoQLID,
    SoQLVersion
  )
  private val Equatable = Ordered
  private val NumLike = Seq[SoQLType](SoQLNumber, SoQLDouble, SoQLMoney)
  private val RealNumLike = Seq[SoQLType](SoQLNumber, SoQLDouble)
  private val GeospatialLike = Seq[SoQLType](SoQLLocation)

  def AAb(identity: String, name: FunctionName, A: Seq[SoQLType], b: SoQLType) = new Function[SoQLType](identity, name) {
    val functions = for(a <- A) yield f(Seq(a, a), None, b)
  }
  def AAAb(identity: String, name: FunctionName, A: Seq[SoQLType], b: SoQLType) = new Function[SoQLType](identity, name) {
    val functions = for(a <- A) yield f(Seq(a, a, a), None, b)
  }
  def AA(identity: String, name: FunctionName, A: Seq[SoQLType], isAggregate: Boolean = false) = new Function[SoQLType](identity, name) {
    val functions = for(a <- A) yield f(Seq(a), None, a, isAggregate)
  }
  def AAA(identity: String, name: FunctionName, A: Seq[SoQLType], isAggregate: Boolean = false) = new Function[SoQLType](identity, name) {
    val functions = for(a <- A) yield f(Seq(a, a), None, a, isAggregate)
  }
  def OrdOrdBool(identity: String, name: FunctionName) = AAb(identity, name, Ordered, SoQLBoolean)
  def OrdOrdOrdBool(identity: String, name: FunctionName) = AAAb(identity, name, Ordered, SoQLBoolean)
  def EqEqBool(identity: String, name: FunctionName) = AAb(identity, name, Equatable, SoQLBoolean)
  def AnyT(identity: String, name: FunctionName, t: SoQLType, isAggregate: Boolean = false) = new Function[SoQLType](identity, name) {
    val functions = for(a <- Universe) yield f(Seq(a), None, t, isAggregate)
  }

  val TextToFixedTimestamp = Function.simple("text to fixed timestamp", SpecialFunctions.Cast(SoQLFixedTimestamp.name), Seq(SoQLText), None, SoQLFixedTimestamp)
  val TextToFloatingTimestamp = Function.simple("text to floating timestamp", SpecialFunctions.Cast(SoQLFloatingTimestamp.name), Seq(SoQLText), None, SoQLFloatingTimestamp)
  val TextToDate = Function.simple("text to date", SpecialFunctions.Cast(SoQLDate.name), Seq(SoQLText), None, SoQLDate)
  val TextToTime = Function.simple("text to time", SpecialFunctions.Cast(SoQLTime.name), Seq(SoQLText), None, SoQLTime)

  val Concat = AAb("||", SpecialFunctions.Operator("||"), Universe, SoQLText)

  val Gte = OrdOrdBool(">=", SpecialFunctions.Operator(">="))
  val Gt = OrdOrdBool(">", SpecialFunctions.Operator(">"))
  val Lt = OrdOrdBool("<", SpecialFunctions.Operator("<"))
  val Lte = OrdOrdBool("<=", SpecialFunctions.Operator("<="))
  val Eq = EqEqBool("=", SpecialFunctions.Operator("="))
  val EqEq = EqEqBool("==", SpecialFunctions.Operator("=="))
  val Neq = EqEqBool("<>", SpecialFunctions.Operator("<>"))
  val BangEq = EqEqBool("!=", SpecialFunctions.Operator("!="))

  // arguments: lat, lon, distance in meter
  val WithinCircle = new Function[SoQLType]("within_circle", FunctionName("within_circle")) {
    val functions = for {
      a <- GeospatialLike
      b <- RealNumLike
    } yield f(Seq(a, b, b, b), None, SoQLBoolean)
  }

  // arguments: nwLat, nwLon, seLat, seLon (yMax,  xMin , yMin,  xMax)
  val WithinBox = new Function[SoQLType]("within_box", FunctionName("within_box")) {
    val functions = for {
      a <- GeospatialLike
      b <- RealNumLike
    } yield f(Seq(a, b, b, b, b), None, SoQLBoolean)
  }

  val LatitudeField = Function.simple("latitude field", SpecialFunctions.Subscript, Seq(SoQLLocation, SoQLTextLiteral("latitude")), None, SoQLDouble)
  val LongitudeField = Function.simple("longitude field", SpecialFunctions.Subscript, Seq(SoQLLocation, SoQLTextLiteral("longitude")), None, SoQLDouble)

  val IsNull = AnyT("is null", SpecialFunctions.IsNull, SoQLBoolean)
  val IsNotNull = AnyT("is not null", SpecialFunctions.IsNotNull, SoQLBoolean)

  val Between = OrdOrdOrdBool("between", SpecialFunctions.Between)
  val NotBetween = OrdOrdOrdBool("not between", SpecialFunctions.NotBetween)

  val Min = AA("min", FunctionName("min"), Ordered, isAggregate = true)
  val Max = AA("max", FunctionName("max"), Ordered, isAggregate = true)
  val CountStar = Function.simple("count(*)", SpecialFunctions.StarFunc("count"), Seq(), None, SoQLNumber, isAggregate = true)
  val Count = AnyT("count", FunctionName("count"), SoQLNumber, isAggregate = true)
  val Sum = AA("sum", FunctionName("sum"), NumLike, isAggregate = true)
  val Avg = AA("avg", FunctionName("avg"), NumLike, isAggregate = true)

  val UnaryPlus = AA("unary +", SpecialFunctions.Operator("+"), NumLike)
  val UnaryMinus = AA("unary -", SpecialFunctions.Operator("-"), NumLike)

  val BinaryPlus = AAA("+", SpecialFunctions.Operator("+"), NumLike)
  val BinaryMinus = AAA("-", SpecialFunctions.Operator("-"), NumLike)

  val Times = new Function[SoQLType]("*", SpecialFunctions.Operator("*")) {
    val functions = List(
      f(Seq(SoQLNumber, SoQLNumber), None, SoQLNumber),
      f(Seq(SoQLDouble, SoQLDouble), None, SoQLDouble),
      f(Seq(SoQLNumber, SoQLMoney), None, SoQLMoney),
      f(Seq(SoQLMoney, SoQLNumber), None, SoQLMoney)
    )
  }

  val Div = new Function[SoQLType]("/", SpecialFunctions.Operator("/")) {
    val functions = List(
      f(Seq(SoQLNumber, SoQLNumber), None, SoQLNumber),
      f(Seq(SoQLDouble, SoQLDouble), None, SoQLDouble),
      f(Seq(SoQLMoney, SoQLNumber), None, SoQLMoney),
      f(Seq(SoQLMoney, SoQLMoney), None, SoQLNumber)
    )
  }

  val Exp = new Function[SoQLType]("^", SpecialFunctions.Operator("^")) {
    val functions = List(
      f(Seq(SoQLDouble, SoQLDouble), None, SoQLDouble),
      f(Seq(SoQLNumber, SoQLNumber), None, SoQLNumber)
    )
  }

  val Mode = new Function[SoQLType]("%", SpecialFunctions.Operator("%")) {
    val functions = List(
      f(Seq(SoQLNumber, SoQLNumber), None, SoQLNumber),
      f(Seq(SoQLDouble, SoQLDouble), None, SoQLDouble),
      f(Seq(SoQLMoney, SoQLNumber), None, SoQLMoney),
      f(Seq(SoQLMoney, SoQLMoney), None, SoQLNumber)
    )
  }

  val NumberToMoney = Function.simple("number to money", SpecialFunctions.Cast(SoQLMoney.name), Seq(SoQLNumber), None, SoQLMoney)
  val NumberToDouble = Function.simple("number to double", SpecialFunctions.Cast(SoQLDouble.name), Seq(SoQLNumber), None, SoQLDouble)

  val And = Function.simple("and", SpecialFunctions.Operator("and"), Seq(SoQLBoolean, SoQLBoolean), None, SoQLBoolean)
  val Or = Function.simple("or", SpecialFunctions.Operator("or"), Seq(SoQLBoolean, SoQLBoolean), None, SoQLBoolean)
  val Not = Function.simple("not", SpecialFunctions.Operator("not"), Seq(SoQLBoolean), None, SoQLBoolean)

  def EqEqst(identity: String, name: FunctionName, t: SoQLType) = new Function[SoQLType](identity, name) {
    val functions = for(a <- Equatable) yield f(Seq(a), Some(a), t)
  }
  val In = EqEqst("in", SpecialFunctions.In, SoQLBoolean)
  val NotIn = EqEqst("not in", SpecialFunctions.NotIn, SoQLBoolean)

  val Like = Function.simple("like", SpecialFunctions.Like, Seq(SoQLText, SoQLText), None, SoQLBoolean)
  val NotLike = Function.simple("not like", SpecialFunctions.NotLike, Seq(SoQLText, SoQLText), None, SoQLBoolean)

  val Contains = Function.simple("contains",  FunctionName("contains"), Seq(SoQLText, SoQLText), None, SoQLBoolean)
  val StartsWith = Function.simple("starts_with",  FunctionName("starts_with"), Seq(SoQLText, SoQLText), None, SoQLBoolean)
  val Lower = Function.simple("lower",  FunctionName("lower"), Seq(SoQLText), None, SoQLText)
  val Upper = Function.simple("upper",  FunctionName("upper"), Seq(SoQLText), None, SoQLText)

  val FloatingTimeStampTruncYmd = Function.simple("floating timestamp trunc day", FunctionName("date_trunc_ymd"), Seq(SoQLFloatingTimestamp), None, SoQLFloatingTimestamp)
  val FloatingTimeStampTruncYm = Function.simple("floating timestamp trunc month", FunctionName("date_trunc_ym"), Seq(SoQLFloatingTimestamp), None, SoQLFloatingTimestamp)
  val FloatingTimeStampTruncY = Function.simple("floating timestamp trunc year", FunctionName("date_trunc_y"), Seq(SoQLFloatingTimestamp), None, SoQLFloatingTimestamp)

  val castIdentities = for(t <- Universe) yield {
    Function.simple(t.name.caseFolded+"::"+t.name.caseFolded, SpecialFunctions.Cast(t.name), Seq(t), None, t)
  }

  val NumberToText = Function.simple("number to text", SpecialFunctions.Cast(SoQLText.name), Seq(SoQLNumber), None, SoQLText)
  val TextToNumber = Function.simple("text to number", SpecialFunctions.Cast(SoQLNumber.name), Seq(SoQLText), None, SoQLNumber)
  val TextToMoney = Function.simple("text to money", SpecialFunctions.Cast(SoQLMoney.name), Seq(SoQLText), None, SoQLMoney)

  val TextToBool = Function.simple("text to boolean",  SpecialFunctions.Cast(SoQLBoolean.name), Seq(SoQLText), None, SoQLBoolean)
  val BoolToText = Function.simple("boolean to text", SpecialFunctions.Cast(SoQLText.name), Seq(SoQLBoolean), None, SoQLText)

  val Prop = Function.simple(".", SpecialFunctions.Subscript, Seq(SoQLObject, SoQLText), None, SoQLJson)
  val Index = Function.simple("[]", SpecialFunctions.Subscript, Seq(SoQLArray, SoQLNumber), None, SoQLJson)
  val JsonProp = Function.simple(".J", SpecialFunctions.Subscript, Seq(SoQLJson, SoQLText), None, SoQLJson)
  val JsonIndex = Function.simple("[]J", SpecialFunctions.Subscript, Seq(SoQLJson, SoQLNumber), None, SoQLJson)

  val JsonToText = Function.simple("json to text", SpecialFunctions.Cast(SoQLText.name), Seq(SoQLJson), None, SoQLText)
  val JsonToNumber = Function.simple("json to number", SpecialFunctions.Cast(SoQLNumber.name), Seq(SoQLJson), None, SoQLNumber)
  val JsonToBool = Function.simple("json to bool", SpecialFunctions.Cast(SoQLBoolean.name), Seq(SoQLJson), None, SoQLBoolean)
  val JsonToObject = Function.simple("json to obj", SpecialFunctions.Cast(SoQLObject.name), Seq(SoQLJson), None, SoQLObject)
  val JsonToArray = Function.simple("json to array", SpecialFunctions.Cast(SoQLArray.name), Seq(SoQLJson), None, SoQLArray)

  val TextToRowIdentifier = Function.simple("text to rid", SpecialFunctions.Cast(SoQLID.name), Seq(SoQLText), None, SoQLID)
  val TextToRowVersion = Function.simple("text to rowver", SpecialFunctions.Cast(SoQLVersion.name), Seq(SoQLText), None, SoQLVersion)

  def potentialAccessors = for {
    method <- getClass.getMethods
    if Modifier.isPublic(method.getModifiers) && method.getParameterTypes.length == 0
  } yield method

  val allFunctions: Seq[Function[SoQLType]] = {
    val reflectedFunctions = for {
      method <- getClass.getMethods
      if Modifier.isPublic(method.getModifiers) && method.getParameterTypes.length == 0 && method.getReturnType == classOf[Function[_]]
    } yield method.invoke(this).asInstanceOf[Function[SoQLType]]
    castIdentities ++ reflectedFunctions
  }

  val functionsByIdentity = allFunctions.map { f => f.identity -> f }.toMap

  val nAdicMonomorphicFunctions = SoQLFunctions.allFunctions.flatMap(_.functions.filterNot(_.isVariadic))
  val variadicMonomorphicFunctions = SoQLFunctions.allFunctions.flatMap(_.functions.filter(_.isVariadic))

  private def analysisify(f: MonomorphicFunction[SoQLType]): MonomorphicFunction[SoQLAnalysisType] = f

  val nAdicMonomorphicFunctionsByNameThenArity: Map[FunctionName, Map[Int, Set[MonomorphicFunction[SoQLAnalysisType]]]] =
    nAdicMonomorphicFunctions.groupBy(_.name).mapValues { fs =>
      fs.groupBy(_.minArity).mapValues(_.map(analysisify).toSet).toMap
    }.toMap

  val variadicMonomorphicFunctionsByNameThenMinArity: Map[FunctionName, Map[Int, Set[MonomorphicFunction[SoQLAnalysisType]]]] =
    variadicMonomorphicFunctions.groupBy(_.name).mapValues { fs =>
      fs.groupBy(_.minArity).mapValues(_.map(analysisify).toSet).toMap
    }.toMap
}
