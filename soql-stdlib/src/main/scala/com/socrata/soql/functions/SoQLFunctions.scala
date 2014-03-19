package com.socrata.soql.functions

import com.socrata.soql.types._
import com.socrata.soql.ast.SpecialFunctions
import com.socrata.soql.environment.FunctionName
import java.lang.reflect.{Type, ParameterizedType, Modifier}

sealed abstract class SoQLFunctions

object SoQLFunctions {
  private val log = org.slf4j.LoggerFactory.getLogger(classOf[SoQLFunctions])

  val types = SoQLType.typesByName.values.toSet

  private val Ordered = Set[SoQLType](
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
  private val NumLike = Set[SoQLType](SoQLNumber, SoQLDouble, SoQLMoney)
  private val RealNumLike = Set[SoQLType](SoQLNumber, SoQLDouble)
  private val GeospatialLike = Set[SoQLType](SoQLLocation)

  def i(label: String, ts: SoQLType*) =
    if(ts.isEmpty) label
    else ts.mkString(label + " ", " ", "")

  val TextToFixedTimestamp = Function(i("text to fixed timestamp"), SpecialFunctions.Cast(SoQLFixedTimestamp.name), Seq(SoQLText), None, SoQLFixedTimestamp)
  val TextToFloatingTimestamp = Function(i("text to floating timestamp"), SpecialFunctions.Cast(SoQLFloatingTimestamp.name), Seq(SoQLText), None, SoQLFloatingTimestamp)
  val TextToDate = Function(i("text to date"), SpecialFunctions.Cast(SoQLDate.name), Seq(SoQLText), None, SoQLDate)
  val TextToTime = Function(i("text to time"), SpecialFunctions.Cast(SoQLTime.name), Seq(SoQLText), None, SoQLTime)
  val Concat = for {
    a <- types
    b <- types
  } yield Function(i("||", a, b), SpecialFunctions.Operator("||"), Seq(a, b), None, SoQLText)
  val Gte = Ordered.map { a =>
    Function(i(">=", a), SpecialFunctions.Operator(">="), Seq(a, a), None, SoQLBoolean)
  }
  val Gt = Ordered.map { a =>
    Function(i(">", a), SpecialFunctions.Operator(">"), Seq(a, a), None, SoQLBoolean)
  }
  val Lt = Ordered.map { a =>
    Function(i("<", a), SpecialFunctions.Operator("<"), Seq(a, a), None, SoQLBoolean)
  }
  val Lte = Ordered.map { a =>
    Function(i("<=", a), SpecialFunctions.Operator("<="), Seq(a, a), None, SoQLBoolean)
  }
  val Eq = Equatable.map { a =>
    Function(i("=", a), SpecialFunctions.Operator("="), Seq(a, a), None, SoQLBoolean)
  }
  val EqEq = Eq.map { f => f.copy(identity = "=" + f.identity, name = SpecialFunctions.Operator("==")) }
  val Neq = Equatable.map { a =>
    Function(i("<>", a), SpecialFunctions.Operator("<>"), Seq(a, a), None, SoQLBoolean)
  }
  val BangEq = Neq.map { f => f.copy(identity = "!=" + f.identity.drop(2), name = SpecialFunctions.Operator("!=")) }

  // arguments: lat, lon, distance in meter
  val WithinCircle = for {
    a <- GeospatialLike
    b <- RealNumLike
  } yield Function(i("within_circle", a, b), FunctionName("within_circle"), Seq(a, b, b, b), None, SoQLBoolean)
  // arguments: nwLat, nwLon, seLat, seLon (yMax,  xMin , yMin,  xMax)
  val WithinBox = for {
    a <- GeospatialLike
    b <- RealNumLike
  } yield Function(i("within_box", a, b), FunctionName("within_box"), Seq(a, b, b, b, b), None, SoQLBoolean)

  val LatitudeField = Function(i("latitude field"), SpecialFunctions.Subscript, Seq(SoQLLocation, SoQLTextLiteral("latitude")), None, SoQLDouble)
  val LongitudeField = Function(i("longitude field"), SpecialFunctions.Subscript, Seq(SoQLLocation, SoQLTextLiteral("longitude")), None, SoQLDouble)

  val IsNull = for {
    a <- types
  } yield Function(i("is null", a), SpecialFunctions.IsNull, Seq(a), None, SoQLBoolean)

  val IsNotNull = for {
    a <- types
  } yield Function(i("is not null", a), SpecialFunctions.IsNotNull, Seq(a), None, SoQLBoolean)

  val Between = for {
    a <- Ordered
  } yield Function(i("between", a), SpecialFunctions.Between, Seq(a,a,a), None, SoQLBoolean)
  val NotBetween = for {
    a <- Ordered
  } Function(i("not between", a), SpecialFunctions.NotBetween, Seq(a,a,a), None, SoQLBoolean)

  val Min = for {
    a <- Ordered
  } yield Function(i("min", a), FunctionName("min"), Seq(a), None, a, isAggregate = true)
  val Max = for {
    a <- Ordered
  } yield Function(i("max", a), FunctionName("max"), Seq(a), None, a, isAggregate = true)
  val CountStar = Function("count(*)", SpecialFunctions.StarFunc("count"), Seq(), None, SoQLNumber, isAggregate = true)
  val Count = for {
    a <- types
  } yield Function(i("count", a), FunctionName("count"), Seq(a), None, SoQLNumber, isAggregate = true)
  val Sum = for {
    a <- NumLike
  } yield Function(i("sum", a), FunctionName("sum"), Seq(a), None, a, isAggregate = true)
  val Avg = for {
    a <- NumLike
  } yield Function(i("avg", a), FunctionName("avg"), Seq(a), None, a, isAggregate = true)

  val UnaryPlus = for {
    a <- NumLike
  } yield Function(i("unary +", a), SpecialFunctions.Operator("+"), Seq(a), None, a)
  val UnaryMinus = for {
    a <- NumLike
  } yield Function(i("unary - ", a), SpecialFunctions.Operator("-"), Seq(a), None, a)

  val BinaryPlus = for {
    a <- NumLike
  } yield Function(i("+", a), SpecialFunctions.Operator("+"), Seq(a, a), None, a)
  val BinaryMinus = for {
    a <- NumLike
  } yield Function(i("-", a), SpecialFunctions.Operator("-"), Seq(a, a), None, a)

  val TimesNumNum = Function("*NN", SpecialFunctions.Operator("*"), Seq(SoQLNumber, SoQLNumber), None, SoQLNumber)
  val TimesDoubleDouble = Function("*DD", SpecialFunctions.Operator("*"), Seq(SoQLDouble, SoQLDouble), None, SoQLDouble)
  val TimesNumMoney = Function("*NM", SpecialFunctions.Operator("*"), Seq(SoQLNumber, SoQLMoney), None, SoQLMoney)
  val TimesMoneyNum = Function("*MN", SpecialFunctions.Operator("*"), Seq(SoQLMoney, SoQLNumber), None, SoQLMoney)

  val DivNumNum = Function("/NN", SpecialFunctions.Operator("/"), Seq(SoQLNumber, SoQLNumber), None, SoQLNumber)
  val DivDoubleDouble = Function("/DD", SpecialFunctions.Operator("/"), Seq(SoQLDouble, SoQLDouble), None, SoQLDouble)
  val DivMoneyNum = Function("/MN", SpecialFunctions.Operator("/"), Seq(SoQLMoney, SoQLNumber), None, SoQLMoney)
  val DivMoneyMoney = Function("/MM", SpecialFunctions.Operator("/"), Seq(SoQLMoney, SoQLMoney), None, SoQLNumber)

  val ExpNumNum = Function("^NN", SpecialFunctions.Operator("^"), Seq(SoQLNumber, SoQLNumber), None, SoQLNumber)
  val ExpDoubleDouble = Function("^DD", SpecialFunctions.Operator("^"), Seq(SoQLDouble, SoQLDouble), None, SoQLDouble)

  val ModNumNum = Function("%NN", SpecialFunctions.Operator("%"), Seq(SoQLNumber, SoQLNumber), None, SoQLNumber)
  val ModDoubleDouble = Function("%DD", SpecialFunctions.Operator("%"), Seq(SoQLDouble, SoQLDouble), None, SoQLDouble)
  val ModMoneyNum = Function("%MN", SpecialFunctions.Operator("%"), Seq(SoQLMoney, SoQLNumber), None, SoQLMoney)
  val ModMoneyMoney = Function("%MM", SpecialFunctions.Operator("%"), Seq(SoQLMoney, SoQLMoney), None, SoQLNumber)

  val NumberToMoney = Function("number to money", SpecialFunctions.Cast(SoQLMoney.name), Seq(SoQLNumber), None, SoQLMoney)
  val NumberToDouble = Function("number to double", SpecialFunctions.Cast(SoQLDouble.name), Seq(SoQLNumber), None, SoQLDouble)

  val And = Function("and", SpecialFunctions.Operator("and"), Seq(SoQLBoolean, SoQLBoolean), None, SoQLBoolean)
  val Or = Function("or", SpecialFunctions.Operator("or"), Seq(SoQLBoolean, SoQLBoolean), None, SoQLBoolean)
  val Not = Function("not", SpecialFunctions.Operator("not"), Seq(SoQLBoolean), None, SoQLBoolean)

  val In = for {
    a <- Equatable
  } yield Function(i("in", a), SpecialFunctions.In, Seq(a), Some(a), SoQLBoolean)
  val NotIn = for {
    a <- Equatable
  } yield Function(i("not in", a), SpecialFunctions.NotIn, Seq(a), Some(a), SoQLBoolean)

  val Like = Function("like", SpecialFunctions.Like, Seq(SoQLText, SoQLText), None, SoQLBoolean)
  val NotLike = Function("not like", SpecialFunctions.NotLike, Seq(SoQLText, SoQLText), None, SoQLBoolean)

  val Contains = Function("contains",  FunctionName("contains"), Seq(SoQLText, SoQLText), None, SoQLBoolean)
  val StartsWith = Function("starts_with",  FunctionName("starts_with"), Seq(SoQLText, SoQLText), None, SoQLBoolean)
  val Lower = Function("lower",  FunctionName("lower"), Seq(SoQLText), None, SoQLText)
  val Upper = Function("upper",  FunctionName("upper"), Seq(SoQLText), None, SoQLText)

  val FloatingTimeStampTruncYmd = Function("floating timestamp trunc day", FunctionName("date_trunc_ymd"), Seq(SoQLFloatingTimestamp), None, SoQLFloatingTimestamp)
  val FloatingTimeStampTruncYm = Function("floating timestamp trunc month", FunctionName("date_trunc_ym"), Seq(SoQLFloatingTimestamp), None, SoQLFloatingTimestamp)
  val FloatingTimeStampTruncY = Function("floating timestamp trunc year", FunctionName("date_trunc_y"), Seq(SoQLFloatingTimestamp), None, SoQLFloatingTimestamp)

  val castIdentities = for(t <- types) yield {
    Function(i("::", t, t), SpecialFunctions.Cast(t.name), Seq(t), None, t)
  }

  val NumberToText = Function("number to text", SpecialFunctions.Cast(SoQLText.name), Seq(SoQLNumber), None, SoQLText)
  val TextToNumber = Function("text to number", SpecialFunctions.Cast(SoQLNumber.name), Seq(SoQLText), None, SoQLNumber)
  val TextToMoney = Function("text to money", SpecialFunctions.Cast(SoQLMoney.name), Seq(SoQLText), None, SoQLMoney)

  val TextToBool = Function("text to boolean",  SpecialFunctions.Cast(SoQLBoolean.name), Seq(SoQLText), None, SoQLBoolean)
  val BoolToText = Function("boolean to text", SpecialFunctions.Cast(SoQLText.name), Seq(SoQLBoolean), None, SoQLText)

  val Prop = Function(".", SpecialFunctions.Subscript, Seq(SoQLObject, SoQLText), None, SoQLJson)
  val Index = Function("[]", SpecialFunctions.Subscript, Seq(SoQLArray, SoQLNumber), None, SoQLJson)
  val JsonProp = Function(".J", SpecialFunctions.Subscript, Seq(SoQLJson, SoQLText), None, SoQLJson)
  val JsonIndex = Function("[]J", SpecialFunctions.Subscript, Seq(SoQLJson, SoQLNumber), None, SoQLJson)

  val JsonToText = Function("json to text", SpecialFunctions.Cast(SoQLText.name), Seq(SoQLJson), None, SoQLText)
  val JsonToNumber = Function("json to number", SpecialFunctions.Cast(SoQLNumber.name), Seq(SoQLJson), None, SoQLNumber)
  val JsonToBool = Function("json to bool", SpecialFunctions.Cast(SoQLBoolean.name), Seq(SoQLJson), None, SoQLBoolean)
  val JsonToObject = Function("json to obj", SpecialFunctions.Cast(SoQLObject.name), Seq(SoQLJson), None, SoQLObject)
  val JsonToArray = Function("json to array", SpecialFunctions.Cast(SoQLArray.name), Seq(SoQLJson), None, SoQLArray)

  val TextToRowIdentifier = Function("text to rid", SpecialFunctions.Cast(SoQLID.name), Seq(SoQLText), None, SoQLID)
  val TextToRowVersion = Function("text to rowver", SpecialFunctions.Cast(SoQLVersion.name), Seq(SoQLText), None, SoQLVersion)

  def potentialAccessors = for {
    method <- getClass.getMethods
    if Modifier.isPublic(method.getModifiers) && method.getParameterTypes.length == 0
  } yield method

  val allFunctions: Seq[Function[SoQLType]] = {
    def isSoQLFunction(t: Type): Boolean = t match {
      case p: ParameterizedType if p.getRawType.asInstanceOf[Class[_]] == classOf[Function[_]] =>
        p.getActualTypeArguments()(0) match {
          case c: Class[_] if classOf[SoQLType].isAssignableFrom(c) =>
            true
          case _ =>
            false
        }
      case _ =>
        false
    }
    val reflectedFunctions = for {
      method <- getClass.getMethods
      if Modifier.isPublic(method.getModifiers) && method.getParameterTypes.length == 0 && isSoQLFunction(method.getGenericReturnType)
    } yield method.invoke(this).asInstanceOf[Function[SoQLType]]
    val reflectedFunctionLists = for {
      method <- getClass.getMethods
      if Modifier.isPublic(method.getModifiers) && method.getParameterTypes.length == 0 && method.getReturnType == classOf[Set[_]] && isSoQLFunction(method.getGenericReturnType.asInstanceOf[ParameterizedType].getActualTypeArguments()(0))
    } yield {
      method.invoke(this).asInstanceOf[Set[Function[SoQLType]]]
    }
    reflectedFunctions.toSeq ++ reflectedFunctionLists.toSeq.flatten
  }

  val functionsByIdentity = allFunctions.map { f => f.identity -> f }.toMap
  println(functionsByIdentity.keys)

  val nAdicFunctions = SoQLFunctions.allFunctions.filterNot(_.isVariadic)
  val variadicFunctions = SoQLFunctions.allFunctions.filter(_.isVariadic)

  private def analysisify(f: Function[SoQLType]): Function[SoQLAnalysisType] = f

  val nAdicFunctionsByNameThenArity: Map[FunctionName, Map[Int, Set[Function[SoQLAnalysisType]]]] =
    nAdicFunctions.groupBy(_.name).mapValues { fs =>
      fs.groupBy(_.minArity).mapValues(_.map(analysisify).toSet).toMap
    }.toMap

  val variadicFunctionsByNameThenMinArity: Map[FunctionName, Map[Int, Set[Function[SoQLAnalysisType]]]] =
    variadicFunctions.groupBy(_.name).mapValues { fs =>
      fs.groupBy(_.minArity).mapValues(_.map(analysisify).toSet).toMap
    }.toMap
}
