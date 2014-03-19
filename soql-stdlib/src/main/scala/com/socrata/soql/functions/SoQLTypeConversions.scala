package com.socrata.soql.functions

import scala.collection.JavaConverters._

import com.socrata.soql.collection.OrderedSet
import com.socrata.soql.types._
import com.socrata.soql.types.SoQLNumberLiteral
import com.socrata.soql.parsing.Lexer
import com.socrata.soql.tokens.{EOF, NumberLiteral}

object SoQLTypeConversions {
  val typeParameterUniverse: OrderedSet[SoQLAnalysisType] = OrderedSet(
    SoQLText,
    SoQLNumber,
    SoQLDouble,
    SoQLMoney,
    SoQLBoolean,
    SoQLFixedTimestamp,
    SoQLFloatingTimestamp,
    SoQLDate,
    SoQLTime,
    SoQLLocation,
    SoQLObject,
    SoQLArray,
    SoQLID,
    SoQLVersion
  )

  private def getMonomorphically(f: Function[SoQLType]): MonomorphicFunction[SoQLType] = {
    val Seq(res) = f.functions
    res
  }

  private val textToFixedTimestampFunc =
    Some(getMonomorphically(SoQLFunctions.TextToFixedTimestamp))
  private val textToFloatingTimestampFunc =
    Some(getMonomorphically(SoQLFunctions.TextToFloatingTimestamp))
  private val textToDateFunc =
    Some(getMonomorphically(SoQLFunctions.TextToDate))
  private val textToTimeFunc =
    Some(getMonomorphically(SoQLFunctions.TextToTime))
  private val numberToMoneyFunc =
    Some(getMonomorphically(SoQLFunctions.NumberToMoney))
  private val numberToDoubleFunc =
    Some(getMonomorphically(SoQLFunctions.NumberToDouble))
  private val textToRowIdFunc =
    Some(getMonomorphically(SoQLFunctions.TextToRowIdentifier))
  private val textToRowVersionFunc =
    Some(getMonomorphically(SoQLFunctions.TextToRowVersion))
  private val textToNumberFunc =
    Some(getMonomorphically(SoQLFunctions.TextToNumber))
  private val textToMoneyFunc =
    Some(getMonomorphically(SoQLFunctions.TextToMoney))

  private def isNumberLiteral(s: String) = try {
    val lexer = new Lexer(s)
    lexer.yylex() match {
      case NumberLiteral(_) => lexer.yylex() == EOF()
      case _ => false
    }
  } catch {
    case e: Exception =>
      false
  }

  def implicitConversions(from: SoQLAnalysisType, to: SoQLAnalysisType): Option[MonomorphicFunction[SoQLType]] = {
    (from,to) match {
      case (SoQLTextLiteral(SoQLFixedTimestamp.StringRep(_)), SoQLFixedTimestamp) =>
        textToFixedTimestampFunc
      case (SoQLTextLiteral(SoQLFloatingTimestamp.StringRep(_)), SoQLFloatingTimestamp) =>
        textToFloatingTimestampFunc
      case (SoQLTextLiteral(SoQLDate.StringRep(_)), SoQLDate) =>
        textToDateFunc
      case (SoQLTextLiteral(SoQLTime.StringRep(_)), SoQLTime) =>
        textToTimeFunc
      case (SoQLNumberLiteral(num), SoQLMoney) =>
        numberToMoneyFunc
      case (SoQLNumberLiteral(num), SoQLDouble) =>
        numberToDoubleFunc
      case (SoQLTextLiteral(s), SoQLID) if SoQLID.isPossibleId(s) =>
        textToRowIdFunc
      case (SoQLTextLiteral(s), SoQLVersion) if SoQLVersion.isPossibleVersion(s) =>
        textToRowVersionFunc
      case (SoQLTextLiteral(s), SoQLNumber) if isNumberLiteral(s.toString) =>
        textToNumberFunc
      case (SoQLTextLiteral(s), SoQLMoney) if isNumberLiteral(s.toString) =>
        textToMoneyFunc
      case _ =>
        None
    }
  }

  def canBePassedToWithoutConversion(actual: SoQLAnalysisType, expected: SoQLAnalysisType) =
    actual.isPassableTo(expected)
}
