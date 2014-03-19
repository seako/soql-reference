package com.socrata.soql.typechecker

import com.socrata.soql.functions._
import com.socrata.soql.typed.Typable
import com.socrata.soql.environment.FunctionName

object UtilTypes {
  type ConversionSet[Type] = Seq[Option[Function[Type]]]
}

import UtilTypes._

sealed abstract class OverloadResult[+Type]
case object NoMatch extends OverloadResult[Nothing]
case class Ambiguous[Type](candidates: Map[Function[Type], ConversionSet[Type]]) extends OverloadResult[Type] {
  val conversionsRequired = candidates.head._2.size
  require(candidates.forall(_._2.size == conversionsRequired), "ambiguous functions do not all have the same number of possible conversions")
}
case class Matched[Type](function: Function[Type], conversions: ConversionSet[Type]) extends OverloadResult[Type]

sealed abstract class CandidateEvaluation[Type]
case class TypeMismatchFailure[Type](expected: Set[Type], found: Type, idx: Int) extends CandidateEvaluation[Type] {
  require(!expected.contains(found), "type mismatch but found in expected")
}
case class UnificationFailure[Type](found: Type, idx: Int) extends CandidateEvaluation[Type]
case class Passed[Type](conversionSet: ConversionSet[Type]) extends CandidateEvaluation[Type]

class FunctionCallTypechecker[Type](typeInfo: TypeInfo[Type], functionInfo: FunctionInfo[Type]) {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[FunctionCallTypechecker[_]])

  type Func = Function[Type]
  type Val = Typable[Type]
  type ConvSet = ConversionSet[Type]

  import typeInfo._
  import functionInfo._

  def goodArity(function: Func, parameters: Seq[Val]): Boolean = {
    if(function.isVariadic) {
      function.minArity <= parameters.length
    } else {
      function.minArity == parameters.length
    }
  }

  /** Resolve a set of overloads into a single candidate.  If you have only one, you should use
    * `evaluateCandidate` directly, as this loses type mismatch information. */
  def resolveOverload(candidates: Set[Func], parameters: Seq[Val]): OverloadResult[Type] = {
    require(candidates.nonEmpty, "empty candidate set")
    require(candidates.forall { candidate => candidate.name == candidates.iterator.next().name }, "differently-named functions") // this is _overload_ resolution!
    require(candidates.forall(goodArity(_, parameters)), "bad candidate arity")

    // "good" will be a list of pairs of functions together with the
    // conversions necessary to call those functions with these
    // parameters.
    val good = for {
      candidate <- candidates.toSeq
      Passed(conversions) <- Iterator.single(evaluateCandidate(candidate, parameters))
    } yield (candidate, conversions)

    if(good.isEmpty) {
      NoMatch
    } else {
      // grouped will be a "good" chunked by number-of-conversions-required
      val grouped = good.groupBy(_._2.count(_.isDefined))
      val minConversionsRequired = grouped.keys.min
      val bestGroup = grouped(minConversionsRequired)
      if(bestGroup.size > 1) {
        // ok, still ambiguous.  So let's try finding by minimum-number-of-distinct-types
        // The idea here is to make "null :: number" produce ::(number -> number) as its result.
        val numberOfDistinctTypes = bestGroup.map { case (func, _) =>
          func.distinctTypes.size
        }
        val minDistinct = numberOfDistinctTypes.min
        val minimalOperations = (bestGroup, numberOfDistinctTypes).zipped.filter { (_, numDistinct) => numDistinct == minDistinct }._1
        if(minimalOperations.size == 1) {
          val (bestFunc, conversions) = minimalOperations.head
          Matched(bestFunc, conversions)
        } else {
          Ambiguous[Type](bestGroup.toMap)
        }
      } else {
        val (bestFunc, conversions) = bestGroup.head
        Matched(bestFunc, conversions)
      }
    }
  }

  def evaluateCandidate(candidate: Func, parameters: Seq[Val]): CandidateEvaluation[Type] = {
    require(goodArity(candidate, parameters))
    val parameterConversions: ConvSet = (candidate.allParameters, parameters, Stream.from(0)).zipped.map { (expectedTyp, value, idx) =>
      if(canBePassedToWithoutConversion(value.typ, expectedTyp)) {
        None
      } else {
        val maybeConv = implicitConversions(value.typ, expectedTyp)
        maybeConv match {
          case Some(f) =>
            log.debug("Conversion found: {}", f.name)
            assert(f.result == expectedTyp, "conversion result is not what's expected")
            assert(f.minArity == 1, "conversion is not an arity-1 function")
            assert(canBePassedToWithoutConversion(value.typ, f.parameters(0)), "conversion does not take the type to be converted")
          case None =>
            log.debug("Type mismatch, and there is no usable implicit conversion from {} to {}", value.typ:Any, expectedTyp:Any)
            return TypeMismatchFailure(Set(expectedTyp), value.typ, idx)
        }
        maybeConv
      }
    }.toSeq

    log.debug("Successfully type-checked {}; implicit conversions {}", candidate.name:Any, parameterConversions:Any)
    Passed(parameterConversions)
  }

  def narrowDownFailure(fs: Set[Func], params: Seq[Val]): UnificationFailure[Type] = {
    // the first value we return will be the first index whose
    // addition causes a type error.
    for(paramList <- params.inits.toIndexedSeq.reverse.drop(1)) {
      val n = paramList.length
      val res = resolveOverload(fs.map { f => f.takeParameters(n) }, paramList)
      res match {
        case NoMatch =>
          val paramIdx = n - 1
          return UnificationFailure(params(paramIdx).typ, paramIdx)
        case _ => // ok, try again
      }
    }
    sys.error("Can't get here")
  }

  /** Heuristics to try to make "null" not cause type-checking failures.
   * This is kinda evil (and since no one uses literal nulls in ambiguous
   * places in expressions ANYWAY hopefully nearly useless), but meh.  Has
   * to be done. */
  def disambiguateNulls(failure: Map[Func, ConvSet], parameters: Seq[Val], nullType: Type): Option[(String, Func, ConvSet)] = {
    case class SimpleValue(typ: Type) extends Typable[Type]

    // ok.  First, if half or more of all parameters are the same type
    // after conversion, and all the rest are nulls, try to just use
    // those values in the nulls' places (WITH implicit conversions)
    // and see if that resolves things.

    // TODO: make this "the same type post-implicit conversions" which
    // means filtering the failures down to just those instances in
    // which that is true.
    val paramsByType = parameters.groupBy(_.typ)
    if(paramsByType.size == 2 && paramsByType.contains(nullType)) {
      val typedParams = (paramsByType - nullType).head._2
      if(typedParams.size >= parameters.size/2) {
        resolveOverload(failure.keySet.filter(allParamsTheSameType(_).isDefined), parameters.map { p => if(p.typ == nullType) typedParams(0) else p }) match {
          case Matched(f, c) => return Some(("half or more not-null same type", f, c))
          case _ => // nothing
        }
      }
    }

    // if there's exactly one null parameter, try the types in
    // typeParameterUniverse (WITHOUT implicit conversions) until we
    // find one that succeeds.
    if(paramsByType.contains(nullType) && paramsByType(nullType).size == 1) {
      val functionInfoWithoutImplicitConversions = new FunctionInfo[Type] {
        def implicitConversions(from: Type, to: Type) = None
        def functionsWithArity(name: FunctionName, n: Int) = functionInfo.functionsWithArity(name, n)
      }
      val withoutImplicitConversions = new FunctionCallTypechecker[Type](typeInfo, functionInfoWithoutImplicitConversions)
      for(t <- typeParameterUniverse) {
        withoutImplicitConversions.resolveOverload(failure.map(_._1).toSet, parameters.map { p => if(p.typ == nullType) SimpleValue(t) else p }) match {
          case Matched(f,c) => return Some(("one null parameter", f, c))
          case _ => // nothing
        }
      }
    } else {
      return None // there was a not-null parameter, we can't do anything.
    }

    sys.error("nyi")
  }

  def allParamsTheSameType(f: Func): Option[Type] = {
    if(f.parameters.nonEmpty && f.parameters.forall(_ == f.parameters(0))) Some(f.parameters(0))
    else None
  }
}
