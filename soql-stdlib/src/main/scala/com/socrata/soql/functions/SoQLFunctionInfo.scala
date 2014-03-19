package com.socrata.soql.functions

import com.socrata.soql.typechecker.FunctionInfo
import com.socrata.soql.types.{SoQLAnalysisType, SoQLType}
import com.socrata.soql.environment.FunctionName
import com.socrata.soql.types.obfuscation.CryptProvider

object SoQLFunctionInfo extends FunctionInfo[SoQLAnalysisType] {
  def functionsWithArity(name: FunctionName, n: Int) =
    SoQLFunctions.nAdicMonomorphicFunctionsByNameThenArity.get(name) match {
      case Some(funcsByArity) =>
        funcsByArity.get(n) match {
          case Some(fs) =>
            fs
          case None =>
            variadicFunctionsWithArity(name ,n)
        }
      case None =>
        variadicFunctionsWithArity(name, n)
    }

  def variadicFunctionsWithArity(name: FunctionName, n: Int): Set[MonomorphicFunction[SoQLAnalysisType]] = {
    SoQLFunctions.variadicMonomorphicFunctionsByNameThenMinArity.get(name) match {
      case Some(funcsByArity) =>
        var result = Set.empty[MonomorphicFunction[SoQLAnalysisType]]
        var i = n
        while(i >= 0) {
          funcsByArity.get(i) match {
            case Some(fs) => result ++= fs
            case None => // nothing
          }
          i -= 1
        }
        result
      case None =>
        Set.empty
    }
  }

  val typeConversions = SoQLTypeConversions
  def implicitConversions(from: SoQLAnalysisType, to: SoQLAnalysisType) = typeConversions.implicitConversions(from, to)
}
