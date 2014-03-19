package com.socrata.soql.functions

import com.socrata.soql.environment.FunctionName


class MonomorphicFunction[+Type](val function: Function[Type],
                                 val identity: String,
                                 val parameters: Seq[Type],
                                 val repeated: Option[Type],
                                 val result: Type,
                                 val isAggregate: Boolean = false)
{
  def allParameters = repeated match {
    case Some(r) => parameters.toStream ++ Stream.continually(r)
    case None => parameters
  }
  val name = function.name
  val minArity = parameters.length
  val isVariadic = repeated.isDefined

  override def toString = {
    val sb = new StringBuilder(function.name.toString).append(" :: ")
    sb.append(parameters.mkString("", " -> ", " -> "))
    repeated.foreach { r =>
      sb.append(r).append("* -> ")
    }
    sb.append(result)
    sb.toString
  }

  def takeParameters(n: Int) = {
    if(n < minArity) new MonomorphicFunction(function, identity, parameters.take(n), None, result, isAggregate)
    else repeated match {
      case None => this
      case Some(r) => new MonomorphicFunction(function, identity, parameters ++ Seq.fill(n - minArity)(r), None, result, isAggregate)
    }
  }
}
