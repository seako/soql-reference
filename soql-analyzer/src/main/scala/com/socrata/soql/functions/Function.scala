package com.socrata.soql.functions

import com.socrata.soql.environment.FunctionName

/**
 * @note This class has identity equality semantics.
 */
class Function[+Type](val identity: String, val name: FunctionName, val parameters: Seq[Type], val repeated: Option[Type], val result: Type, val isAggregate: Boolean = false) {
  val minArity = parameters.length
  val isVariadic = repeated.isDefined

  val distinctTypes: Set[_ <: Type] = parameters.toSet ++ repeated + result

  val allParameters = repeated match {
    case Some(r) => parameters.toStream ++ Stream.continually(r)
    case None => parameters
  }

  override def toString = {
    val sb = new StringBuilder(name.toString).append(" :: ")
    sb.append(parameters.mkString("", " -> ", " -> "))
    repeated.foreach { rep =>
      sb.append(rep).append("* -> ")
    }
    sb.append(result)
    sb.toString
  }

  override final def hashCode = System.identityHashCode(this)
  override final def equals(that: Any) = this eq that.asInstanceOf[AnyRef]

  def takeParameters(n: Int) =
    if(parameters.length >= n) copy(parameters = parameters.take(n), repeated = None)
    else repeated match {
      case None => this
      case Some(r) => copy(parameters = parameters ++ Stream.fill(n - parameters.length)(r), repeated = None)
    }

  def copy[T >: Type](identity: String = identity, name: FunctionName = name, parameters: Seq[T] = parameters, repeated: Option[T] = repeated, result: T = result, isAggregate: Boolean = isAggregate) =
    new Function[T](identity, name, parameters, repeated, result, isAggregate)
}

object Function {
  def apply[Type](identity: String, name: FunctionName, parameters: Seq[Type], repeated: Option[Type], result: Type, isAggregate: Boolean = false) =
    new Function(identity, name, parameters, repeated, result, isAggregate)

  def unapply[Type](f: Function[Type]) = Some((f.identity, f.name, f.parameters, f.repeated, f.result, f.isAggregate))
}
