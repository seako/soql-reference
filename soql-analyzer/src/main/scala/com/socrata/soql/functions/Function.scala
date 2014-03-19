package com.socrata.soql.functions

import com.socrata.soql.environment.FunctionName
import java.util.concurrent.atomic.AtomicInteger

/**
 * A container for conceptually related monomorphic functions.
 *
 * @note This class has identity equality semantics.  There should be one list of all
 *       the Functions in the system from which all function objects are taken.
 * @param identity A _unique_ name for this function that can be used to identify it when
 *                 serializing things that refer to it.
 */
abstract class Function[+Type](val identity: String, val name: FunctionName) {
  private val identityCtr = new AtomicInteger(0)

  // This should only be called from the ctor.
  protected def f[T >: Type](parameters: Seq[T], repeated: Option[T], result: T, isAggregate: Boolean = false) =
    new MonomorphicFunction[T](this, identity + " " + identityCtr.getAndIncrement, parameters, repeated, result, isAggregate)

  override def toString = "#<Function " + name + ">"

  val functions: Seq[MonomorphicFunction[Type]]

  lazy val byIdentity = functions.groupBy(_.identity).mapValues(_.head).toMap

  override final def hashCode = super.hashCode
  override final def equals(that: Any) = super.equals(that)
}

object Function {
  def simple[T](identity: String, name: FunctionName, parameters: Seq[T], repeated: Option[T], result: T, isAggregate: Boolean = false) = {
    new Function[T](identity, name) {
      val functions = List(f(parameters, repeated, result, isAggregate))
    }
  }
}
