package com.socrata.soql.names

import com.ibm.icu.text.{Normalizer, Collator}
import com.ibm.icu.util.ULocale
import com.ibm.icu.lang.UCharacter

final class TypeName(val name: String) extends Ordered[TypeName] {
  import TypeName._

  val canonicalName = Normalizer.normalize(UCharacter.toLowerCase(locale, name.replaceAll("-","_")), Normalizer.DEFAULT)

  override def hashCode = canonicalName.hashCode ^ 0x32fa2313

  override def equals(o: Any) = o match {
    case that: TypeName =>
      collator.compare(this.canonicalName, that.canonicalName) == 0
    case _ => false
  }

  def compare(that: TypeName) = {
    collator.compare(this.canonicalName, that.canonicalName)
  }

  override def toString = canonicalName
}

object TypeName extends (String => TypeName) {
  val locale = ULocale.ENGLISH
  val collator = Collator.getInstance(locale)

  def apply(functionName: String) = new TypeName(functionName)
}