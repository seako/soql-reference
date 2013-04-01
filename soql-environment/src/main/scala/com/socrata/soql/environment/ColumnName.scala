package com.socrata.soql.environment

import com.ibm.icu.lang.UCharacter
import com.ibm.icu.text.Normalizer

final class ColumnName(val name: String) extends Ordered[ColumnName] {
  lazy val caseFolded = UCharacter.foldCase(name.replaceAll("-", "_"), UCharacter.FOLD_CASE_DEFAULT)
  override lazy val hashCode = caseFolded.hashCode ^ 0x342a3466

  // two column names are the same if they share the same dataset
  // context (which any two names under comparison ought to) and if
  // they are equal following downcasing under that dataset context's
  // locale's rules.
  override def equals(o: Any) = o match {
    case that: ColumnName =>
      this.caseFolded.equals(that.caseFolded)
    case _ => false
  }

  def compare(that: ColumnName) =
    this.caseFolded.compareTo(that.caseFolded)

  override def toString = name
}

object ColumnName extends (String => ColumnName) {
  def apply(columnName: String) = new ColumnName(columnName)
}