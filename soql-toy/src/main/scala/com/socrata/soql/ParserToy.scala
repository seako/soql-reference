package com.socrata.soql

import scala.util.control.Breaks._

import com.ibm.icu.util.ULocale

import com.socrata.soql.names.ColumnName
import com.socrata.soql.parsing.{BadParseException, LexerError, Parser}
import com.socrata.collection.OrderedSet

object ParserToy extends (Array[String] => Unit) {
  def fail(msg: String) = {
    System.err.println(msg)
    sys.exit(1)
  }

  def menu() {
    println("1) selectStatement")
    println("2) selection")
    println("3) expression")
    println("4) orderings")
    println("5) groupBys")
    println("6) limit")
    println("7) offset")
  }

  def apply(args: Array[String]) {
    implicit val dstctx = new DatasetContext {
      val locale = ULocale.ENGLISH
      val columns = OrderedSet.empty[ColumnName]
    }
    var last = -1
    menu()
    val p = new Parser
    while(true) {
      val cmd = readLine(if(last != -1) "["+last+"]> " else "> ")
      if(cmd == null) return;
      breakable {
        val f = cmd match {
          case "1" => p.selectStatement _
          case "2" => p.selection _
          case "3" => p.expression _
          case "4" => p.orderings _
          case "5" => p.groupBys _
          case "6" => p.limit _
          case "7" => p.offset _
          case "?" => menu(); break(); sys.error("can't get here")
          case other => println("bad command"); break(); sys.error("can't get here")
        }
        val input = readLine("... ")
        if(input == null) return;
        try {
          println(f(input))
        } catch {
          case e: LexerError => println(e.getMessage)
          case e: BadParseException => println(e.getMessage)
        }
      }
    }
  }
}