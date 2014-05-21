package org.rakam.util

import scala.util.parsing.combinator.RegexParsers
import scala.concurrent.duration.Duration

/**
 * Created by buremba on 30/04/14.
 */
abstract class ScalaTime {
  var period : Int
  var cursor_timestamp : Long = -1

  def this(period : Int) {
    this()
    this.period = period
  }

  def span(ts : Int) {
    cursor_timestamp = ts
    val passed = ts / period
    return passed * period
  }

  def previous() {
    if(cursor_timestamp == -1)
      throw new IllegalStateException("you must set cursor timestamp before using this method")
    cursor_timestamp -= period
    return this
  }

  def next() {
    if(cursor_timestamp == -1)
      throw new IllegalStateException("you must set cursor timestamp before using this method")
    cursor_timestamp += period
    return this
  }

}
/*
object DurationParser extends RegexParsers {
  override def skipWhitespace = false
  val parser = "(?:([0-9]+)(month)s?)?(?: ([0-9]+)(week)s?)?(?: ([0-9]+)(day)s?)?(?: ([0-9]+)(hour)s?)?(?: ([0-9]+)(min)s?)?".r

  def parse(str: String) : ScalaTime = {
    val z = parse(parser, str)
    new ScalaTime(3333)
  }
}*/