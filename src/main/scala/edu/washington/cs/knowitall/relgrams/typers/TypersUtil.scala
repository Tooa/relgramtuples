package edu.washington.cs.knowitall.relgrams.typers

import edu.washington.cs.knowitall.tool.tokenize.Token
import edu.washington.cs.knowitall.collection.immutable.Interval

/**
 * Created with IntelliJ IDEA.
 * User: niranjan
 * Date: 2/14/13
 * Time: 3:49 PM
 * To change this template use File | Settings | File Templates.
 */
object TypersUtil {

  def span(tokens:Seq[Token]) = {
    val (start:Int, end:Int) = startEnd(tokens)
    Interval.open(start, end)
  }
  def startEnd(tokens:Seq[Token]) = tokens.map(t => (t.interval.start, t.interval.end)).maxBy(x => (x._1, -x._2))
}
