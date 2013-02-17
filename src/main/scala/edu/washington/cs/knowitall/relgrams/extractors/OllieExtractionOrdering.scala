package edu.washington.cs.knowitall.relgrams.extractors

/**
 * Created with IntelliJ IDEA.
 * User: niranjan
 * Date: 2/16/13
 * Time: 1:58 PM
 * To change this template use File | Settings | File Templates.
 */


import scala.collection.JavaConversions._
import edu.washington.cs.knowitall.ollie.OllieExtractionInstance
import edu.washington.cs.knowitall.openparse.extract.Extraction.Part

class OllieExtractionOrdering extends scala.math.Ordering[OllieExtractionInstance] {
  def compare(xinst: OllieExtractionInstance, yinst: OllieExtractionInstance):Int = {
    val x = xinst.extr
    val y = yinst.extr
    val xarg1:Part = x.arg1
    val xrel:Part = x.rel
    val xarg2:Part = x.arg2

    val yarg1:Part = y.arg1
    val yrel:Part = y.rel
    val yarg2:Part = y.arg2

    var out = xrel.span.start.compareTo(yrel.span.start)//.getRange.getStart.compareTo(y.getRelation.getRange.getStart)
    if (out == 0){
      out = xarg1.span.start.compareTo(yarg1.span.start)
    }
    if (out == 0){
      out = xarg2.span.start.compareTo(yarg2.span.start)
    }
    if (out == 0){
      x.toString.compareTo(y.toString)
    }
    return out
  }
}
