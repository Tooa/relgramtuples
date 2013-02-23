package edu.washington.cs.knowitall.relgrams.extractors

/**
 * Created with IntelliJ IDEA.
 * User: niranjan
 * Date: 2/17/13
 * Time: 11:53 AM
 * To change this template use File | Settings | File Templates.
 */


import scala.collection.JavaConversions._
import edu.washington.cs.knowitall.relgrams.typers.{TypedExtractionInstance, ArgumentsTyper}
import edu.washington.cs.knowitall.relgrams.utils.SentenceHasher
import edu.washington.cs.knowitall.ollie.OllieExtractionInstance
import org.slf4j.LoggerFactory
import edu.washington.cs.knowitall.tool.parse.graph.DependencyNode
import edu.washington.cs.knowitall.tool.typer.Type
import edu.washington.cs.knowitall.collection.immutable.Interval

class RelgramTuplesExtractor(extractor:Extractor, argTyper:ArgumentsTyper) {

  val logger = LoggerFactory.getLogger(this.getClass)



  def extract(docid: String, sentid: Int, sentence: String):Seq[RelgramTuple] = {
    val hashes = SentenceHasher.sentenceHashes(sentence)
    val sortedExtrInstances = extractOllieInstances(sentence)
    if (!sortedExtrInstances.isEmpty){
      val sentenceTokens = sortedExtrInstances.head._2.sentence.nodes.toSeq
      val neTypes = argTyper.getNETypes(sentenceTokens)
      var eid = 0
      val assignTypes = argTyper.assignTypes(neTypes) _
      sortedExtrInstances.flatMap(confExtrInstance => {
        val confidence = confExtrInstance._1
        val extrInstance = confExtrInstance._2
        assignTypes(extrInstance) match {
          case Some(typedExtrInstance: TypedExtractionInstance) => {
            val relgramTuple = new RelgramTuple(docid, sentid, eid, sentence, hashes, typedExtrInstance)
            eid = eid + 1
            Some(relgramTuple)
          }
          case _ => {
            logger.debug("Failed to extract head and assign types to extraction: " + extrInstance.extr.toString())
            None
          }
        }
      })
    }else{
      Seq[RelgramTuple]()
    }
  }


  val preps = ("in"::"of"::"to"::"by"::"at"::Nil).toSet

  def hasImposedPrepositions(extrInstance: OllieExtractionInstance):Boolean = {
    val template = extrInstance.pattern.pattern.toString()
    template.split("-")(0).split(" ").foreach(x => {
      if (preps.contains(x)) {
        return true
      }
    })
    return false

  }

  def hasInvertedOrdering(extrInstance: OllieExtractionInstance): Boolean = {
    val arg1Interval = extrInstance.extr.arg1.span
    val relInterval = extrInstance.extr.rel.span
    val arg2Interval = extrInstance.extr.arg2.span
    def after(x:Interval, y:Interval) = x.start > y.start
    after(arg1Interval, relInterval) || after(relInterval, arg2Interval) || after(arg1Interval, arg2Interval)
  }

  def badOllieTuple(extrInstance: OllieExtractionInstance) = {
    hasImposedPrepositions(extrInstance) || hasInvertedOrdering(extrInstance)
  }

  def extractOllieInstances(sentence: String): Seq[(Double, OllieExtractionInstance)] = {
    extractor.extract(sentence)
      .filter(confExtr => confExtr._1 > 0.1)
      .filter(confExtr => !badOllieTuple(confExtr._2))
      .toSeq
      .sorted(new OllieExtractionOrdering)
  }
}
