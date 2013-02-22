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

class RelgramTuplesExtractor(extractor:Extractor, argTyper:ArgumentsTyper) {

  val logger = LoggerFactory.getLogger(this.getClass)

  val preps = ("in"::"of"::"to"::"by"::"at"::Nil).toSet
  def isPrepImposed(string: String):Boolean = {
    string.split("-")(0).split(" ").foreach(x => {
      if (preps.contains(x)) {
        return true
      }
    })
    return false

  }

  def badOllieTuple(extrInstance: OllieExtractionInstance) = {
    val template = extrInstance.pattern.pattern.toString()
    isPrepImposed(template)
  }

  def extract(docid: String, sentid: Int, sentence: String):Seq[RelgramTuple] = {
    val hashes = SentenceHasher.sentenceHashes(sentence)
    val sortedExtrInstances = extractOllieInstances(sentence)
    def sentenceString(sentenceTokens: Seq[DependencyNode]): String = {
      sentenceTokens.map(t => t.string).mkString(",")
    }
    def typeDebug(typ: Type, sentenceTokens: Seq[DependencyNode]) = {
      typ.name + ":" + typ.source
      //"%s(%s) spans [%d, %d] and tokens (%s)".format(typ.name, typ.text, typ.interval.start, typ.interval.end, sentenceString(sentenceTokens))
    }
    if (sortedExtrInstances.size > 0){
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
            logger.error("Failed to extract head and assign types to extraction: " + extrInstance.extr.toString())
            None
          }
        }
      })
    }else{
      Seq[RelgramTuple]()
    }
  }


  def extractOllieInstances(sentence: String): Seq[(Double, OllieExtractionInstance)] = {
    extractor.extract(sentence)
      .filter(confExtr => confExtr._1 > 0.1)
      .filter(confExtr => !badOllieTuple(confExtr._2))
      .toSeq
      .sorted(new OllieExtractionOrdering)
  }
}
