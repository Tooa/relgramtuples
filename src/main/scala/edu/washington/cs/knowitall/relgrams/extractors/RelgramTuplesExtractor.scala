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

class RelgramTuplesExtractor(extractor:Extractor, argTyper:ArgumentsTyper) {

  def extractRelgramTuples(docid:String, sentid:Int, extrid:Int, sentence:String){
    val hashes = SentenceHasher.sentenceHashes(sentence)
    val sortedExtrInstances = extractor.extract(sentence)
      .filter(confExtr => confExtr._1 > 0.1)
      .map(confExtr => confExtr._2)
      .toSeq
      .sorted(new OllieExtractionOrdering)
    var eid = 0
    sortedExtrInstances.flatMap(extrInstance => {
      argTyper.assignTypes(extrInstance) match {
        case Some(typedExtrInstance:TypedExtractionInstance) => {
          val template = extrInstance.pattern.pattern.toString()
          Some(new RelgramTuple(docid, sentid, extrid, sentence, hashes, typedExtrInstance))
        }
        case None => None
      }
    })
  }

}
