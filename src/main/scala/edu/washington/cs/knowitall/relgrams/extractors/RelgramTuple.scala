package edu.washington.cs.knowitall.relgrams.extractors

/**
 * Created with IntelliJ IDEA.
 * User: niranjan
 * Date: 2/17/13
 * Time: 11:55 AM
 * To change this template use File | Settings | File Templates.
 */


import scala.collection.JavaConversions._
import edu.washington.cs.knowitall.relgrams.typers.TypedExtractionInstance


case class RelgramTuple(docid:String, sentid:Int, extrid:Int,
                        sentence:String,
                        hashes:Iterable[Int],
                        typedExtrInstance:TypedExtractionInstance) {


}
