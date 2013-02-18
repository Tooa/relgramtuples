package edu.washington.cs.knowitall.relgrams.utils

/**
 * Created with IntelliJ IDEA.
 * User: niranjan
 * Date: 2/17/13
 * Time: 11:39 AM
 * To change this template use File | Settings | File Templates.
 */


import scala.collection.JavaConversions._

object SentenceHasher {

  def main(args:Array[String]){
    val asentence = "This is Barack Obama, our president, who won the presidency handsomely."
    val bsentence = " no matter what you think, Who won the presidency handsomely.This is Barack Obama; "
    println(sentenceHashes(asentence))
    println(sentenceHashes(bsentence))
  }
  val delims = "[.,!?:;]+"
  def sentenceHashes(insentence:String) = {
    val sentence = insentence.toLowerCase()
    val sentenceHashCode = sentence.replaceAll("[^a-zA-Z0-9]", "").hashCode
    sentenceHashCode::Nil ++ sentence.split(delims)
                                     .filter(split => split.split(" ").size >= 5)
                                     .map(split => split.replaceAll("[^a-zA-Z0-9]", "").hashCode)
  }

}
