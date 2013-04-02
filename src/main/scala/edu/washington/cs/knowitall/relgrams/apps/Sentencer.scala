package edu.washington.cs.knowitall.relgrams.apps

/**
 * Created with IntelliJ IDEA.
 * User: niranjan
 * Date: 4/1/13
 * Time: 5:14 PM
 * To change this template use File | Settings | File Templates.
 */


import scala.collection.JavaConversions._
import io.Source
import util.matching.Regex
import edu.washington.cs.knowitall.tool.sentence.OpenNlpSentencer
import java.io.PrintWriter

object Sentencer {


  def main(args:Array[String]){
    val inputFile = args(0)
    val outputFile = args(1)

    val sentencer = new OpenNlpSentencer
    val content = Source.fromFile(inputFile, "UTF-8").mkString

    val docRePattern = """(?s)<DOC\s*id="(.*?)"\s*type="(.*?)"\s*>.*?<TEXT>(.*?)</TEXT>.*?</DOC>"""
    val docRe = new Regex(docRePattern, "id", "type", "content")
    val docIterator = docRe.findAllIn(content)
    def replacePTags(docContent:String) = docContent.replaceAll("""<P>""", "\n").replaceAll("""</P>""", "")
    def removeHeadlines(docContent:String) = docContent.replaceAll("""(?s)<HEADLINE>(.*?)</HEADLINE>""", "")
    def removePlacePrefix(sentence:String) = sentence.replaceAll("""(.*?), (.*?) --""", "")
    val writer = new PrintWriter(outputFile, "UTF-8")
    for (m <- docIterator.matchData){
      val id = m.group("id")
      val docType = m.group("type")
      val docContent = m.group("content").split("\n").map(line => removePlacePrefix(line)).mkString(" ")
      val clean = replacePTags(removeHeadlines(docContent))
      val sentences = sentencer(clean).zipWithIndex
      sentences.foreach(segid => writer.println("%s\t%s\t%s\t%s".format(id, segid._2, segid._1.text.replaceAll("\n", " "), docType)))
    }
    writer.close
  }
}
