package edu.washington.cs.knowitall.relgrams.typers

import java.io.{BufferedInputStream, FileInputStream, File}
import edu.washington.cs.knowitall.common.Resource._
import edu.washington.cs.knowitall.tool.typer.{Typer, Type, StanfordNer}
import edu.stanford.nlp.ie.crf.CRFClassifier
import java.util.zip.GZIPInputStream
import edu.washington.cs.knowitall.tool.tokenize.Token
import edu.washington.cs.knowitall.tool.postag.PostaggedToken
import org.slf4j.LoggerFactory
import edu.washington.cs.knowitall.collection.immutable.Interval
import edu.stanford.nlp.ie.AbstractSequenceClassifier
import edu.washington.cs.knowitall.tool.parse.graph.DependencyNode

/**
 * Created with IntelliJ IDEA.
 * User: niranjan
 * Date: 2/14/13
 * Time: 11:27 AM
 * To change this template use File | Settings | File Templates.
 */

class MyStanfordNer(private val classifier: AbstractSequenceClassifier[_]) {//extends Typer[Token]("Stanford", "Stanford") {


  def buildString(nodes: Iterable[Token]) = {
      val builder = new StringBuilder()
      for (node <- nodes) {
        builder.append(" " * (node.offset - builder.length))
        builder.append(node.string)
      }
      builder.toString()
  }

  def apply(text: String, seq: Seq[Token]): List[Type] = {
    import scala.collection.JavaConverters._
    //println("Text: " + text)
    //println("Seq: " + seq.mkString("__"))
    val response = classifier.classifyToCharacterOffsets(text).asScala
    var tags = List.empty[Type]

    def endsMatch(tokenInterval:Interval, entityInterval:Interval):Boolean = {
      //if(tokenInterval.end == entityInterval.end) return true
      if ((entityInterval.end >= tokenInterval.start) && (entityInterval.end <= tokenInterval.end)) return true
      return false
    }
    def startsMatch(tokenInterval:Interval, entityInterval:Interval):Boolean = tokenInterval.start == entityInterval.start

    for (triple <- response) {
      val nerInterval = Interval.open(triple.second, triple.third)
      val nerType = triple.first
      // find actual token offsets from NER offsets
      val start = seq.find(token => startsMatch(token.interval, nerInterval)).map(_.interval.start)
      val end = seq.find(token => endsMatch(token.interval, nerInterval)).map(_.interval.end)
      for (s <- start; e <- end) {
        val origInterval = Interval.open(s,e)
        val entityText = text.substring(nerInterval.start, nerInterval.end)
        val typ = new Type("Stanford" + nerType, "Stanford", origInterval, entityText)
        tags ::= typ
      }
    }
    if (tags.size != response.size){
      println("#Tags = %d but #Entities = %d\ntext: %s\nnerIntervals: %s\nTokens: %s".format(tags.size, response.size, text, response.map(x => x.first + ":" + x.second + "_" + x.third).mkString(","), seq.map(y => y.string + ":" + y.interval)))
    }
    tags
  }

 /** def apply(text: String, seq: Seq[Token], originalStarts:Map[Int, Int], originalEnds:Map[Int, Int]) = {
    import scala.collection.JavaConverters._

    val response = classifier.classifyToCharacterOffsets(text).asScala

    var tags = List.empty[Type]
    for (triple <- response) {
      val nerInterval = Interval.open(triple.second, triple.third)
      val nerType = triple.first
      // find actual token offsets from NER offsets
      val start = seq.find(_.interval.start == nerInterval.start).map(_.interval.start)
      val end = seq.find(_.interval.end == nerInterval.end).map(_.interval.end)
      for (s <- start; e <- end) {
        val origInterval = Interval.open(originalStarts(s), originalEnds(e))
        val entityText = text.substring(nerInterval.start, nerInterval.end)
        val typ = new Type("Stanford" + nerType, "Stanford", origInterval, entityText)
        tags ::= typ
      }
    }
    if (tags.size != response.size){
      println("#Tags = %d but #Entities = %d\ntext: %s\nnerIntervals: %s\nTokens: %s".format(tags.size, response.size, text, response.map(x => x.first + ":" + x.second + "_" + x.third).mkString(","), seq.map(y => y.string + ":" + y.interval)))
    }
    tags
  } */

  def apply(nodes:Seq[Token]) : List[Type] = apply(buildString(nodes), nodes)
  //def apply(seq: Seq[Token], originalStarts:Map[Int, Int], originalEnds:Map[Int, Int]) = apply(seq.iterator.map(_.string).mkString(" "), seq, originalStarts, originalEnds)
}
object NETyper{
  def fromModelUrl(file: File) = {
    using (new FileInputStream(file)) { stream =>
      println("Loading NE tagger from model file: " + file.getAbsolutePath)
      new MyStanfordNer(CRFClassifier.getClassifier(new BufferedInputStream(new GZIPInputStream(stream))))
    }
  }

}
class PersonTyper {
  def assignTypes(argTokens:Seq[PostaggedToken]) = {
    if (argTokens.size == 1 && argTokens(0).string.equals("people")){
      new Type("person", "People", argTokens(0).interval, argTokens(0).string)::Nil
    }else{
      Iterable[Type]()
    }
  }
}
class PronounTyper{
  val allowedPronouns = "he,she,they,you,we,i,me,him,her,them,us".split(",").toSet
  def assignTypes(argTokens:Seq[PostaggedToken]) = {
    if (argTokens.size == 1
      && allowedPronouns.contains(argTokens(0).string.toLowerCase)
      && argTokens(0).isPronoun){
      new Type("person", "Pronoun", argTokens(0).interval, argTokens(0).string)::Nil
    }else{
      Iterable[Type]()
    }


  }
}

object NumberFinder{

  val logger = LoggerFactory.getLogger(this.getClass)
  val fractionOnlyRe = """^([0-9]+)\.([0-9])+$""".r
  val numbersRe = """[0-9]+""".r
  val numberOnlyRe = """^[0-9]+$""".r

  def findNumbers(argTokens:Seq[PostaggedToken]) = {
    val argString = argTokens.map(at => at.string).mkString(" ")
    val numType = new Type("number", "NUM", TypersUtil.span(argTokens), argString)
    if (argTokens.find(token => !token.postag.equals("CD")) == None){
      numType::Nil
    }
    else {
      if (fractionOnlyRe.findFirstIn(argString) != None){
        numType::Nil
      }else{
        Iterable[Type]()
      }
    }
  }

}

object DayTyper{
  val daysOfTheWeek = "monday,tuesday,wednesday,thursday,friday,saturday,sunday".split(",").toSet
  def assignTypes(argTokens:Seq[PostaggedToken]):Iterable[Type] = {
    val argstring = argTokens(0).string.toLowerCase()
    val dayType = new Type("time_unit", "Day", TypersUtil.span(argTokens), argstring)
    if (argTokens.size == 1 && daysOfTheWeek.contains(argstring)) {
      dayType::Nil
    }else{
      Iterable[Type]()
    }
  }
}
class NETyper(val ne7modelFile:String, ne3ModelFile:String) {
  import NETyper._
  val ne3typer = fromModelUrl(new File(ne3ModelFile))
  val ne7typer = fromModelUrl(new File(ne7modelFile))

  def rename(old: Type): Type = {
    var newName = old.name.replaceAll("""Stanford""", "").toLowerCase
    newName = if (newName.equals("date")) "time_unit" else newName
    new Type(newName, old.source, old.interval, old.text)
  }


  val notAlphaNumericRe_1 = """^.*[^a-zA-Z0-9-:,.]+[a-zA-Z0-9]+""".r // can't, don't etc.
  val notAlphaNumericRe_2 = """^[^a-zA-Z0-9_]+""".r // 's, ``, $ etc.
  val endsWithSpecialCharRe = """^[a-zA-Z0-9].*[^a-zA-Z0-9]$""".r
  def endsWithSpecialChar(string:String) = endsWithSpecialCharRe.findFirstMatchIn(string) != None
  def isNotAlphaNumeric(string:String):Boolean = {
    if (endsWithSpecialChar(string)) return false
    return notAlphaNumericRe_1.findFirstMatchIn(string) != None  || notAlphaNumericRe_2.findFirstMatchIn(string) != None
  }

  def preprocessTokens(tokens:Seq[Token]) = {
    var inc = 0
    var starts = Map[Int, Int]()
    var ends = Map[Int, Int]()
    val newTokens = tokens.map(token => {
      val newToken = new Token(token.string, token.offset + inc)
      val newStart = token.interval.start + inc
      val newEnd = token.interval.end + inc
      starts += newStart -> token.interval.start
      ends += newEnd -> token.interval.end

      if (isNotAlphaNumeric(token.string)){//(token.string.equals("'s") || token.string.equals(",") || token.string.equals("$")){
        inc = inc + 1
      }
      newToken
    })
    (newTokens, starts, ends)

  }

  def preprocessTokensDontAdjust(tokens:Seq[Token]) = {
    var inc = 0
    var starts = Map[Int, Int]()
    var ends = Map[Int, Int]()
    val newTokens = tokens.map(token => {
      val newToken = new Token(token.string, token.offset + inc)
      val newStart = token.interval.start
      val newEnd = token.interval.end
      starts += newStart -> token.interval.start
      ends += newEnd -> token.interval.end
      newToken
    })
    (newTokens, starts, ends)
  }


  def assignTypes(tokens:Seq[Token]):List[Type] = {
    def isNotOrgPerLoc(typ: Type) = !(typ.name.startsWith("person") || typ.name.startsWith("organization") || typ.name.startsWith("location"))
    ne7typer(tokens).filter(typ => isNotOrgPerLoc(typ))
                    .map(typ => rename(typ)) ++
    ne3typer(tokens).map(typ => rename(typ))
  }
  /**def assignTypesToSentence(sentenceTokens:Seq[Token]) = {
    val (tokens, originalStarts, originalEnds) = preprocessTokens(sentenceTokens)
    assignTypes(tokens, originalStarts, originalEnds)
  }
  def assignTypesToSentenceNoAdjustments(sentenceTokens:Seq[Token]) = {
    val (tokens, originalStarts, originalEnds) = preprocessTokensDontAdjust(sentenceTokens)
    assignTypes(tokens, originalStarts, originalEnds)
  }


  def assignTypes(tokens: Seq[Token], originalStarts: Map[Int, Int], originalEnds: Map[Int, Int]): List[Type] = {
    val sentenceText = tokens.map(_.string).mkString(" ")
    def isNotOrgPerLoc(typ: Type) = typ.name.startsWith("person") || typ.name.startsWith("organization") || typ.name.startsWith("location")
    val types = ne7typer(tokens).filter(typ => isNotOrgPerLoc(typ)).map(typ => rename(typ)) ++ ne3typer(tokens).map(typ => rename(typ))

/**    val types = ne7typer(sentenceText, tokens, originalStarts, originalEnds).filter(typ => isNotOrgPerLoc(typ)).map(typ => rename(typ)) ++
      ne3typer(sentenceText, tokens, originalStarts, originalEnds).map(typ => rename(typ))*/
    types
  }  */



}
