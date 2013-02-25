package edu.washington.cs.knowitall.relgrams.typers

import java.io.{BufferedInputStream, FileInputStream, File}
import edu.washington.cs.knowitall.common.Resource._
import edu.washington.cs.knowitall.tool.typer.{Type, StanfordNer}
import edu.stanford.nlp.ie.crf.CRFClassifier
import java.util.zip.GZIPInputStream
import edu.washington.cs.knowitall.tool.tokenize.Token
import edu.washington.cs.knowitall.tool.postag.PostaggedToken
import org.slf4j.LoggerFactory

/**
 * Created with IntelliJ IDEA.
 * User: niranjan
 * Date: 2/14/13
 * Time: 11:27 AM
 * To change this template use File | Settings | File Templates.
 */
object NETyper{
  def fromModelUrl(file: File) = {
    using (new FileInputStream(file)) { stream =>
      new StanfordNer(CRFClassifier.getClassifier(new BufferedInputStream(new GZIPInputStream(stream))))
    }
  }
  val defaultURL = "/Users/niranjan/work/projects/git/scala/argtyping/src/main/resources/english.muc.7class.nodistsim.prop"

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
class NETyper(val modelFile:String) {
  import NETyper._
  val netyper = fromModelUrl(new File(modelFile))

  def rename(old: Type): Type = {
    var newName = old.name.replaceAll("""Stanford""", "").toLowerCase
    newName = if (newName.equals("date")) "time_unit" else newName
    new Type(newName, old.source, old.interval, old.text)
  }

  def assignTypesToSentence(sentenceTokens:Seq[Token]) = netyper(sentenceTokens).map(typ => rename(typ))

}
