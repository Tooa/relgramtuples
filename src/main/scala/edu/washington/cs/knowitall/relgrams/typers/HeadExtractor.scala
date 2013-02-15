package edu.washington.cs.knowitall.relgrams.typers

/**
 * Created with IntelliJ IDEA.
 * User: niranjan
 * Date: 2/14/13
 * Time: 2:26 PM
 * To change this template use File | Settings | File Templates.
 */




import scala.collection.JavaConversions._

import edu.washington.cs.knowitall.relgrams.utils.Pairable
import collection.mutable.{HashSet, ArrayBuffer}
import java.lang.String
import edu.washington.cs.knowitall.normalization.NormalizedField
import edu.washington.cs.knowitall.tool.postag.PostaggedToken
import collection.mutable
import scala.Predef._
import scala.Some

import edu.washington.cs.knowitall.tool.stem.MorphaStemmer
import io.Source

/**
 * Created by IntelliJ IDEA.
 * User: niranjan
 * Date: 11/26/11
 * Time: 10:20 PM
 * To change this template use File | Settings | File Templates.
 */

object HeadExtractor {


  def main(args:Array[String]){
    val testCasesFile = args(0)
    Source.fromFile(testCasesFile).getLines.foreach(line => {
      val splits = line.split("\t")
      val arg1Text = splits(0)
      val arg1PosTags = splits(1)
      println("Line: " + line)
      println("Head(%s)=%s".format(arg1Text, lemmatizedArgumentHead(arg1Text, arg1PosTags)))
    })
  }


  val stopTagsForRelation = new HashSet[String]
  stopTagsForRelation += "MD"
  stopTagsForRelation += "JJ"
  stopTagsForRelation += "JJR"
  stopTagsForRelation += "JJS"
  stopTagsForRelation += "CC"
  stopTagsForRelation += "UH"
  stopTagsForRelation += "RP"
  stopTagsForRelation += "PRP"
  stopTagsForRelation += "PRP$"
  stopTagsForRelation += "DT"
  stopTagsForRelation += "WP"
  stopTagsForRelation += "WP$"
  stopTagsForRelation += "WRB"





  val stopwordsForRelations = new HashSet[String]
  stopwordsForRelations += "be"
  stopwordsForRelations += "have"
  stopwordsForRelations += "it"
  stopwordsForRelations += "he"
  stopwordsForRelations += "she"
  stopwordsForRelations += "they"
  stopwordsForRelations += "them"
  stopwordsForRelations += "his"
  stopwordsForRelations += "her"
  stopwordsForRelations += "their"
  stopwordsForRelations += "its"
  stopwordsForRelations += "this"
  stopwordsForRelations += "that"
  stopwordsForRelations += "these"
  stopwordsForRelations += "whose"
  stopwordsForRelations += "'s"

  def headWordsForRelation(relText:String, relPosTagString:String): String = {
    headWordsForRelation(relText.split(" "), relPosTagString.split(" ")) match{
      case Some(x:String) => x
      case None => relText
    }
  }
  def headWordsForRelation(words: Seq[String], postags: Seq[String]): Option[String] = {
    var outtokens = new ArrayBuffer[String]
    for (i <- 0 until words.size) {
      val token = words(i)
      val pos = postags(i)
      if (stopTagsForRelation.contains(pos) == false &&
        stopwordsForRelations.contains(token) == false) {
        outtokens += token
      }

    }
    if (outtokens.size >= 0) {
      return Some(words.mkString(" "))
    }
    None
  }



  val pronouns = ("you"::"i"::"he"::"she"::"they"::"it"::"them"::"him"::"her"::"whom"::Nil).toSet


  def replacePronouns(text: String): String = {
    if (pronouns.contains(text)){
      return "[PRN]"
    }else{
      return text
    }

  }


  val whwords = ("what"::"which"::"who"::"whose"::"that"::"where"::"when"::Nil).toSet
  def isConjunction(token:PostaggedToken) = {
    token.postag.startsWith("CC") || token.postag.startsWith("W") || whwords.contains(token.string)
  }
  def isPreposition(token:PostaggedToken) = token.postag.startsWith("IN")

  def removeTokensAfterConjunctionsOrPrepositions(tokens: Seq[PostaggedToken]): Seq[PostaggedToken] = {
    var firstConjPrepIndex = tokens.indexWhere(token => isConjunction(token) || isPreposition(token))
    var firstNounIndex = tokens.indexWhere(token => token.isNoun)
    if(firstConjPrepIndex > 0 && firstNounIndex < firstConjPrepIndex) {
      val subTokens = tokens.take(firstConjPrepIndex)
      subTokens
    }else{
      tokens
    }

  }


  //val punctuationRe = """[^a-zA-Z0-9]""".r
  val leadingModPatterns = """^(DT|CD|(DT*) JJ|JJ|RBS) of""".r

  def dropLeadingModifierOfPatterns(tokens:Seq[PostaggedToken]):Seq[PostaggedToken] = {
    val leadingOfIndex = tokens.indexWhere(token => token.string.equals("of"))
    if(leadingOfIndex > 0){
      val posTokenString = tokens.take(leadingOfIndex+1).map(token => if(token.string.equals("of")) "of" else token.postag).mkString(" ")
      if(leadingModPatterns.findFirstIn(posTokenString) != None){
        return tokens.slice(leadingOfIndex+1,tokens.size)
      }
    }
    return tokens
  }


  def removeTokensBeforeAppositive (tokens: Seq[PostaggedToken]): Seq[PostaggedToken] = {
    val apostropheIndex = tokens.indexWhere(token => token.string.equals("POS") || token.postag.equals("POS"))
    if(apostropheIndex > 0 && (apostropheIndex+1) < tokens.size){
      tokens.drop(apostropheIndex+1)
    }else{
      tokens
    }
  }
  val punctuationOtherThanDotRe = """[^a-zA-Z0-9.]""".r
  def removeTokensWithPunctuation(tokens:Seq[PostaggedToken]): Seq[PostaggedToken] = {
    tokens.filter(p => punctuationOtherThanDotRe.findFirstIn(p.string) == None)
  }

  val punctuationOtherThanDotDollarRe = """[^a-zA-Z0-9.$]""".r
  val alphaNumericDotOrDollarRe = """[^a-zA-Z0-9.$]""".r
  //If the entire token is a punctuation then
  def removeTokensAfterPunctuation(tokens: Seq[PostaggedToken]): Seq[PostaggedToken] = {
    val puncIndex = tokens.indexWhere(token => alphaNumericDotOrDollarRe.findFirstIn(token.string) == None)
    if (puncIndex > 0) {
      tokens.take(puncIndex)
    }else{
      tokens
    }
  }

  var wnHome = "/Users/niranjan/work/local/wordnet3.0"
  var wnTypers = new mutable.HashMap[Thread, WordNetTyper] with mutable.SynchronizedMap[Thread, WordNetTyper]
  def setWnHome(wnHomeInp:String){
    wnHome = wnHomeInp
  }
  def getWordnetTyper(wnhome: String, wnTypesFile:String) = new WordNetTyper(wnhome, wnTypesFile, 1::Nil, 3, true, true)

  def findNPofNP(tokens:Seq[PostaggedToken]):Seq[PostaggedToken] = {
    val leadingOfIndex = tokens.indexWhere(token => token.string.equals("of") || token.string.equals("in"))
    if(leadingOfIndex > 0){

      //println("leading of index: " + leadingOfIndex + " for tokens: " + tokens.map(x => x.string).mkString(" "))
      /**
       * First step is to find the head noun of an argument phrase, as follows:
          If the phrase begins with "NP of NP", check the WN type and hypernyms of the first NP.
          If the first NP has type { number[n2], group[n1], quantity[n1], part[n1],amount[n1] }, then remove "NP of".
          If the phrase begins with "Adj of NP", then remove "Adj of NP".  (example:"some of NP", "any of NP").
          Find first token of remaining phrase that is not in { Noun, Adj, Determiner}. Truncate phrase at that word.
       */
      val posTokenString = tokens.take(leadingOfIndex+1).map(token => {
        if(token.string.equals("of") || token.string.equals("in")) "of" else token.postag
      }).mkString(" ")

      if(leadingModPatterns.findFirstIn(posTokenString) != None){
        //println("dropping leading of tokens from: " + tokens.mkString(" ") + " with " + posTokenString)
        return tokens.drop(leadingOfIndex+1)
      }

      var leadingNPs = tokens.take(leadingOfIndex).filter(x => x.isNoun)
      var trailingNPs = tokens.drop(leadingOfIndex+1).filter(x => x.isNoun)

      if (leadingNPs.find(x => !x.isProperNoun) == None){
        return tokens
      }
      val wnTyper = wnTypers.getOrElseUpdate(Thread.currentThread(), getWordnetTyper(wnHome, ""))
      leadingNPs.filter(x => !x.isProperNoun).foreach(x => if (wnTyper.isGroupQuantityAmountNumberOrPart(x)) return trailingNPs)
      return leadingNPs
    }
    return tokens
  }



  private def stem(intoken: String, posTag: String): String = MorphaStemmer.stem(intoken, posTag)


  def lemmatizedArgumentHead(argText:String, argPosTagString:String):Option[String] = {
    try{
      import edu.washington.cs.knowitall.relgrams.utils.Pairable._
      val postaggedTokens = (argPosTagString.split(" ").toSeq pairElements argText.split(" ")).map(x => new PostaggedToken(x._1, x._2, offset=0))
      lemmatizedArgumentHead(postaggedTokens)
    }catch{
      case e:Exception => {
        println("Caught exception processing arg: " + argText + " with postags: " + argPosTagString)
        None
      }
    }
  }

  private def lemmatize(tokens: Seq[PostaggedToken]): String = tokens.map(token => stem(token.string, token.postag)).mkString(" ")

  def lemmatizedArgumentHead(tokens:Seq[PostaggedToken]):Option[String] = argumentHead(tokens) match {
    case Some(headTokens:Seq[PostaggedToken]) => Some(lemmatize(headTokens))
    case None => None
  }

  def argumentHead(argText:String, argPosTagString:String):Option[Seq[PostaggedToken]] = {
    try{
      import edu.washington.cs.knowitall.relgrams.utils.Pairable._
      val postaggedTokens = (argPosTagString.split(" ").toSeq pairElements argText.split(" ")).map(x => new PostaggedToken(x._1, x._2, offset=0))
      argumentHead(postaggedTokens)
    }catch{
      case e:Exception => {
        println("Caught exception processing arg: " + argText + " with postags: " + argPosTagString)
        None
      }
    }
  }

  def argumentHead(tokens:Seq[PostaggedToken]):Option[Seq[PostaggedToken]] = {

    val argString = tokens.map(token => token.string).mkString(" ")
    var subTokens = tokens.take(tokens.indexWhere(p => p.postag.startsWith("W")))
    /**val w_index = tokens.indexWhere(p => p.postag.startsWith("W"))
    subTokens = if(w_index > 0){
      tokens.take(w_index)
    }else{
      tokens
    }  */

    subTokens = findNPofNP(subTokens)
    if (subTokens.isEmpty){
      subTokens = tokens
    }

    val matchWord = "southeast"

    if(argString.contains(matchWord)){
      println("After NP of NP: " + argString + " --> " + subTokens.mkString(","))
    }

    var returnTokens = removeTokensBeforeAppositive(subTokens) //Jan 30    -- should fix Fazil 's rank to rank instead of Fazil.
    if (!returnTokens.isEmpty){
      subTokens = returnTokens
    }

    returnTokens = removeTokensAfterPunctuation(subTokens)
    if (!returnTokens.isEmpty){
      subTokens = returnTokens
    }

    if(argString.contains(matchWord)){
      println("After punctuation: " + subTokens.mkString(","))
    }

    returnTokens = removeTokensAfterConjunctionsOrPrepositions(subTokens) //Check this. Why isn't this filtering out which led to....
    if(!returnTokens.isEmpty){
      subTokens = returnTokens
    }

    if(argString.contains(matchWord)){
      println("After conjunctions or prepositions: " + subTokens.mkString(","))
    }


    def allowedToken(p:PostaggedToken) = !p.postag.startsWith("W") &&
      (p.isNoun || p.isAdjective || p.postag.equals("CD") ||
        p.postag.equals("DT") || p.string.equals("the") || p.string.equals("a") || p.string.equals("an"))

    val truncateIndex = subTokens.indexWhere(p => !allowedToken(p))
    returnTokens = subTokens.take(truncateIndex)

    if (truncateIndex >= 0 && returnTokens.find(p => p.isNoun).isDefined){
      subTokens = returnTokens
    }


    if(argString.contains(matchWord)){
      println("After truncate: " + subTokens.mkString(","))
    }


    /**
     * If NNPS* NNPS* -> return full sequence. (Saudi Arabia --> Saudi Arabia)
     * If NNPS* NN  -> return last NN (Saudi exile -> exile)
     * If NN NN -> return last NN (air plane --> air plane)
     *
     */

    return findLastNounAndNormalized(subTokens)

  }


  def findLastNounAndNormalized(subTokens: Seq[PostaggedToken]): Option[Seq[PostaggedToken]] = {

    assert(subTokens.size > 0, " Sub tokens cannot be zero.")
    //If only one token left return the stemmed version.
    if(subTokens.size < 2){
      return Some(subTokens)
    }
    //If no noun exists return the stemmed version of the entire string.
    if(subTokens.find(token => token.isNoun) == None){
      return None
    }

    def findLastPosSequence(tokens:Seq[PostaggedToken], posTester:PostaggedToken => Boolean):Option[Seq[PostaggedToken]] = {
      val nounSequence = tokens.zipWithIndex.filter(x => posTester(x._1)).reverse.toSeq
      if(nounSequence.isEmpty) return None
      var prevIndex = nounSequence.head._2
      val lastSeq = new ArrayBuffer[PostaggedToken]
      nounSequence.foreach(n => {
        val curIndex = n._2
        if ((prevIndex - curIndex) > 1){
          return Some(lastSeq.reverse)
        }
        prevIndex = curIndex
        lastSeq += n._1
      })
      return Some(lastSeq.reverse)
    }
    def isNoun(x:PostaggedToken) = x.isNoun
    def isProperNoun(x:PostaggedToken) = x.isProperNoun
    findLastPosSequence(subTokens, isNoun) match {
      case Some(lastNounSeq:Seq[PostaggedToken]) => {
        val lastNoun = lastNounSeq.last
        if (lastNoun.isCommonNoun){
          return Some(Seq(lastNoun))
        }else{
          return findLastPosSequence(lastNounSeq, isProperNoun)
        }
      }
      case _ => None
    }
  }



}


