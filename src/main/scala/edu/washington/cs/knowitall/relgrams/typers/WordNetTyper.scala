package edu.washington.cs.knowitall.relgrams.typers

/**
 * Created with IntelliJ IDEA.
 * User: niranjan
 * Date: 2/14/13
 * Time: 1:42 PM
 * To change this template use File | Settings | File Templates.
 */


import edu.washington.cs.knowitall.relgrams.utils.JwiTools
import edu.washington.cs.knowitall.relgrams.utils.tool.WNDictionary

import scala.collection.JavaConversions._
import collection.mutable.ArrayBuffer
import edu.mit.jwi.item.{IIndexWord, Pointer, POS, ISynset}
import io.Source
import java.io.File
import collection.mutable
import edu.washington.cs.knowitall.tool.postag.PostaggedToken
import edu.mit.jwi.morph.WordnetStemmer
import edu.washington.cs.knowitall.tool.typer.Type
import org.slf4j.LoggerFactory

import edu.washington.cs.knowitall.collection.immutable.Interval


/**
 * Created with IntelliJ IDEA.
 * User: niranjan
 * Date: 8/29/12
 * Time: 10:31 AM
 * To change this template use File | Settings | File Templates.
 */



object  NoType extends Type("NA", "NA", Interval.open(0,0), "NA")

object WordNetTyper{

  val DEFAULT_TAG = "PRP"

  var WordNet: String = "WordNet"

  def isWordnetType(t:Type):Boolean = t.source.contains(WordNet)
  def filterTypes(types: Seq[Type]): Seq[Type] = {
    types.filterNot(f => f.name.equals("NA"))
  }

  def filterUnmappedTypes(types:(Seq[Type], Seq[Type])):(Seq[Type], Seq[Type]) = {
    (filterTypes(types._1), filterTypes(types._2))
  }


  def getPosTagsForArg(arg:String, sentTokens:Seq[String], sentPosTags:Seq[String]):Seq[String] = {
    val postags = new ArrayBuffer[String]
    val argTokens = arg.split(" ")
    argTokens.foreach(token => {
      val filteredToken = token.replaceAll("""[^a-zA-Z0-9 ]""", " ")
      var index = sentTokens.indexOf(token)
      if (index < 0){
        index = sentTokens.indexOf(token.trim)
      }
      if(index < 0){
        index = sentTokens.indexOf(filteredToken)
      }
      val postag = if (index >= 0){
        sentPosTags(index)
      }else{
        if (token.equals("of"))
          DEFAULT_TAG
        else if (token.equals("be"))
          "VB"
        else
          "NN"
      }
      postags += postag
    })
    return postags
  }


  def main(args:Array[String]){
    val wordnetDictPath = args(0)//"/Users/niranjan/work/local/wordnet3.0"
    val typesFile = "src/main/resources/wordnet-classes-large.txt"
    val senses = 1::2::3::Nil
    val retainUnmappedTypes = true
    val filterTypes = true
    val wnTyper = new WordNetTyper(wordnetDictPath, typesFile, senses,  filterTypes, retainUnmappedTypes)



    var ptoken = new PostaggedToken("NN", "number", 0)
    var isGroup = wnTyper.isGroupQuantityAmountNumberOrPart(ptoken)//, 0::1::2::Nil, 3, false)
    println("Is Group: " + isGroup)
    exit



    var ptokens = new PostaggedToken("NN", "plan", 0)::Nil
    var wtypes = wnTyper.assignTypes("plan", ptokens)
    //var wtypes = wnTyper.getWordnetTypes(ptokens, 1::2::3::Nil, 3, false)
    println("Wordnet Types for plan: \n" + wtypes.mkString("\n"))

    ptokens = new PostaggedToken("NN", "antenna", 0)::Nil
    wtypes = wnTyper.assignTypes("antenna", ptokens)
    //wtypes = wnTyper.getWordnetTypes(ptokens, 1::2::3::Nil, 3, false)
    println("Wordnet Types for ambassador: \n" + wtypes.mkString("\n"))



    ptoken = new PostaggedToken("NN", "Bank", 0)
    isGroup = wnTyper.isGroupQuantityAmountNumberOrPart(ptoken)//, 0::1::2::Nil, 3, false)
    println("Is Group: " + isGroup)

    ptoken = new PostaggedToken("NN", "percentage", 0)
    isGroup = wnTyper.isGroupQuantityAmountNumberOrPart(ptoken)//, 0::1::2::Nil, 3, false)
    println("Is Group: " + isGroup)



    ptokens = new PostaggedToken("NN", "Bank", 0)::Nil
    wtypes = wnTyper.getWordnetTypes(ptokens, 0::1::2::Nil, false)
    println("Wordnet Types for Bank: \n" + wtypes.mkString("\n"))

    ptokens = new PostaggedToken("NN", "bomb", 0)::Nil
    wtypes = wnTyper.getWordnetTypes(ptokens, 0::1::2::Nil, false)

    println("Wordnet Types for bomb: \n" + wtypes.mkString("\n"))

    ptokens = new PostaggedToken("NN", "thing", 0)::Nil
    wtypes = wnTyper.getWordnetTypes(ptokens, 0::1::2::Nil, false)

    println("Wordnet Types for thing: \n" + wtypes.mkString("\n"))


    println
    println
    println
    ptokens = new PostaggedToken("DT", "A", 0)::new PostaggedToken("JJ", "collection", 0)::new PostaggedToken("IN", "of", 0)::new PostaggedToken("NNS", "flowers", 0)::Nil
    wtypes = wnTyper.getWordnetTypes(ptokens, 0::1::2::Nil, false)


    println("Wordnet Types for 'A collection of flowers': \n" + wtypes.mkString("\n"))

    ptokens = new PostaggedToken("CD", "Twenty", 0)::new PostaggedToken("NNS", "flowers", 0)::Nil
    wtypes = wnTyper.getWordnetTypes(ptokens, 0::1::2::Nil, false)

    println
    println
    println

    println("Wordnet Types for 'Twenty flowers': \n" + wtypes.mkString("\n"))



    ptokens = new PostaggedToken("NN", "Department", 0)::new PostaggedToken("IN", "of", 0)::new PostaggedToken("NN", "computer", 0)::new PostaggedToken("NN", "science", 0)::Nil

    wtypes = wnTyper.getWordnetTypes(ptokens, 0::1::2::3::Nil, false)

    println
    println
    println

    println("Wordnet Types for 'Department of computer science': \n" + wtypes.mkString("\n"))

    ptokens = new PostaggedToken("CD", "Twenty", 0)::new PostaggedToken("'s", "POS", 0)::new PostaggedToken("NN", "drama", 0)::Nil
    wtypes = wnTyper.getWordnetTypes(ptokens, 0::1::2::Nil, false)

    println
    println
    println

    println("Wordnet Types for 'Twenty's drama': \n" + wtypes.mkString("\n"))


    ptokens = new PostaggedToken("DT", "the", 0)::new PostaggedToken("town", "NN", 0)::new PostaggedToken("'s", "POS", 0)::new PostaggedToken("JJ", "rich", 0)::new PostaggedToken("NN", "farmland", 0)::Nil
    wtypes = wnTyper.getWordnetTypes(ptokens, 0::1::2::Nil, false)

    println
    println
    println

    println("Wordnet Types for 'the town's rich farmland': \n" + wtypes.mkString("\n"))


    ptokens = new PostaggedToken("NNP", "Stanford", 0)::new PostaggedToken("NNP", "University", 0)::Nil
    wtypes = wnTyper.getWordnetTypes(ptokens, 0::1::2::Nil, false)

    println
    println
    println

    println("Wordnet Types for 'Stanford University': \n" + wtypes.mkString("\n"))

    ptokens = new PostaggedToken("NNP", "Random", 0)::new PostaggedToken("NNP", "University", 0)::Nil
    wtypes = wnTyper.getWordnetTypes(ptokens, 0::1::2::Nil, false)

    println
    println
    println

    println("Wordnet Types for 'Random University': \n" + wtypes.mkString("\n"))

    ptokens = new PostaggedToken("JJ", "small", 0)::new PostaggedToken("NN", "portions", 0)::Nil
    wtypes = wnTyper.getWordnetTypes(ptokens, 0::1::2::Nil, false)

    println
    println
    println

    println("Wordnet Types for 'small portions': \n" + wtypes.mkString("\n"))


    ptokens = new PostaggedToken("NNP", "Devils", 0)::new PostaggedToken("NN", "coach", 0)::new PostaggedToken("NNP", "Barack", 0)::new PostaggedToken("NNP", "Obama", 0)::Nil
    wtypes = wnTyper.getWordnetTypes(ptokens, 0::1::2::Nil, false)

    println
    println
    println

    println("Wordnet Types for 'Devils coach John Peter': \n" + wtypes.mkString("\n"))



    ptokens = new PostaggedToken("NNP", "Devils", 0)::new PostaggedToken("NN", "coach", 0)::new PostaggedToken("NNP", "Barack", 0)::new PostaggedToken("NNP", "Obama", 0)::Nil
    wtypes = wnTyper.getWordnetTypes(ptokens, 0::1::2::Nil, false)

    println
    println
    println

    println("Wordnet Types for 'Devils coach John Peter': \n" + wtypes.mkString("\n"))

    ptokens = new PostaggedToken("NNP", "Devils", 0)::new PostaggedToken("NN", "coach", 0)::new PostaggedToken("NNP", "Barack", 0)::new PostaggedToken("NNP", "Obama", 0)::Nil
    wtypes = wnTyper.getWordnetTypes(ptokens, 0::1::2::Nil, false)

    println
    println
    println

    println("Wordnet Types for 'Devils coach John Peter': \n" + wtypes.mkString("\n"))

    ptokens = new PostaggedToken("NNP", "Devils", 0)::new PostaggedToken("NN", "coach", 0)::new PostaggedToken("NNP", "Barack", 0)::new PostaggedToken("NNP", "Obama", 0)::Nil
    wtypes = wnTyper.getWordnetTypes(ptokens, 0::1::2::Nil, false)

    println
    println
    println

    println("Wordnet Types for 'Devils coach John Peter': \n" + wtypes.mkString("\n"))

    ptokens = new PostaggedToken("NNP", "Devils", 0)::new PostaggedToken("NN", "coach", 0)::new PostaggedToken("NNP", "Barack", 0)::new PostaggedToken("NNP", "Obama", 0)::Nil
    wtypes = wnTyper.getWordnetTypes(ptokens, 0::1::2::Nil, false)


    ptokens = new PostaggedToken("NNP", "Devils", 0)::new PostaggedToken("NN", "coach", 0)::new PostaggedToken("NNP", "Barack", 0)::new PostaggedToken("NNP", "Obama", 0)::Nil
    wtypes = wnTyper.getWordnetTypes(ptokens, 0::1::2::Nil, false)



  }


}
class WordNetTyper {

  val logger = LoggerFactory.getLogger(this.getClass)

  var jwiTools:JwiTools = null

  var dict:edu.mit.jwi.Dictionary = null
  var stemmer:WordnetStemmer = null
  //var types:HashMap[String, Type] = null

  var types:collection.immutable.HashSet[ISynset] = null

  var typeSynsetIds:collection.immutable.HashSet[String] = null

  var synsetClasses:collection.immutable.HashMap[ISynset, String] = null

  var filterTypes:Boolean = true

  var argsList = new ArrayBuffer[String]()
  var typesCache = new mutable.HashMap[String, Seq[Type]]()

  var wordnetBlacklist:Set[String] = null

  var MAX_CACHE_SIZE = 10000

  var retainUnmappedTypes = false
  var senses = Seq(1,2,3)

  def this(wnhome:String,
           typesFile:String,
           senses:Seq[Int],
           filterTypes:Boolean,
           retainUnmappedTypes:Boolean) = {
    this()
    setup(wnhome, typesFile, senses, filterTypes, retainUnmappedTypes)
  }

  val Xclasses = "number:1,group:0,quantity:0,part:0,amount:0,percentage:0,proportion:3"
  val XclassNames = "number,group,quantity,part,amount,percentage,proportion".split(",").toSet
  def setup(wnhome:String, typesFile: String, senses:Seq[Int],  filterTypes:Boolean, retainUnmappedTypes:Boolean) {
    setWNResources(wnhome)
    loadTypes(typesFile)
    setupBlackList
    if(!new File(typesFile).exists()){
      Xclasses.split(",").foreach(x => {
        val splits = x.split(":")
        addXClasses(splits(0),splits(1).toInt)
      })
    }
    println("XClasses size: " + types.size)
    this.senses = senses
    this.filterTypes = filterTypes
    this.retainUnmappedTypes = retainUnmappedTypes
  }

  def addXClasses(word:String, senseId:Int){
    val stemmedWord = stem(word, 0)
    val indexedWord = dict.getIndexWord(stemmedWord, POS.NOUN)
    val wordIDs = indexedWord.getWordIDs
    val dictWord = wordIDs.get(senseId)
    val synset = dict.getWord(dictWord).getSynset
    types += synset
    typeSynsetIds += synset.getID.toString
    synsetClasses += synset -> stemmedWord


  }

  def setWNResources(wnhome: String) {
    jwiTools = new JwiTools(wnhome)
    if (wnhome != "") {
      dict = WNDictionary.fetchDictionary(wnhome)
    } else {
      dict = WNDictionary.fetchDictionary()
    }
    stemmer = new WordnetStemmer(dict)
  }
  def setupBlackList {
    wordnetBlacklist = ("it"::"he"::"she":: "they"::Nil).toSet
  }

  def loadTypes(typesFile: String){
    types = new collection.immutable.HashSet[ISynset]
    typeSynsetIds  = new collection.immutable.HashSet[String]
    synsetClasses = new collection.immutable.HashMap[ISynset, String]
    if(new File(typesFile).exists()){
      Source.fromFile(typesFile).getLines.filter(line => !line.startsWith("#")).foreach(line => {
        addToSynsetClasses(line)
      })
    }else{
      logger.error("Type file is not a valid file: " + typesFile)
    }
    logger.info("Number of wordnet types: " + types.size)
  }


  def addToSynsetClasses(line: String) {
    val stemmedWord = stem(line, 0)
    try{
      val indexedWord = dict.getIndexWord(stemmedWord, POS.NOUN)
      if(indexedWord != null){
        val wordIDs = indexedWord.getWordIDs.take(1)
        wordIDs.foreach(wordID => {
          val synset = dict.getWord(wordID).getSynset
          types += synset
          typeSynsetIds += synset.getID.toString
          synsetClasses += synset -> stemmedWord
        })
      }else{
        logger.error("Failed to find index word for class: " + line)
      }
    }catch{
      case e:Exception => {
        logger.error("Caught exception finding synset classes for line: " + line)
        logger.error(e.getStackTraceString)
      }

    }
  }

  def getNonNNPNouns(tokens: Seq[PostaggedToken]):Seq[PostaggedToken] = tokens.filter(p => p.isCommonNoun)

  def hypernymStream(tokens:Seq[PostaggedToken], senses:Iterable[Int]):(String, Seq[Set[ISynset]]) = findIndexedWord(tokens) match {
    case (matchString: String, Some(entry:IIndexWord)) => {
      val wordIDs = entry.getWordIDs.take(senses.max)
      var synsets = wordIDs.map( wordID => dict.getWord(wordID).getSynset).toSet
      (matchString, synsets :: hypernymStream(synsets))
    }
    case (x:String, None) => {
      val nonNNPNouns = getNonNNPNouns(tokens)
      if(tokens.find(t => t.isProperNoun) != None && nonNNPNouns.isEmpty == false){
        findIndexedWord(nonNNPNouns) match {
          case (ms:String, Some(nentry:IIndexWord)) =>{
            val wordIDs = nentry.getWordIDs.take(senses.max)
            var synsets = wordIDs.map( wordID => dict.getWord(wordID).getSynset).toSet
            (ms, synsets :: hypernymStream(synsets))
          }
          case (x:String, None) => (x, List[Set[ISynset]]())
        }
      }else{
        (x, List[Set[ISynset]]())
      }
    }
  }


  def hypernymStream(synsets:Iterable[ISynset]):List[Set[ISynset]] = {
    val nextLevel = synsets.flatMap(synset => {
      hypernyms(synset)
    }).toSet
    if(nextLevel.isEmpty == false){
      return nextLevel :: hypernymStream(nextLevel)
    }
    return List[Set[ISynset]]()
  }

  def hypernyms(synset:ISynset):Set[ISynset] = {
    var hypernymIDs = synset.getRelatedSynsets(Pointer.HYPERNYM).toSet
    if(hypernymIDs.isEmpty){
      hypernymIDs = synset.getRelatedSynsets(Pointer.HYPERNYM_INSTANCE).toSet
    }
    // map from jwi synsets to synsets
    hypernymIDs.map(sid => dict.getSynset(sid))
  }

  def findIndexedWord(tokens:Seq[PostaggedToken]):(String, Option[IIndexWord]) = {
    if(!tokens.isEmpty){
      val word = dict.getIndexWord(tokens.mkString(" "), POS.NOUN)
      if(word != null){
        return (tokens.map(token => token.string).mkString(" "), Some(word))
      }else{
        return findNoun(tokens)
      }
    }
    return ("", None)
  }


  def findNoun(toSearch: Seq[PostaggedToken]): (String, Option[IIndexWord]) = {
    if (toSearch == Nil) return ("", None)
    // search for word in WordNet
    val strings = toSearch.map(token => token.string)
    val stemmedWord = stem(strings.mkString(" "), 0)

    if (stemmedWord == "") {
      // was not found by stemmer. if toSearch is a single noun n followed
      // by "of" and more tokens, search WordNet for n; else search WordNet
      // for toSearch's tail.
      if (toSearch.length > 1 && toSearch(1).postag == "IN") {
        findNoun(List(toSearch(0)))
      } else {
        findNoun(toSearch.tail)
      }
    } else {
      // look for stemmedWord in WordNet
      val idxWord = dict.getIndexWord(stemmedWord, POS.NOUN)

      if (idxWord != null && wordnetBlacklist.contains(stemmedWord) == false) {
        return (stemmedWord, Some(idxWord))
      }else {
        // word not found in WordNet. do the same check for "of"
        // described above
        if (toSearch.length > 1 && toSearch(1).postag == "IN") {
          findNoun(List(toSearch(0)))
        } else {
          findNoun(toSearch.tail)
        }
      }
    }
  }



  /** Find a word's corresponding WordNet stem.
    *
    * @param word the word to find the stem of.
    * @param n which stem to return - set this to 0, rarely will you want others.
    * @return If word's nth WordNet stem exists, returns it; else empty string.
    */
  def stem(word: String, n: Int): String = {

    val stems = try {
      stemmer.findStems(word.replaceAll("""[^a-zA-Z0-9]""", " "), POS.NOUN).toSeq
    }catch{
      case e:Exception => {
        Seq[String]()
      }
      case _ => {
        Seq[String]()
      }


    }
    if (stems.size > n) stems(n) else ""

  }



  def isGroupQuantityAmountNumberOrPart(token: PostaggedToken): Boolean = {
    if(XclassNames.contains(token.string)) return true
    val (matchString:String, hypernyms:Seq[Set[ISynset]]) = hypernymStream(token::Nil, senses)
    hypernyms.iterator.filter(st => st.size > 0).foreach(st => {
      st.foreach(s => if (typeSynsetIds.contains(s.getID.toString)) {
        return true
      })
    })
    return false
  }



  def getWordnetTypes(tokens:Seq[PostaggedToken]):Iterable[Type] =  getWordnetTypes(tokens, senses, retainUnmappedTypes)

  /**
   *
   * @param tokens
   * @param senses
   * @return
   */
  def getWordnetTypes(tokens:Seq[PostaggedToken], senses:Iterable[Int], retainUnmappedTypes:Boolean):Iterable[Type] = {
    val (matchString:String, hypernyms:Seq[Set[ISynset]]) = hypernymStream(tokens, senses)
    val span = TypersUtil.span(tokens)
    findMatchingTypes(matchString, hypernyms, retainUnmappedTypes, span)
  }


  def findMatchingTypes(matchingString:String, stream:Seq[Set[ISynset]], retainUnmappedTypes:Boolean, interval:Interval):Iterable[Type] = {
    val outTypes = new collection.mutable.HashSet[Type]
    stream.iterator.filter(st => st.size > 0).foreach(st => {
      var i = 1
      val matchingTypes = if(filterTypes) st.intersect(types) else st
      matchingTypes.foreach( synset => {
        val className = synsetClasses.getOrElse(synset, "NA")
        if(!className.equals("NA")){
          outTypes += new Type(className, WordNetTyper.WordNet, interval, matchingString)
        }
      })
      i = i + 1
    })
    outTypes
  }
  def assignTypes(text:String, tokens:Seq[PostaggedToken]) = {//er: OllieExtractionInstance, retainUnmappedTypes:Boolean): (scala.Seq[Type], scala.Seq[Type]) = {
    val types = typesCache.getOrElse(text, getWordnetTypes(tokens))
    trimCacheIfNecessary()
    typesCache += text -> types.toSeq
    if(argsList.contains(text) == false){
      argsList += text
    }
    types
  }


  def trimCacheIfNecessary() {
    if (typesCache.keys.size > MAX_CACHE_SIZE) {
      val numToRemove = (argsList.size - MAX_CACHE_SIZE)
      val keysToRemove = argsList.take(numToRemove)
      argsList = argsList.drop(numToRemove)
      keysToRemove.foreach(key => typesCache.remove(key))
    }

  }
}

