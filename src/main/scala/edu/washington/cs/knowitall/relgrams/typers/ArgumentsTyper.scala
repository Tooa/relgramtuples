package edu.washington.cs.knowitall.relgrams.typers

import edu.washington.cs.knowitall.ollie.OllieExtractionInstance
import edu.washington.cs.knowitall.tool.tokenize.Token
import edu.washington.cs.knowitall.tool.postag.PostaggedToken
import edu.washington.cs.knowitall.tool.typer.Type
import org.slf4j.LoggerFactory
import edu.washington.cs.knowitall.openparse.extract.Extraction.Part
import collection.mutable.{HashSet, ArrayBuffer}
import collection.mutable


/**
 * Created with IntelliJ IDEA.
 * User: niranjan
 * Date: 2/14/13
 * Time: 1:30 PM
 * To change this template use File | Settings | File Templates.
 */

case class TypedExtractionInstance(extractionInstance:OllieExtractionInstance,
                                   arg1Head:Seq[Token], relHead:Seq[Token], arg2Head:Seq[Token],
                                   arg1Types:Iterable[Type], arg2Types:Iterable[Type]) {


  override def toString:String = "%s\t%s\t%s\t%s\t%s\t%s".format(extractionInstance.toString, arg1Head.mkString(" "), relHead.mkString(" "), arg2Head.mkString(" "), arg1Types.mkString(" "), arg2Types.mkString(" "))
}

class ArgumentsTyper(val ne7ModelFile:String, val ne3ModelFile:String, val wordnetLocation:String, val wordnetTypesFile:String, val wnSenses:Int) {

  val logger = LoggerFactory.getLogger(this.getClass)


  val extractor = new HeadExtractor(wordnetLocation)
  //HeadExtractor.setWnHome(wordnetLocation)
  val wnTyper = new WordNetTyper(wordnetLocation, wordnetTypesFile, (1 until wnSenses+1), true, false)
  val prnTyper = new PronounTyper
  val personTyper = new PersonTyper
  val neTyper = new NETyper(ne7ModelFile, ne3ModelFile)

  def getNETypes(sentenceTokens:Seq[Token], dontAdjustOffsets:Boolean) = neTyper.assignTypes(sentenceTokens)//if(!dontAdjustOffsets) neTyper.assignTypes(sentenceTokens) else neTyper.assignTypesToSentenceNoAdjustments(sentenceTokens)

  val conjunctions = new HashSet[String]
 // conjunctions += "for"
  conjunctions += "and"
  conjunctions += "nor"
  conjunctions += "but"
  conjunctions += "or"
  conjunctions += "yet"
  conjunctions += "so"

  val beVerbs = new HashSet[String]
  beVerbs += "be"
  beVerbs += "is"
  beVerbs += "are"
  beVerbs +="was"
  beVerbs += "were"

  def assignTag(token: String): String = {
    if (beVerbs.contains(token)) {
      "VB"
    }else if (conjunctions.contains(token)){
      "CC"
    }else {
      "PP"
    }


  }
  def addMissingTokens(field:Part) = {
    val text = field.text
    val nodes = field.nodes.map(node => node.text -> node).toMap
    def makeToken(word:String) = {
      nodes.getOrElse(word, new PostaggedToken(assignTag(word), word, 0))
    }
    text.split(" ").map(word => makeToken(word))
  }
  def assignTypes(neTypes:Seq[Type])(extractionInstance:OllieExtractionInstance):Option[TypedExtractionInstance] = {
    val arg1Tokens = extractionInstance.extr.arg1.nodes.toSeq
    val arg2Tokens = extractionInstance.extr.arg2.nodes.toSeq
    val relTokens = addMissingTokens(extractionInstance.extr.rel)
    val arg1HeadTokensOption = extractor.argumentHead(arg1Tokens)
    val arg2HeadTokensOption = extractor.argumentHead(arg2Tokens)
    val relHeadTokensOption = extractor.relHead(relTokens)

    (arg1HeadTokensOption, relHeadTokensOption, arg2HeadTokensOption) match {
      case (Some(arg1HeadTokens:Seq[PostaggedToken]),
            Some(relHeadTokens:Seq[PostaggedToken]),
            Some(arg2HeadTokens:Seq[PostaggedToken])) => {
        val arg1Types = assignTypes(arg1HeadTokens, neTypes)
        val arg2Types = assignTypes(arg2HeadTokens, neTypes)
        Some(new TypedExtractionInstance(extractionInstance,
          extractor.lemmatize(arg1HeadTokens),
          extractor.lemmatize(relHeadTokens),
          extractor.lemmatize(arg2HeadTokens),
          arg1Types, arg2Types))
      }
      case _ => {
        None
      }
    }
  }


  def isDate(typ:Type) = typ.name.toLowerCase.contains("date")
  def isMoney(typ:Type) = typ.name.toLowerCase.contains("money")
  def isPercent(typ:Type) = typ.name.toLowerCase.contains("percent")
  def isDateOrMoneyOrPercent(typ:Type) = isDate(typ) || isMoney(typ) || isPercent(typ)

  private def assignTypes(argHeadTokens: Seq[PostaggedToken], neTypes: Seq[Type]): Iterable[Type] = {

    //Get wordnet types
    val wordNetHead = argHeadTokens.filter(p => (!p.isProperNoun && !p.isPronoun))
    val wordNetHeadText = wordNetHead.map(x => x.string).mkString(" ")
    val wnArgTypes = if(!wordNetHead.isEmpty) wnTyper.assignTypes(wordNetHeadText, wordNetHead) else Iterable[Type]()

    //Named entity types
    val neArgTypes = assignNETypes(neTypes, argHeadTokens)

    //Pronoun types
    val prnTypes = prnTyper.assignTypes(argHeadTokens)

    val pesonTypes = personTyper.assignTypes(argHeadTokens)

    //If no date or money or percent in arg then use number regexes.
    val numberTypes = if (neArgTypes.find(typ => isDateOrMoneyOrPercent(typ)) == None){
      NumberFinder.findNumbers(argHeadTokens)
    }else{
      Iterable[Type]()
    }

    val dayTypes = DayTyper.assignTypes(argHeadTokens)
    (wnArgTypes ++ neArgTypes ++ prnTypes ++ pesonTypes ++ numberTypes ++ dayTypes).toSet
  }

  private def assignNETypes(types:Seq[Type], tokens:Seq[Token]):Iterable[Type] = {
    val span = TypersUtil.span(tokens)
    /**
     * Tests whether typ interval is a subset of the interval span of the tokens.
     * or if the span interval is a subset of the typ interval.
     */
    val argString = tokens.map(x => x.string).mkString(" ").toLowerCase
    var somematch = false
    val filteredTypes = types.filter(typ => {
      val out = span.subset(typ.interval) || argString.contains(typ.text.toLowerCase)
      somematch |= out
      out
    })
    filteredTypes
  }
}
