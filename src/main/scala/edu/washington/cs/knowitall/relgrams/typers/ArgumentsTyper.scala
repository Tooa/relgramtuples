package edu.washington.cs.knowitall.relgrams.typers

import edu.washington.cs.knowitall.ollie.OllieExtractionInstance
import edu.washington.cs.knowitall.tool.tokenize.Token
import edu.washington.cs.knowitall.tool.postag.PostaggedToken
import edu.washington.cs.knowitall.tool.typer.Type
import org.slf4j.LoggerFactory


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

class ArgumentsTyper(val neModelFile:String, val wordnetLocation:String, val wordnetTypesFile:String, val wnSenses:Int) {

  val logger = LoggerFactory.getLogger(this.getClass)

  HeadExtractor.setWnHome(wordnetLocation)
  val wnTyper = new WordNetTyper(wordnetLocation, wordnetTypesFile, (1 until wnSenses+1), true, false)
  val prnTyper = new PronounTyper
  val neTyper = new NETyper(neModelFile)

  def getNETypes(sentenceTokens:Seq[Token]) = neTyper.assignTypesToSentence(sentenceTokens)

  def assignTypes(neTypes:Seq[Type])(extractionInstance:OllieExtractionInstance):Option[TypedExtractionInstance] = {
    val arg1Tokens = extractionInstance.extr.arg1.nodes.toSeq
    val arg2Tokens = extractionInstance.extr.arg2.nodes.toSeq
    val relTokens = extractionInstance.extr.rel.nodes.toSeq
    val arg1HeadTokensOption = HeadExtractor.argumentHead(arg1Tokens)
    val arg2HeadTokensOption = HeadExtractor.argumentHead(arg2Tokens)
    var relHeadTokens = HeadExtractor.relHead(relTokens)
    relHeadTokens = if (relHeadTokens.isEmpty) relTokens else relHeadTokens
    (arg1HeadTokensOption,  arg2HeadTokensOption) match {
      case (Some(arg1HeadTokens:Seq[PostaggedToken]), Some(arg2HeadTokens:Seq[PostaggedToken])) => {
        val arg1Types = assignNETypes(arg1HeadTokens, neTypes)
        val arg2Types = assignNETypes(arg2HeadTokens, neTypes)

        Some(new TypedExtractionInstance(extractionInstance,
          HeadExtractor.lemmatize(arg1HeadTokens),
          HeadExtractor.lemmatize(relHeadTokens),
          HeadExtractor.lemmatize(arg2HeadTokens),
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

  private def assignNETypes(argHeadTokens: Seq[PostaggedToken], neTypes: Seq[Type]): Iterable[Type] = {

    //Get wordnet types
    val wordNetHead = argHeadTokens.filter(p => !p.isProperNoun)// || p.postag.equals("CD"))
    val wordNetHeadText = wordNetHead.map(x => x.string).mkString(" ")
    val wnArgTypes = if(!wordNetHead.isEmpty) wnTyper.assignTypes(wordNetHeadText, wordNetHead) else Iterable[Type]()

    //Named entity types
    val neArgTypes = assignTypes(neTypes, argHeadTokens)

    //Pronoun types
    val prnTypes = prnTyper.assignTypes(argHeadTokens)

    //If no date or money or percent in arg then use number regexes.
    val numberTypes = if (neArgTypes.find(typ => isDateOrMoneyOrPercent(typ)) == None){
      NumberFinder.findNumbers(argHeadTokens)
    }else{
      Iterable[Type]()
    }
    wnArgTypes ++ neArgTypes ++ prnTypes ++ numberTypes
  }

  private def assignTypes(types:Seq[Type], tokens:Seq[Token]):Iterable[Type] = {
    val span = TypersUtil.span(tokens)
    /**
     * Tests whether typ interval is a subset of the interval span of the tokens.
     * or if the span interval is a subset of the typ interval.
     */
    types.filter(typ => span.subset(typ.interval))
  }



}
