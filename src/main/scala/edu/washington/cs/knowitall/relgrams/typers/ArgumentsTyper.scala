package edu.washington.cs.knowitall.relgrams.typers

import edu.washington.cs.knowitall.ollie.OllieExtractionInstance
import edu.washington.cs.knowitall.tool.tokenize.Token
import edu.washington.cs.knowitall.tool.postag.PostaggedToken
import edu.washington.cs.knowitall.tool.typer.Type


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

  val neTyper = new NETyper(neModelFile)
  HeadExtractor.setWnHome(wordnetLocation)
  val wnTyper = new WordNetTyper(wordnetLocation, wordnetTypesFile, (1 until wnSenses+1), 3, true, false)
  val prnTyper = new PronounTyper
  def assignTypes(extractionInstance:OllieExtractionInstance):Option[TypedExtractionInstance] = {
    val tokens = extractionInstance.sentence.nodes.toSeq
    val neTypes = neTyper.assignTypesToSentence(tokens)
    //neTypes.foreach(net => println("NEType: " + net))
    val arg1Tokens = extractionInstance.extr.arg1.nodes.toSeq
    val arg2Tokens = extractionInstance.extr.arg2.nodes.toSeq
    val relTokens = extractionInstance.extr.rel.nodes.toSeq
    val arg1HeadTokensOption = HeadExtractor.argumentHead(arg1Tokens)
    val arg2HeadTokensOption = HeadExtractor.argumentHead(arg2Tokens)
    var relHeadTokens = HeadExtractor.relHead(relTokens)
    relHeadTokens = if (relHeadTokens.isEmpty) relTokens else relHeadTokens
    (arg1HeadTokensOption,  arg2HeadTokensOption) match {
      case (Some(arg1HeadTokens:Seq[PostaggedToken]), Some(arg2HeadTokens:Seq[PostaggedToken])) => {
        val arg1Types = assignTypes(arg1HeadTokens, neTypes)
        val arg2Types = assignTypes(arg2HeadTokens, neTypes)

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


  private def assignTypes(argHeadTokens: Seq[PostaggedToken], neTypes: List[Type]): Iterable[Type] = {

    //Get wordnet types
    val wordNetHead = argHeadTokens.filter(p => p.isCommonNoun)
    val wordNetHeadText = wordNetHead.map(x => x.string).mkString(" ")
    val wnArgTypes = if(!wordNetHead.isEmpty) wnTyper.assignTypes(wordNetHeadText, wordNetHead) else Iterable[Type]()

    //Named entity types
    val neArgTypes = assignTypes(neTypes, argHeadTokens)

    //Pronoun types
    val prnTypes = prnTyper.assignTypes(argHeadTokens)

    wnArgTypes ++ neArgTypes ++ prnTypes
  }

  private def assignTypes(types:Seq[Type], tokens:Seq[Token]):Iterable[Type] = {
    val span = TypersUtil.span(tokens)
    types.filter(typ => typ.interval.subset(span))
  }



}
