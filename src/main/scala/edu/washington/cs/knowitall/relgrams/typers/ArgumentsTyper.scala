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

case class TypedExtractionInstance(extractionInstance:OllieExtractionInstance, arg1Types:Iterable[Type], arg2Types:Iterable[Type])

class ArgumentsTyper(val neModelFile:String, val wordnetLocation:String) {

  val neTyper = new NETyper(neModelFile)
  val wnTyper = new WordNetTyper(wordnetLocation, "", 0::1::2::Nil, 3, true, true)

  def assignTypes(extractionInstance:OllieExtractionInstance):Option[TypedExtractionInstance] = {
    val tokens = extractionInstance.sentence.nodes.toSeq
    val neTypes = neTyper.assignTypesToSentence(tokens)
    val arg1Tokens = extractionInstance.extr.arg1.nodes.toSeq
    val arg2Tokens = extractionInstance.extr.arg1.nodes.toSeq
    val arg1HeadTokensOption = HeadExtractor.argumentHead(arg1Tokens)
    val arg2HeadTokensOption = HeadExtractor.argumentHead(arg2Tokens)
    (arg1HeadTokensOption, arg2HeadTokensOption) match {
      case (Some(arg1HeadTokens:Seq[PostaggedToken]), Some(arg2HeadTokens:Seq[PostaggedToken])) => {
        val arg1Types = assignTypes(arg1HeadTokens, neTypes)
        val arg2Types = assignTypes(arg2HeadTokens, neTypes)
        Some(new TypedExtractionInstance(extractionInstance, arg1Types, arg2Types))
      }
      case _ => {
        None
      }
    }
  }


  private def assignTypes(argHeadTokens: Seq[PostaggedToken], neTypes: List[Type]): Iterable[Type] = {
    val argHeadText = argHeadTokens.map(token => token.string).mkString(" ")
    val wnArgTypes = wnTyper.assignTypes(argHeadText, argHeadTokens)
    val neArgTypes = assignTypes(neTypes, argHeadTokens)
    wnArgTypes ++ neArgTypes
  }

  private def assignTypes(types:Seq[Type], tokens:Seq[Token]):Iterable[Type] = {
    val span = TypersUtil.span(tokens)
    types.filter(typ => typ.interval.subset(span))
  }



}
