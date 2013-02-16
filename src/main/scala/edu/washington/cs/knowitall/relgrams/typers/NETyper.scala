package edu.washington.cs.knowitall.relgrams.typers

import java.io.{BufferedInputStream, FileInputStream, File}
import edu.washington.cs.knowitall.common.Resource._
import edu.washington.cs.knowitall.tool.typer.{Type, StanfordNer}
import edu.stanford.nlp.ie.crf.CRFClassifier
import java.util.zip.GZIPInputStream
import edu.washington.cs.knowitall.tool.tokenize.Token
import edu.washington.cs.knowitall.tool.postag.PostaggedToken

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
class PronounTyper{
  def assignTypes(argTokens:Seq[PostaggedToken]) = {
    if (argTokens.size == 1 && argTokens(0).isPronoun){
      new Type("person:Pronoun", "PRN", argTokens(0).interval, argTokens(0).string)::Nil
    }else{
      Iterable[Type]()
    }


  }

}
class NETyper(val modelFile:String) {
  import NETyper._
  val netyper = fromModelUrl(new File(modelFile))
  def assignTypesToSentence(sentenceTokens:Seq[Token]) = netyper(sentenceTokens)

}
