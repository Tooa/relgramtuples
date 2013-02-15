package edu.washington.cs.knowitall.relgrams.typers

import java.io.{BufferedInputStream, FileInputStream, File}
import edu.washington.cs.knowitall.common.Resource._
import edu.washington.cs.knowitall.tool.typer.StanfordNer
import edu.stanford.nlp.ie.crf.CRFClassifier
import java.util.zip.GZIPInputStream
import edu.washington.cs.knowitall.tool.tokenize.Token

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
class NETyper(val modelFile:String) {
  import NETyper._
  val netyper = fromModelUrl(new File(modelFile))
  def assignTypesToSentence(sentenceTokens:Seq[Token]) = netyper(sentenceTokens)

}
