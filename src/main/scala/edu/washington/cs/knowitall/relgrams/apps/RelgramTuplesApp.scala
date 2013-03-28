package edu.washington.cs.knowitall.relgrams.apps

import scala.collection.JavaConversions._
import scopt.mutable.OptionParser
import edu.washington.cs.knowitall.relgrams.typers.{TypersUtil, TypedExtractionInstance, ArgumentsTyper}
import edu.washington.cs.knowitall.relgrams.extractors.{RelgramTuplesExtractor, RelgramTuple, OllieExtractionOrdering, Extractor}
import io.Source
import org.slf4j.LoggerFactory
import java.io.PrintWriter
import edu.washington.cs.knowitall.tool.typer.Type

/**
 * Created with IntelliJ IDEA.
 * User: niranjan
 * Date: 2/14/13
 * Time: 4:03 PM
 * To change this template use File | Settings | File Templates.
 */


object RelgramTuplesApp{

  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args:Array[String]) {

    var inputPath, outputPath = ""
    var wnHome = "/home/niranjan/local/Wordnet3.0"
    var wnTypesFile = "wordnet-classes-large.txt"

    var ne7ModelFile = "/Users/niranjan/work/projects/git/scala/argtyping/src/main/resources/english.muc.7class.nodistsim.prop"
    var ne3ModelFile = "/Users/niranjan/work/projects/git/scala/argtyping/src/main/resources/english.all.3class.nodistsim.prop"
    var dontAdjustOffsets = false
    var maltParserPath="/Users/niranjan/work/projects/git/relgrams/relgramtuples/src/main/resources/engmalt.linear-1.7.mco"
    val parser = new OptionParser() {
      arg("inputPath", "hdfs input path", {str => inputPath = str})
      arg("outputPath", "hdfs output path", { str => outputPath = str })
      opt("wnHome", "wordnet home", { str => wnHome = str })
      opt("wnTypesFile", "wordnet home", { str => wnTypesFile = str })
      opt("ne7ModelFile", "Stanford NE 7 class model file.", { str => ne7ModelFile = str })
      opt("ne3ModelFile", "Stanford NE 3 class model file.", { str => ne3ModelFile = str })
      opt("dontAdjustOffsets", "Don't adjust offsets.", {str => dontAdjustOffsets = str.toBoolean})
      opt("mpp", "maltParserPath", "Malt parser file path.", {str => maltParserPath = str})
    }

    if (!parser.parse(args)) return
    val extractor = new Extractor(maltParserPath)
    val argTyper = new ArgumentsTyper(ne7ModelFile, ne3ModelFile, wnHome, wnTypesFile, 1)
    val relgramExtractor = new RelgramTuplesExtractor(extractor, argTyper, dontAdjustOffsets)
    val writer = new PrintWriter(outputPath, "utf-8")
    writer.println("%s-%s\t%s\t%s\t%s\t%s".format("DOCID", "SENTID", "Tuple Part", "Original", "Head", "Type"))
    Source.fromFile(inputPath).getLines().foreach(line => {
      val splits = line.split("\t")
      if(splits.size > 4){
        val docid = splits(2)
        val sentid = splits(3).toInt
        val sentence = splits(4)
        val relgramTuples = relgramExtractor.extract(docid, sentid, sentence)
        relgramTuples.foreach(relgramTuple => {
          val typedExtrInstance = relgramTuple.typedExtrInstance
          val arg1Head = typedExtrInstance.arg1Head.map(h => h.string).mkString(" ")
          val relHead = typedExtrInstance.relHead.map(h => h.string).mkString(" ")
          val arg2Head = typedExtrInstance.arg2Head.map(h => h.string).mkString(" ")
          def typesString(argTypes:Iterable[Type]) = argTypes.map(atype => atype.name + ":" + atype.source)
          val arg1String = "%s\t%s\t%s".format(typedExtrInstance.extractionInstance.extr.arg1.text, arg1Head, typesString(typedExtrInstance.arg1Types).mkString(","))
          val relString = "%s\t%s".format(typedExtrInstance.extractionInstance.extr.rel.text, relHead)
          val arg2String = "%s\t%s\t%s".format(typedExtrInstance.extractionInstance.extr.arg2.text, arg2Head, typesString(typedExtrInstance.arg2Types).mkString(","))
          writer.println("%s-%s\t%s\t%s".format(docid, sentid, "ARG1", arg1String))
          writer.println("%s%s\t%s\t%s".format("", "", "REL", relString))
          writer.println("%s%s\t%s\t%s".format("", "", "ARG2", arg2String))
          writer.println

      })

      }else{
        logger.error("Ignoring line with less than 3 splits: " + line)
      }
    })
    writer.close
  }
}












