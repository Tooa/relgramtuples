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


object RelgramTuplesApp {

  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args:Array[String]) {

    var inputPath, outputPath = ""
    var wnHome = "src/main/resources/WordNet-3.0/"
    var wnTypesFile = "src/main/resources/wordnet-classes-large.txt"
    var ne7ModelFile = "src/main/resources/english.muc.7class.nodistsim.crf.ser.gz"
    var ne3ModelFile = "src/main/resources/english.all.3class.nodistsim.crf.ser.gz"
    var dontAdjustOffsets = false
    var maltParserPath="src/main/resources/engmalt.linear-1.7.mco"
    var removeInvertedExtractions = false
    var removeImposedPrepExtractions = false
    var confidenceThreshold = 0.1
    val parser = new OptionParser() {
      arg("inputPath", "hdfs input path", {str => inputPath = str})
      arg("outputPath", "hdfs output path", { str => outputPath = str })
      opt("wnHome", "wordnet home", { str => wnHome = str })
      opt("wnTypesFile", "wordnet home", { str => wnTypesFile = str })
      opt("ne7ModelFile", "Stanford NE 7 class model file.", { str => ne7ModelFile = str })
      opt("ne3ModelFile", "Stanford NE 3 class model file.", { str => ne3ModelFile = str })
      opt("dontAdjustOffsets", "Don't adjust offsets.", {str => dontAdjustOffsets = str.toBoolean})
      opt("removeInvertedExtractions", "Remove extractions where the arguments do not follow the sentence order.", {str => removeInvertedExtractions = str.toBoolean})
      opt("removeImposedPrepExtractions", "Remove extractions with imposed prepositions.", {str => removeImposedPrepExtractions = str.toBoolean})
      opt("confidenceThreshold", "Confidence threshold for extractions.", {str => confidenceThreshold = str.toDouble})
      opt("mpp", "maltParserPath", "Malt parser file path.", {str => maltParserPath = str})
    }

    if (!parser.parse(args)) return
    val extractor = new Extractor(maltParserPath)
    val argTyper = new ArgumentsTyper(ne7ModelFile, ne3ModelFile, wnHome, wnTypesFile, 1)
    val relgramExtractor = new RelgramTuplesExtractor(extractor, argTyper, dontAdjustOffsets, confidenceThreshold, removeInvertedExtractions, removeImposedPrepExtractions)
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
        logger.error("Ignoring line with less than 4 splits: " + line)
      }
    })
    writer.close
  }
}












