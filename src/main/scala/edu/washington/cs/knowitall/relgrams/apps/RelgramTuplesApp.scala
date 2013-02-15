package edu.washington.cs.knowitall.relgrams.apps

import scopt.mutable.OptionParser
import edu.washington.cs.knowitall.relgrams.typers.{TypedExtractionInstance, ArgumentsTyper}
import edu.washington.cs.knowitall.relgrams.extractors.Extractor
import io.Source
import org.slf4j.LoggerFactory

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

    var neModelFile = "/Users/niranjan/work/projects/git/scala/argtyping/src/main/resources/english.muc.7class.nodistsim.prop"
    var maltParserPath="/Users/niranjan/work/projects/git/relgrams/relgramtuples/src/main/resources/engmalt.linear-1.7.mco"
    val parser = new OptionParser() {
      arg("inputPath", "hdfs input path", {str => inputPath = str})
      arg("outputPath", "hdfs output path", { str => outputPath = str })
      opt("wnHome", "wordnet home", { str => wnHome = str })
      opt("wnTypesFile", "wordnet home", { str => wnTypesFile = str })
      opt("neModelFile", "Stanford NE model file.", { str => neModelFile = str })
      opt("mpp", "maltParserPath", "Malt parser file path.", {str => maltParserPath = str})
    }

    if (!parser.parse(args)) return
    val extractor = new Extractor(maltParserPath)
    val argTyper = new ArgumentsTyper(neModelFile, wnHome, wnTypesFile)
    Source.fromFile(inputPath).getLines().foreach(line => {
      val splits = line.split("\t")
      if(splits.size > 2){
      val docid = splits(0)
      val sentid = splits(1)
      val sentence = splits(2)
      extractor.extract(sentence)
               .filter(confExtr => confExtr._1 > 0.1)
               .map(ce => ce._2)
                .foreach(extrInstance => {
          argTyper.assignTypes(extrInstance) match {
            case Some(typedExtrInstance:TypedExtractionInstance) => {
              logger.info(typedExtrInstance.toString)

              val origTuple = "(%s, %s, %s)".format(typedExtrInstance.extractionInstance.extr.arg1.text,
                                    typedExtrInstance.extractionInstance.extr.rel.text,
                                    typedExtrInstance.extractionInstance.extr.arg2.text)

              val extractionString = "%s\t%s\t%s".format(docid, sentid,origTuple)
              //println("Original Tuple: " + origTuple)
              val arg1Head = typedExtrInstance.arg1Head.map(h => h.string).mkString(" ")
              val relHead = typedExtrInstance.extractionInstance.extr.rel.text
              val arg2Head = typedExtrInstance.arg2Head.map(h => h.string).mkString(" ")
              val headTuple = "(%s, %s, %s)".format(arg1Head, relHead, arg2Head)
              println("Head\t%s\t%s".format(extractionString, headTuple))
              //println("Head Tuple: " + headTuple)
              var typedArg1s = arg1Head::Nil
              typedArg1s = typedArg1s ++ typedExtrInstance.arg1Types.map(a1type => a1type.name + ":" + a1type.source)
              var typedArg2s = arg2Head::Nil
              typedArg2s = typedArg2s ++ typedExtrInstance.arg2Types.map(a2type => a2type.name + ":" + a2type.source)

              typedArg1s.foreach(a1type => {
                typedArg2s.foreach(a2type =>  {
                  val typedString =  "(%s, %s, %s)".format(a1type, relHead, a2type)
                  println("Typed\t%s\t%s".format(extractionString, typedString))
                  //println("Typed Tuple: (%s, %s, %s)".format(a1type, relHead, a2type))
                  //println
                })
              })
              println

            }
            case None => logger.error("Failed to obtian typed extr instance for: " + extrInstance)
          }
      })
      }
    })
  }
}
