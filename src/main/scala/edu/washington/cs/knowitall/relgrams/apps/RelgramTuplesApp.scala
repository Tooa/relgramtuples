package edu.washington.cs.knowitall.relgrams.apps

import com.nicta.scoobi.application.ScoobiApp
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
object RelgramTuplesApp extends ScoobiApp{

  val logger = LoggerFactory.getLogger(this.getClass)

  def run() {

    var inputPath, outputPath = ""
    var wnHome = "/home/niranjan/local/Wordnet3.0"
    var neModelFile = "/Users/niranjan/work/projects/git/scala/argtyping/src/main/resources/english.muc.7class.nodistsim.prop"
    val parser = new OptionParser() {
      arg("inputPath", "hdfs input path", {str => inputPath = str})
      arg("outputPath", "hdfs output path", { str => outputPath = str })
      opt("wnHome", "wordnet home", { str => wnHome = str })
      opt("neModelFile", "Stanford NE model file.", { str => neModelFile = str })
    }

    if (!parser.parse(args)) return
    val extractor = new Extractor()
    val argTyper = new ArgumentsTyper(wnHome, neModelFile)
    Source.fromFile(inputPath).getLines().foreach(line => {
      extractor.extract(line)
               .filter(confExtr => confExtr._1 > 0.1)
               .map(ce => ce._2)
                .foreach(extrInstance => {
          argTyper.assignTypes(extrInstance) match {
            case Some(typedExtrInstance:TypedExtractionInstance) => {
              logger.info(typedExtrInstance.toString)
            }
            case None => logger.error("Failed to obtian typed extr instance for: " + extrInstance)
          }
      })

    })
  }
}
