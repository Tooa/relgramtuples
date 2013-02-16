package edu.washington.cs.knowitall.relgrams.extractors

import edu.washington.cs.knowitall.tool.parse.MaltParser
import edu.washington.cs.knowitall.openparse.OpenParse
import edu.washington.cs.knowitall.ollie.Ollie
import edu.washington.cs.knowitall.ollie.confidence.OllieConfidenceFunction
import java.io.File
import edu.washington.cs.knowitall.tool.parse.graph.DependencyGraph
import org.slf4j.LoggerFactory

/**
 * Created with IntelliJ IDEA.
 * User: niranjan
 * Date: 2/14/13
 * Time: 11:20 AM
 * To change this template use File | Settings | File Templates.
 */

class Extractor {

  val logger = LoggerFactory.getLogger(this.getClass)

  var parser:MaltParser = null
  var openparse = OpenParse.withDefaultModel(new OpenParse.Configuration(confidenceThreshold = 0.005))
  var ollieExtractor = new Ollie(openparse)
  var confFunction = OllieConfidenceFunction.loadDefaultClassifier



  def this(maltParserPath:String){
    this()
    setMaltParserPath(maltParserPath)
  }

  def setMaltParserPath(maltParserpath: String){
    parser = new MaltParser(new File(maltParserpath))
    openparse = OpenParse.withDefaultModel(new OpenParse.Configuration(confidenceThreshold = 0.005))
    ollieExtractor = new Ollie(openparse)
    confFunction = OllieConfidenceFunction.loadDefaultClassifier
  }

  def extract(sentence:String) = {
    val dgraph: DependencyGraph = parser.dependencyGraph(sentence)
    ollieExtractor.extract(dgraph).map(extr => (confFunction.getConf(extr), extr)).toSeq

  }

  def main(args:Array[String]){
    val extractor = new Extractor
    val tuples = extractor.extract("Obama filed a lawsuit against Microsoft on Monday.")
    tuples.foreach(tuple => {
      logger.info(tuple._1 + "\t" + tuple._2.extr.nodes)
    })
  }

}
