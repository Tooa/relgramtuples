package edu.washington.cs.knowitall.relgrams.apps

/**
 * Created with IntelliJ IDEA.
 * User: niranjan
 * Date: 2/17/13
 * Time: 9:24 AM
 * To change this template use File | Settings | File Templates.
 */


import scala.collection.JavaConversions._
import io.Source
import collection.mutable
import collection.mutable.ArrayBuffer
import java.io.PrintWriter
import org.slf4j.LoggerFactory

object TypeSelection {

  val logger = LoggerFactory.getLogger(this.getClass)
  def main(args:Array[String]){
    val inputFile = args(0)
    val topk = args(1).toInt
    val outputFile = args(2)
    val topArgs = new mutable.HashMap[String, ArrayBuffer[(String, Int)]]()
    //arg     StanfordDATE    May      2
    Source.fromFile(inputFile).getLines().foreach(line => {
      val splits = line.split("\t")
      if(splits.size > 3){
        try{
          topArgs.getOrElseUpdate(splits(1), new ArrayBuffer[(String, Int)]) += new Tuple2[String, Int](splits(2), splits(3).toInt)
        }catch{
          case e:Exception => logger.error("Caught exception processing line: " +splits.mkString("_,_"))
        }
      }else{
        logger.error("Ignoring line with < 3 splits: " + splits.mkString("_,_"))
      }
    })
    val writer = new PrintWriter(outputFile, "utf-8")
    topArgs.keys.toSeq.sortBy(k => k).foreach(key => {
      val targs = topArgs(key).sortBy(x => -x._2).take(topk)
      writer.println(key + "\t" + targs.map(ta => "%s (%d)".format(ta._1, ta._2)).mkString(","))
    })
    writer.close
  }
}
