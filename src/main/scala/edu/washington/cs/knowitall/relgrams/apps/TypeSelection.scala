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
import org.apache.hadoop.mapreduce.{Job, Reducer, Mapper}
import org.apache.hadoop.io.{Text, LongWritable}
import edu.washington.cs.knowitall.relgrams.extractors.{OllieExtractionOrdering, Extractor}
import edu.washington.cs.knowitall.relgrams.typers.{TypedExtractionInstance, ArgumentsTyper}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.GenericOptionsParser
import com.hadoop.mapreduce.LzoTextInputFormat
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat


class ValuesCounterReducer extends Reducer[Text, Text, Text, Text] {
  val logger = LoggerFactory.getLogger(this.getClass)
  override def setup(context:Reducer[Text, Text, Text, Text] #Context){

  }

  override def reduce(key: Text,
                      values: java.lang.Iterable[Text],
                      context:Reducer[Text,Text,Text,Text]#Context) {
    val size = values.size
    context.write(new Text(key), new Text(size.toString))
  }
}
class TypeSelectionReducer extends ValuesCounterReducer

class TypeSelectionMapper extends Mapper[LongWritable, Text, Text, Text] {

  val logger = LoggerFactory.getLogger(this.getClass)
  var extractor:Extractor = null
  var argTyper:ArgumentsTyper = null

  override def setup(context:Mapper[LongWritable,Text, Text, Text] #Context){
    val maltParserPath = context.getConfiguration.get("maltParserPath", "NA")
    val neModelFile = context.getConfiguration.get("neModelFile", "NA")
    val wnHome = context.getConfiguration.get("wnHome", "NA")
    val wnTypesFile = context.getConfiguration.get("wnTypesFile", "NA")
    val numSenses = context.getConfiguration.getInt("numWNSenses", 1)

    extractor = new Extractor(maltParserPath)
    argTyper = new ArgumentsTyper(neModelFile, wnHome, wnTypesFile, numSenses)
  }

  override def map(key: LongWritable,
                   value: Text,
                   context: Mapper[LongWritable,Text, Text, Text] #Context) {
    try{
      val splits = value.toString.split("\t")
      if(splits.size > 4){
        val docid = splits(2)
        val sentid = splits(3)
        val sentence = splits(4)
        extractor.extract(sentence)
          .filter(confExtr => confExtr._1 > 0.1)
          .toSeq
          .foreach(confExtr => {
          val extrInstance = confExtr._2
          argTyper.assignTypes(extrInstance) match {
            case Some(typedExtrInstance:TypedExtractionInstance) => exportTypeSelectionFormat(typedExtrInstance, context)
            case _ => //logger.error("Failed to extract head word for extrInstance: " + extrInstance.extr.arg1.nodes.seq.toString + " and " + extrInstance.extr.arg2.nodes.seq.toString)
          }
        })
      }else{
        logger.error("Skipping line with < 6 fields: " + splits.size + " and string: " + splits.mkString("_,_"))
      }
    }catch {
      case e:Exception => {
        logger.error("Caught exception handling line: " + value.toString)
        logger.error(e.getStackTraceString)
      }
    }
  }

  def exportTypeSelectionFormat(typedExtrInstance: TypedExtractionInstance, context: Mapper[LongWritable, Text, Text, Text]#Context) {
    val arg1Head = typedExtrInstance.arg1Head.map(h => h.string).mkString(" ")
    val relHead = typedExtrInstance.relHead.map(h => h.string).mkString(" ")//typedExtrInstance.extractionInstance.extr.rel.text
    val arg2Head = typedExtrInstance.arg2Head.map(h => h.string).mkString(" ")
    val arg1Types = typedExtrInstance.arg1Types.map(a1type => a1type.name.split(";").head)
    val arg2Types = typedExtrInstance.arg2Types.map(a2type => a2type.name.split(";").head)
    addKeyValueForArgTypes("arg1", arg1Types, arg1Head, relHead, context)
    addKeyValueForArgTypes("arg2", arg2Types, arg2Head, relHead, context)
    addKeyValueForArgTypes("arg", arg1Types, arg1Head, relHead, context)
    addKeyValueForArgTypes("arg", arg2Types, arg2Head, relHead, context)
  }

  def addKeyValueForArgTypes(n:String, argTypes: Iterable[String], argHead: String, relHead: String, context: Mapper[LongWritable, Text, Text, Text]#Context) {
    argTypes.foreach(atype => {
      val argKey = "%s\t%s\t%s".format(n, atype, argHead)
      context.write(new Text(argKey), new Text(""))
      val argRelKey = "%s\t%s\t%s\t%s".format(n + "rel", atype, relHead, argHead)
      context.write(new Text(argRelKey), new Text(""))
    })
  }

}

object TypeSelectionHadoop{



  def main(args:Array[String]){
    val conf = new Configuration()
    val otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs
    if (otherArgs.length < 2) {
      println("Usage: TypeSelectionHadoop -DwnHome=wnHome -DwnTypesFile=wordnet-classes-large.txt -D <inputpath> <outputpath>")
      println("Args: " + otherArgs.mkString(","))
      return
    }
    println
    println("*****************************")
    println("Configuration: " + conf.toString)
    println("*****************************")
    println

    println("Args: " + otherArgs.mkString(","))
    val inputPath = otherArgs(0)
    val outputPath = otherArgs(1)
    println("Input path: " + inputPath)
    println("Output path: " + outputPath)


    val ejob = new Job(conf, "type-selection")
    ejob.setJarByClass(classOf[TypeSelectionMapper])

    if (conf.getBoolean("inLzo", false)) {
      ejob.setInputFormatClass(classOf[LzoTextInputFormat])
    } else {
      ejob.setInputFormatClass(classOf[TextInputFormat]) //LzoTextInputFormat])
    }

    ejob setOutputKeyClass classOf[Text]
    ejob setOutputValueClass classOf[Text]

    ejob setMapperClass classOf[TypeSelectionMapper]
    ejob setReducerClass  classOf[TypeSelectionReducer]
    ejob.setNumReduceTasks(1)

    FileInputFormat.addInputPath(ejob, new Path(inputPath))
    FileOutputFormat.setOutputPath(ejob, new Path(outputPath))

    ejob.waitForCompletion(true)

  }
}
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
