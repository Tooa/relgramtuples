package edu.washington.cs.knowitall.relgrams.apps

import scala.collection.JavaConversions._
import scopt.mutable.OptionParser
import edu.washington.cs.knowitall.relgrams.typers.{TypedExtractionInstance, ArgumentsTyper}
import edu.washington.cs.knowitall.relgrams.extractors.{OllieExtractionOrdering, Extractor}
import io.Source
import org.slf4j.LoggerFactory
import java.io.PrintWriter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.GenericOptionsParser
import com.hadoop.mapreduce.LzoTextInputFormat
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Reducer, Mapper, Job}
import edu.washington.cs.knowitall.tool.tokenize.Token
import edu.washington.cs.knowitall.tool.typer.Type
import edu.washington.cs.knowitall.openparse.extract.TemplateExtractor
import edu.washington.cs.knowitall.ollie.OllieExtractionInstance

/**
 * Created with IntelliJ IDEA.
 * User: niranjan
 * Date: 2/14/13
 * Time: 4:03 PM
 * To change this template use File | Settings | File Templates.
 */

class RelgramReducer extends Reducer[Text, Text, Text, Text] {
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

class RelgramTuplesMapper extends Mapper[LongWritable, Text, Text, Text] {
  val logger = LoggerFactory.getLogger(this.getClass)
  var extractor:Extractor = null
  var argTyper:ArgumentsTyper = null
  var relgramFormat = true
  var typeSelectionFormat = false

  override def setup(context:Mapper[LongWritable,Text, Text, Text] #Context){
    val maltParserPath = context.getConfiguration.get("maltParserPath", "NA")
    val neModelFile = context.getConfiguration.get("neModelFile", "NA")
    val wnHome = context.getConfiguration.get("wnHome", "NA")
    val wnTypesFile = context.getConfiguration.get("wnTypesFile", "NA")
    val numSenses = context.getConfiguration.getInt("numWNSenses", 1)
    relgramFormat = context.getConfiguration.getBoolean("relgramFormat", true)
    typeSelectionFormat = context.getConfiguration.getBoolean("typeSelectionFormat", false)
    extractor = new Extractor(maltParserPath)
    argTyper = new ArgumentsTyper(neModelFile, wnHome, wnTypesFile, numSenses)
  }

  override def map(key: LongWritable,
                   value: Text,
                   context: Mapper[LongWritable,Text, Text, Text] #Context) {
   try{
    val splits = value.toString.split("\t")
    if(splits.size > 5){
      val docid = splits(3)
      val sentid = splits(4)
      val sentence = splits(5)
      val sortedExtrInstances = extractor.extract(sentence)
                                         .filter(confExtr => confExtr._1 > 0.1)
                                         .map(confExtr => confExtr._2)
                                         .toSeq
                                         .sorted(new OllieExtractionOrdering)

      var eid = 0
      sortedExtrInstances.foreach(extrInstance => {
        argTyper.assignTypes(extrInstance) match {
          case Some(typedExtrInstance:TypedExtractionInstance) => {
            if(relgramFormat){
              val template = extrInstance.pattern.pattern.toString()
              exportRelgramTuples(docid, sentid, sentence, eid, template, typedExtrInstance, context)
            }else if(typeSelectionFormat){
              logger.info("Exporting type selection format: ")
              exportTypeSelectionFormat(typedExtrInstance, context)
            }
          }
          case _ => //logger.error("Failed to extract head word for extrInstance: " + extrInstance.extr.arg1.nodes.seq.toString + " and " + extrInstance.extr.arg2.nodes.seq.toString)
        }

      })
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
    val relHead = typedExtrInstance.extractionInstance.extr.rel.text
    val arg2Head = typedExtrInstance.arg2Head.map(h => h.string).mkString(" ")
    val arg1Types = typedExtrInstance.arg1Types.map(a1type => a1type.name.split(";").head)
    val arg2Types = typedExtrInstance.arg2Types.map(a2type => a2type.name.split(";").head)
    addKeyValueForArgTypes("arg1", arg1Types, context, arg1Head, relHead)
    addKeyValueForArgTypes("arg2", arg2Types, context, arg2Head, relHead)
    addKeyValueForArgTypes("arg", arg1Types, context, arg1Head, relHead)
    addKeyValueForArgTypes("arg", arg2Types, context, arg2Head, relHead)
  }

  //sid sentence (orig) (head) arg1types arg2types
 def exportRelgramTuples(docid:String, sid:String, sentence:String, eid:Int, template:String, typedExtractionInstance:TypedExtractionInstance, context: Mapper[LongWritable, Text, Text, Text]#Context){

    val origTuple = "%s\t%s\t%s".format(typedExtractionInstance.extractionInstance.extr.arg1.text, typedExtractionInstance.extractionInstance.extr.rel.text,
                                        typedExtractionInstance.extractionInstance.extr.arg2.text)

    def tokensToString(tokens:Seq[Token]) = tokens.map(t => t.string).mkString(" ")
    val headTuple = "%s\t%s\t%s".format(tokensToString(typedExtractionInstance.arg1Head),
                                        tokensToString(typedExtractionInstance.relHead),
                                        tokensToString(typedExtractionInstance.arg2Head))

    def typesString(types:Iterable[Type]) = types.map(t => t.name + ":" + t.source).mkString(" ")
    val key = "%s\t%s\t%s\t%d".format(docid, sid, sentence, eid)
    val value = "%s\t%s\t%s\t%s\t%s".format(template, origTuple, headTuple, typesString(typedExtractionInstance.arg1Types), typesString(typedExtractionInstance.arg2Types))
    context.write(new Text(key), new Text(value))
 }
  def addKeyValueForArgTypes(n:String, argTypes: Iterable[String], context: Mapper[LongWritable, Text, Text, Text]#Context, argHead: String, relHead: String) {
    argTypes.foreach(atype => {
      val argKey = "%s\t%s\t%s".format(n, atype, argHead)
      context.write(new Text(argKey), new Text(""))
      val argRelKey = "%s\t%s\t%s\t%s".format(n + "rel", atype, relHead, argHead)
      context.write(new Text(argRelKey), new Text(""))
    })
  }
}


object RelgramTuplesHadoop{



  def main(args:Array[String]){
    val conf = new Configuration()
    val otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs
    if (otherArgs.length < 2) {
      println("Usage: RelgramsHadoop --extractReverbTuples --count <in> <out1> <out2>")
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

    /**var inputPath, outputPath = ""
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

    if (!parser.parse(args)) return*/

    val ejob = new Job(conf, "extract-relgram-tuples")
    ejob.setJarByClass(classOf[RelgramTuplesMapper])

    if (conf.getBoolean("inLzo", false)) {
      ejob.setInputFormatClass(classOf[LzoTextInputFormat])
    } else {
      ejob.setInputFormatClass(classOf[TextInputFormat]) //LzoTextInputFormat])
    }


    ejob setOutputKeyClass classOf[Text]
    ejob setOutputValueClass classOf[Text]

    //Input: <docname, <extraction sentenceWords record1__DOCEXTR_DELIM__extraction sentenceWords record2__DOCEXTR_DELIM__extraction sentenceWords record3>
    //Output: List of rel-view grams <vType, first + second + hashes + count>
    ejob setMapperClass classOf[RelgramTuplesMapper]
    ejob setReducerClass  classOf[RelgramReducer]
    ejob.setNumReduceTasks(1)

    FileInputFormat.addInputPath(ejob, new Path(inputPath))
    FileOutputFormat.setOutputPath(ejob, new Path(outputPath))

    ejob.waitForCompletion(true)

  }
}
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
    val argTyper = new ArgumentsTyper(neModelFile, wnHome, wnTypesFile, 1)
    val writer = new PrintWriter(outputPath, "utf-8")
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
              writer.println("Head\t%s\t%s".format(extractionString, headTuple))
              //println("Head Tuple: " + headTuple)
              var typedArg1s = arg1Head::Nil
              typedArg1s = typedArg1s ++ typedExtrInstance.arg1Types.map(a1type => a1type.name + ":" + a1type.source)
              var typedArg2s = arg2Head::Nil
              typedArg2s = typedArg2s ++ typedExtrInstance.arg2Types.map(a2type => a2type.name + ":" + a2type.source)

              typedArg1s.foreach(a1type => {
                typedArg2s.foreach(a2type =>  {
                  val typedString =  "(%s, %s, %s)".format(a1type, relHead, a2type)
                  writer.println("Typed\t%s\t%s".format(extractionString, typedString))
                  //println("Typed Tuple: (%s, %s, %s)".format(a1type, relHead, a2type))
                  //println
                })
              })
              writer.println

            }
            case None => logger.error("Failed to obtian typed extr instance for: " + extrInstance)
          }
      })
      }
    })
    writer.close
  }
}
