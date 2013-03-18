package edu.washington.cs.knowitall.relgrams.apps

/**
 * Created with IntelliJ IDEA.
 * User: niranjan
 * Date: 2/21/13
 * Time: 11:19 AM
 * To change this template use File | Settings | File Templates.
 */


import scala.collection.JavaConversions._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.GenericOptionsParser
import org.apache.hadoop.mapreduce.{Mapper, Reducer, Job}
import com.hadoop.mapreduce.LzoTextInputFormat
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.slf4j.LoggerFactory
import edu.washington.cs.knowitall.relgrams.extractors.{RelgramTuple, Extractor, RelgramTuplesExtractor}
import edu.washington.cs.knowitall.relgrams.typers.{TypersUtil, ArgumentsTyper}
import edu.washington.cs.knowitall.openparse.extract.Extraction.Part
import edu.washington.cs.knowitall.tool.tokenize.Token
import edu.washington.cs.knowitall.tool.typer.Type


class DocTuplesReducer extends Reducer[Text, Text, Text, Text] {

  val logger = LoggerFactory.getLogger(this.getClass)
  override def setup(context:Reducer[Text, Text, Text, Text] #Context){

  }

  override def reduce(key: Text,
                      values: java.lang.Iterable[Text],
                      context:Reducer[Text,Text,Text,Text]#Context) {

    val valuesString = values.map(x => x.toString).mkString("_TUPLE_SEP_")
    context.write(new Text(key), new Text(valuesString))
  }
}


class IdentityReducer extends Reducer[Text, Text, Text, Text] {

  val logger = LoggerFactory.getLogger(this.getClass)
  override def setup(context:Reducer[Text, Text, Text, Text] #Context){

  }

  override def reduce(key: Text,
                      values: java.lang.Iterable[Text],
                      context:Reducer[Text,Text,Text,Text]#Context) {
    values.foreach(value => context.write(new Text(key), new Text(value)))
  }
}




class RelgramTuplesReducer extends IdentityReducer


class RelgramTuplesMapper extends Mapper[LongWritable, Text, Text, Text] {
  val logger = LoggerFactory.getLogger(this.getClass)
  var relgramExtractor:RelgramTuplesExtractor = null

  override def setup(context:Mapper[LongWritable,Text, Text, Text] #Context){
    val maltParserPath = context.getConfiguration.get("maltParserPath", "NA")
    val neModelFile = context.getConfiguration.get("neModelFile", "NA")
    val wnHome = context.getConfiguration.get("wnHome", "NA")
    val wnTypesFile = context.getConfiguration.get("wnTypesFile", "NA")
    val numSenses = context.getConfiguration.getInt("numWNSenses", 1)

    val extractor = new Extractor(maltParserPath)
    val argTyper = new ArgumentsTyper(neModelFile, wnHome, wnTypesFile, numSenses)
    relgramExtractor = new RelgramTuplesExtractor(extractor, argTyper)
  }


  override def map(key: LongWritable,
                   value: Text,
                   context: Mapper[LongWritable,Text, Text, Text] #Context) {
    try{
      val splits = value.toString.split("\t")
      if(splits.size > 4){
        val docid = splits(2)
        val sentid = splits(3).toInt
        val sentence = splits(4)
        val relgramTuples = relgramExtractor.extract(docid, sentid, sentence)
        relgramTuples.foreach(relgramTuple => exportRelgramTuple(relgramTuple, context))
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




  val span_sep = "_SPAN_"
  def textWithSpan(part: Part) = {
    val interval = TypersUtil.span(part.nodes.toSeq)
    "%s%s%s-%s".format(part.text, span_sep, interval.start, interval.end)
  }

  def exportRelgramTuple(relgramTuple:RelgramTuple, context: Mapper[LongWritable, Text, Text, Text]#Context){
    val typedExtractionInstance = relgramTuple.typedExtrInstance
    val docid = relgramTuple.docid
    val sentid = relgramTuple.sentid
    val sentence = relgramTuple.sentence
    val extrid = relgramTuple.extrid
    val hashes = relgramTuple.hashes

    //val origTuple = "%s\t%s\t%s".format(typedExtractionInstance.extractionInstance.extr.anew Type("person", "Pronoun", argTokens(0).interval, argTokens(0).string)::Nilrg1.text, typedExtractionInstance.extractionInstance.extr.rel.text,
    //  typedExtractionInstance.extractionInstance.extr.arg2.text)

    val arg1SpanText = textWithSpan(typedExtractionInstance.extractionInstance.extr.arg1)
    val relSpanText  = textWithSpan(typedExtractionInstance.extractionInstance.extr.rel)
    val arg2SpanText = textWithSpan(typedExtractionInstance.extractionInstance.extr.arg2)

    val origTuple = "%s\t%s\t%s".format(arg1SpanText, relSpanText, arg2SpanText)

    def tokensSpanString(tokens:Seq[Token]) = {
      val span = TypersUtil.span(tokens)
      "%s%s%d-%d".format(tokensToString(tokens), span_sep, span.start, span.end)
    }

    def tokensToString(tokens:Seq[Token]) = tokens.map(t => t.string).mkString(" ")
    val headTuple = "%s\t%s\t%s".format(tokensSpanString(typedExtractionInstance.arg1Head),
      tokensSpanString(typedExtractionInstance.relHead),
      tokensSpanString(typedExtractionInstance.arg2Head))

    def typesString(types:Iterable[Type]) = types.map(t => "Type:" + t.name).mkString(",")//"Type:" + t.name + ":" + t.source).mkString(",")
    val key = "%s\t%s\t%s\t%d".format(docid, sentid, sentence, extrid)
    val value = "%s\t%s\t%s\t%s\t%s".format(hashes.mkString(","), origTuple, headTuple, typesString(typedExtractionInstance.arg1Types), typesString(typedExtractionInstance.arg2Types))
    context.write(new Text(key), new Text(value))
  }

  /*8 //sid sentence (orig) (head) arg1types arg2types
def exportRelgramTuples(docid:String, sid:String, sentence:String, hashes:Iterable[Int], eid:Int, template:String, typedExtractionInstance:TypedExtractionInstance, context: Mapper[LongWritable, Text, Text, Text]#Context){

  val origTuple = "%s\t%s\t%s".format(typedExtractionInstance.extractionInstance.extr.arg1.text, typedExtractionInstance.extractionInstance.extr.rel.text,
                                      typedExtractionInstance.extractionInstance.extr.arg2.text)

  def tokensToString(tokens:Seq[Token]) = tokens.map(t => t.string).mkString(" ")
  val headTuple = "%s\t%s\t%s".format(tokensToString(typedExtractionInstance.arg1Head),
                                      tokensToString(typedExtractionInstance.relHead),
                                      tokensToString(typedExtractionInstance.arg2Head))


  def typesString(types:Iterable[Type]) = types.map(t => t.name + ":" + t.source).mkString(",")
  val key = "%s\t%s\t%s\t%d".format(docid, sid, sentence, eid)
  val value = "%s\t%s\t%s\t%s\t%s".format(template, origTuple, headTuple, typesString(typedExtractionInstance.arg1Types), typesString(typedExtractionInstance.arg2Types))
  context.write(new Text(key), new Text(value))
}   */

}

object RelgramTuplesHadoop{



  def main(args:Array[String]){
    val conf = new Configuration()
    val otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs
    if (otherArgs.length < 2) {
      println("Usage: RelgramTuplesHadoop -DexportRelgramFormat=true " +
             "-DmaltParserPath=src/main/resources/engmalt.linear-1.7.mco " +
             "-DwnHome=/Users/niranjan/work/local/wordnet3.0 " +
             "-DwnTypesFile=wordnet-classes-large.txt " +
             "-DneModelFile=src/main/resources/english.muc.7class.nodistsim.crf.ser.gz  " +
             "<chunker file> <outputfile>")
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
    //ejob setReducerClass classOf[RelgramTuplesReducer]
    ejob.setNumReduceTasks(0)

    FileInputFormat.addInputPath(ejob, new Path(inputPath))
    FileOutputFormat.setOutputPath(ejob, new Path(outputPath))


    ejob.waitForCompletion(true)

  }
}
