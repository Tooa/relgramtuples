relgramtuples
=============

## Fork changes and general Information

- Requires **Java 7** and **Scala 2.9.2**.
- When using IntelliJ, make sure your `JAVA_HOME` variable points to the correct `Java 7 JDK`.
- I've integrated the  [wordnet-demo](https://github.com/xdavidjung/WordNet-Demo) helper classes, because these dependencies are not available via maven.

## Run

In order to run the application you need to download [wordnet](http://wordnetcode.princeton.edu/3.0/WordNet-3.0.tar.gz). Store the `WordNet-3.0` folder in the `src/main/resources` directory. All other required dependencies are already present in the resource folder (see [RelgramTuplesApp](https://github.com/Tooa/relgramtuples/blob/master/src/main/scala/edu/washington/cs/knowitall/relgrams/apps/RelgramTuplesApp.scala))

To run the application, execute the main method defined in `RelgramTuplesApp`. The execution requires the input file as first parameter and output file as second command line parameter (e.g "src/main/resources/samplefile" "outputfile").


## Input File Structure

The program requires a tab-separated input file with 4 parameter.

1. Unknown
2. Unknown
3. Document ID
4. Sentence ID
5. Sentence


See [samplefile](https://github.com/Tooa/relgramtuples/tree/master/src/main/resources/samplefile).

## Output File Structure

```
DOCID-SENTID	Tuple Part	Original	Head	Type
0-0	ARG1	Angela	Angela	person:Stanford
		REL	likes to play with	like to play with
		ARG2	Vladimir	Vladimir	organization:Stanford,person:Stanford

0-1	ARG1	Angela	Angela	person:Stanford
		REL	also likes	like
		ARG2	Peer	Peer
```
