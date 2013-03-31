package edu.washington.cs.knowitall.relgrams.utils

/**
 * Created with IntelliJ IDEA.
 * User: niranjan
 * Date: 3/29/13
 * Time: 1:28 PM
 * To change this template use File | Settings | File Templates.
 */
import org.slf4j.LoggerFactory
import edu.washington.cs.knowitall.tool.parse.graph.DependencyNode

object NETyperUtils {
    val logger = LoggerFactory.getLogger(this.getClass)

  def buildString(nodes: Iterable[DependencyNode]) = {
    val builder = new StringBuilder()
    for (node <- nodes) {
      builder.append(" " * (node.offset - builder.length))
      builder.append(node.text)
    }
    builder.toString()
  }
}
