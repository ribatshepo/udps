package io.gbmm.udps.catalog.lineage

import java.util.UUID

import io.circe.Json
import io.circe.syntax._

/** Supported export formats for lineage graphs. */
sealed trait ExportFormat
object ExportFormat {
  case object GraphML extends ExportFormat
  case object JsonFormat extends ExportFormat
  case object Mermaid extends ExportFormat
}

/** Exports a LineageDAG to various graph representation formats. */
class GraphExporter {

  /**
   * Exports the full DAG in the specified format.
   *
   * @param dag    the lineage DAG to export
   * @param format the target export format
   * @return string representation in the chosen format
   */
  def `export`(dag: LineageDAG, format: ExportFormat): String =
    format match {
      case ExportFormat.GraphML    => exportGraphML(dag.nodes, dag.edges)
      case ExportFormat.JsonFormat => exportJson(dag.nodes, dag.edges)
      case ExportFormat.Mermaid    => exportMermaid(dag.nodes, dag.edges)
    }

  /**
   * Exports a filtered subset of the DAG containing only the specified nodes
   * and edges between them.
   *
   * @param dag           the lineage DAG to export
   * @param format        the target export format
   * @param filterNodeIds the set of node IDs to include
   * @return string representation of the filtered graph
   */
  def exportFiltered(dag: LineageDAG, format: ExportFormat, filterNodeIds: Set[UUID]): String = {
    val filteredNodes = dag.nodes.filter { case (id, _) => filterNodeIds.contains(id) }
    val filteredEdges = dag.edges.filter { e =>
      filterNodeIds.contains(e.sourceTableId) && filterNodeIds.contains(e.targetTableId)
    }
    format match {
      case ExportFormat.GraphML    => exportGraphML(filteredNodes, filteredEdges)
      case ExportFormat.JsonFormat => exportJson(filteredNodes, filteredEdges)
      case ExportFormat.Mermaid    => exportMermaid(filteredNodes, filteredEdges)
    }
  }

  private def exportGraphML(nodes: Map[UUID, LineageNode], edges: Seq[LineageEdge]): String = {
    val sb = new StringBuilder
    sb.append("""<?xml version="1.0" encoding="UTF-8"?>""")
    sb.append('\n')
    sb.append("""<graphml xmlns="http://graphml.graphstruct.org/graphml">""")
    sb.append('\n')
    sb.append("""  <key id="name" for="node" attr.name="name" attr.type="string"/>""")
    sb.append('\n')
    sb.append("""  <key id="nodeType" for="node" attr.name="nodeType" attr.type="string"/>""")
    sb.append('\n')
    sb.append("""  <graph id="lineage" edgedefault="directed">""")
    sb.append('\n')

    nodes.foreach { case (id, node) =>
      sb.append(s"""    <node id="${escapeXml(id.toString)}">""")
      sb.append('\n')
      sb.append(s"""      <data key="name">${escapeXml(node.name)}</data>""")
      sb.append('\n')
      sb.append(s"""      <data key="nodeType">${escapeXml(node.nodeType)}</data>""")
      sb.append('\n')
      sb.append("""    </node>""")
      sb.append('\n')
    }

    edges.foreach { edge =>
      sb.append(s"""    <edge id="${escapeXml(edge.id.toString)}" source="${escapeXml(edge.sourceTableId.toString)}" target="${escapeXml(edge.targetTableId.toString)}"/>""")
      sb.append('\n')
    }

    sb.append("""  </graph>""")
    sb.append('\n')
    sb.append("""</graphml>""")
    sb.toString()
  }

  private def exportJson(nodes: Map[UUID, LineageNode], edges: Seq[LineageEdge]): String = {
    val nodesJson = nodes.values.toSeq.map { node =>
      Json.obj(
        "id"       -> node.id.toString.asJson,
        "name"     -> node.name.asJson,
        "nodeType" -> node.nodeType.asJson
      )
    }

    val edgesJson = edges.map { edge =>
      Json.obj(
        "id"             -> edge.id.toString.asJson,
        "sourceTableId"  -> edge.sourceTableId.toString.asJson,
        "sourceColumnId" -> edge.sourceColumnId.map(_.toString).asJson,
        "targetTableId"  -> edge.targetTableId.toString.asJson,
        "targetColumnId" -> edge.targetColumnId.map(_.toString).asJson,
        "queryId"        -> edge.queryId.map(_.toString).asJson,
        "createdAt"      -> edge.createdAt.toString.asJson
      )
    }

    Json.obj(
      "nodes" -> nodesJson.asJson,
      "edges" -> edgesJson.asJson
    ).spaces2
  }

  private def exportMermaid(nodes: Map[UUID, LineageNode], edges: Seq[LineageEdge]): String = {
    val sb = new StringBuilder
    sb.append("graph LR")
    sb.append('\n')

    nodes.foreach { case (id, node) =>
      val shortId = mermaidId(id)
      val label = escapeMermaid(node.name)
      sb.append(s"""  $shortId["$label"]""")
      sb.append('\n')
    }

    edges.foreach { edge =>
      val srcId = mermaidId(edge.sourceTableId)
      val tgtId = mermaidId(edge.targetTableId)
      sb.append(s"  $srcId --> $tgtId")
      sb.append('\n')
    }

    sb.toString()
  }

  private def escapeXml(s: String): String =
    s.replace("&", "&amp;")
      .replace("<", "&lt;")
      .replace(">", "&gt;")
      .replace("\"", "&quot;")
      .replace("'", "&apos;")

  private def escapeMermaid(s: String): String =
    s.replace("\"", "#quot;")

  private def mermaidId(uuid: UUID): String = {
    val hex = uuid.toString.replace("-", "")
    s"n$hex"
  }
}
