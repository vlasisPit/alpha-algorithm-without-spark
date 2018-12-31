package alphaAlgorithm

import tools.TraceTools

object AlphaAlgorithm {

  def main(args: Array[String]): Unit = {
    val traceTools: TraceTools = new TraceTools()
    val logPath = "src/main/resources/readDataFiltered.csv"
    val numOfTraces = 30
    val percentage : Float = 1.0f //delete trace occurrences which are less than 1% from all traces
    val readAll : Boolean = false
    val filtering : Boolean = false

    val tracesDS : List[(String, List[String])] = traceTools.getTracesToInspect(logPath, numOfTraces, readAll, filtering, percentage)

  }

}
