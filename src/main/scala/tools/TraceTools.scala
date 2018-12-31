package tools

import misc.Pair

import scala.collection.mutable.ListBuffer

class TraceTools {

  def getTracesToInspect(logPath: String, numOfTraces: Int, readAll: Boolean, filtering: Boolean, percentage: Float): List[(String, List[String])] = {
    val initialTracesDS : List[(String, List[String])] = readAll match {
      case true => readAllTracesFromCsvFile(logPath)
      case false => readSpecificNumberOfTracesFromCsvFile(logPath, numOfTraces)
    }

    if (filtering) {
      filterTraces(initialTracesDS, percentage)
    } else {
      initialTracesDS
    }
  }

  /**
    * Read all traces from a CSV file
    * @param path
    * @return
    */
  def readAllTracesFromCsvFile(path: String) : List[(String, List[String])] = {
    //dataset schema "orderid", "eventname", "starttime", "endtime", "status"
    val bufferedSource = io.Source.fromFile(path)

    val traces = bufferedSource.getLines
      .filter(line => List("Completed").contains(line.split(",")(4)))
      .toList
      .sortBy(line=>line.split(",")(2))
      .map(line=>(line.split(",")(0), line.split(",")(1)))
      .groupBy(trace=>trace._1)
      .mapValues(traces=>traces.map(trace=>trace._2))
      .toList

    bufferedSource.close
    traces
  }

  /**
    * Read a specific number of traces (numOfTraces: Int) from a CSV file provided in path.
    * The CSV must contains the following columns
    * orderid", "eventname", "starttime", "endtime", "status"
    * Only logs with status==Completed are examined
    * @param path
    * @param numOfTraces
    * @return
    */
  def readSpecificNumberOfTracesFromCsvFile(path: String, numOfTraces: Int) : List[(String, List[String])] = {
    //dataset schema "orderid", "eventname", "starttime", "endtime", "status"
    val bufferedSource = io.Source.fromFile(path)

    val traces = bufferedSource.getLines
      .filter(line => List("Completed").contains(line.split(",")(4)))
      .toList
      .sortBy(line=>line.split(",")(2))
      .map(line=>(line.split(",")(0), line.split(",")(1)))
      .groupBy(trace=>trace._1)
      .mapValues(traces=>traces.map(trace=>trace._2))
      .take(numOfTraces)
      .toList

    bufferedSource.close
    traces
  }

  /**
    * We assume that the events list contains no duplicates and they are sorted
    * If the events are A,B,C,D,E then pairs for computation are
    * AA, AB AC AD
    * BB BC BD
    * AC AD
    * CD
    * @param events
    * @return
    */
  def constructPairsForComputationFromEvents(events: List[String]): List[Pair] = {
    for {
      (x, idxX) <- events.zipWithIndex
      (y, idxY) <- events.zipWithIndex
      if (idxX == idxY || idxX < idxY)
    } yield new Pair(x,y)
  }

  /**
    * Not needed. Just left there in case of future use
    * Construct pairs for computation from a trace, which may contains duplicate events
    * @param trace
    * @return
    */
  def constructPairsForComputationFromTrace(trace: List[String]): List[String] = {
    val traceWithNoDuplicates = trace.toSet
    var tempTrace = traceWithNoDuplicates.toList
    var pairs = new ListBuffer[String]()

    for( i <- 0 to traceWithNoDuplicates.toList.size-1) {
      for( j <- 0 to tempTrace.length-1) {
        val tuple2 = traceWithNoDuplicates.toList(i)+tempTrace(j)
        pairs = pairs += tuple2
      }
      tempTrace = tempTrace.tail
    }

    return pairs.toList
  }

  /**
    * Filter initial trace set. Keep only unique traces and not duplicates.
    * Also, filter out those traces, which have frequency less than percentage variable.
    * @param tracesDS
    * @param percentage
    * @return
    */
  def filterTraces(tracesDS: List[(String, List[String])], percentage: Float): List[(String, List[String])] = {
    val initNumberOfTraces = tracesDS.length
    println("Initial number of traces = " + initNumberOfTraces)

    val tracesToInspect = tracesDS
      .groupBy(trace=>trace._2)
      .mapValues(_.size)
      .toList
      .filter(trace => (trace._2.toFloat / initNumberOfTraces) > (percentage/100) )
      .map(trace=>("xxx", trace._1))  //TODO trace id is useless from now on. Delete this from all implementation

    println("Number of traces to inspect = " + tracesToInspect.length)
    tracesToInspect
  }
}
