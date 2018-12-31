package alphaAlgorithm

import misc.{CausalGroup, Pair}
import petriNet.PetriNet
import petriNet.flow.Edge
import petriNet.state.Places
import steps.AlphaAlgorithmSteps
import tools.TraceTools

object AlphaAlgorithm {

  def main(args: Array[String]): Unit = {
    val traceTools: TraceTools = new TraceTools()
    val logPath = "src/main/resources/readDataFiltered.csv"
    val numOfTraces = 30
    val percentage : Float = 2.0f //delete trace occurrences which are less than 1% from all traces
    val readAll : Boolean = false
    val filtering : Boolean = true

    val traces : List[(String, List[String])] = traceTools.getTracesToInspect(logPath, numOfTraces, readAll, filtering, percentage)

    val petriNet: PetriNet = executeAlphaAlgorithm(traces)
    println(petriNet)
  }

  /**
    * Alpha algorithm execution consists of 8 steps.
    * The result is a PetriNet flow.
    * @param logPath
    * @return
    */
  def executeAlphaAlgorithm(traces : List[(String, List[String])]) : PetriNet = {

    val steps : AlphaAlgorithmSteps = new AlphaAlgorithmSteps()

    //Step 1 - Find all transitions / events, Sorted list of all event types
    val events = steps.getAllEvents(traces)

    //Step 2 - Construct a set with all start activities (Ti)
    val startActivities = steps.getStartActivities(traces)

    //Step 3 - Construct a set with all final activities (To)
    val finalActivities = steps.getFinalActivities(traces)

    //Step 4 - Footprint graph - Causal groups
    val logRelations : List[(Pair, String)] = steps.getFootprintGraph(traces, events)
    val causalGroups : List[CausalGroup[String]] = steps.getCausalGroups(logRelations)

    //Step 5 - compute only maximal groups
    val maximalGroups : List[CausalGroup[String]] = steps.getMaximalGroups(causalGroups)

    //step 6 - set of places/states
    val places : Places = steps.getPlaces(maximalGroups, startActivities, finalActivities)

    //step 7 - set of arcs (flow)
    val edges : List[Edge] = steps.getEdges(places)

    //step 8 - construct petri net
    return new PetriNet(places, events, edges)
  }

}
