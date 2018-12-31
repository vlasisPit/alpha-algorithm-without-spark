package steps

import misc.{CausalGroup, FullPairsInfoMap, Pair, PairInfo, PairNotation}
import petriNet.actions.FindEdges
import petriNet.flow.Edge
import petriNet.state.{Places, State}
import tools.TraceTools

class AlphaAlgorithmSteps {

  /**
    * Step 1 - Find all transitions / events, Sorted list of all event types
    * @param traces
    * @return
    */
  def getAllEvents(traces: List[(String, List[String])]) : List[String] = {
    return traces
      .map(x=>x._2)
      .flatMap(x=>x.toSet)
      .toSet
      .toList
      .sorted
  }

  /**
    * Step 2 - Construct a set with all start activities (Ti)
    * @param traces
    * @return
    */
  def getStartActivities(traces: List[(String, List[String])]) : Set[String] = {
    return traces.map(x=>x._2.head).toSet
  }

  /**
    * Step 3 - Construct a set with all final activities (To)
    * @param traces
    * @return
    */
  def getFinalActivities(traces: List[(String, List[String])]) : Set[String] = {
    return traces.map(x=>x._2.last).toSet
  }

  /**
    * Step 4 Calculate pairs - Footprint graph
    * construct a list of pair events for which computations must be made
    * @param traces
    * @param events
    * @return
    */
  def getFootprintGraph(traces: List[(String, List[String])], events: List[String]): List[(Pair, String)] = {
    val followRelation: FindFollowRelation = new FindFollowRelation()
    val findLogRelations: FindLogRelations = new FindLogRelations()
    val traceTools: TraceTools = new TraceTools()
    val pairsToExamine = traceTools.constructPairsForComputationFromEvents(events)

    /**
      * pairInfo is in the following form
      * AB,PairNotation(DIRECT, FOLLOW)
      * AB,PairNotation(INVERSE, FOLLOW)
      */
    val pairInfo = traces
      .map(traces => followRelation.findFollowRelation(traces, pairsToExamine))
      .map(x=>x.getPairsMap())
      .flatMap(map=>map.toSeq)  //map to collection of tuples
      .map(x=> List(new PairInfo((x._1, new PairNotation(x._2._1.pairNotation))), new PairInfo((x._1, new PairNotation(x._2._2.pairNotation)))))
      .flatMap(x=>x.toSeq)

    /**
      * relations in  the following form, Footprint graph
      * (FB,CAUSALITY)
      * (BB,NEVER_FOLLOW)
      * (AB,PARALLELISM)
      */
    val logRelations = pairInfo
      .groupBy(x=> x.getPairName())
      .mapValues(pairInformation=>pairInformation.map(pair=>(pair.getPairNotation())).toSet)
      .map(x=>findLogRelations.getDirectAndInverseFollowRelations(x))
      .map(x=>findLogRelations.extractFootPrintGraph(x._1, x._2, x._3))
      .toList

    return logRelations
  }

  /**
    * compute causal groups - Step 4
    * directCausalGroups are all causality relations because they are by default causal group
    * @param logRelations
    * @return
    */
  def getCausalGroups(logRelations: List[(Pair, String)]): List[CausalGroup[String]] = {
    val findCausalGroups: FindCausalGroups = new FindCausalGroups(logRelations)
    findCausalGroups.extractCausalGroups()
  }

  /**
    * Step 5 - compute only maximal groups
    * @param causalGroups
    * @return
    */
  def getMaximalGroups(causalGroups: List[CausalGroup[String]]): List[CausalGroup[String]] = {
    val findMaximalPairs: FindMaximalPairs = new FindMaximalPairs(causalGroups)
    findMaximalPairs.extract()
  }

  /**
    * step 6 - set of places/states
    * @param maximalGroups
    * @param startActivities
    * @param finalActivities
    * @return
    */
  def getPlaces(maximalGroups : List[CausalGroup[String]], startActivities : Set[String], finalActivities : Set[String]): Places = {
    val states = maximalGroups
      .map(x=> new State(x.getFirstGroup(), x.getSecondGroup()))

    val initialState = new State(Set.empty, startActivities)
    val finalState = new State(finalActivities, Set.empty)

    new Places(initialState, finalState, states)
  }

  /**
    * step 7 - set of arcs (flow)
    * @param places
    * @return
    */
  def getEdges(places: Places): List[Edge] = {
    val findEdges: FindEdges = new FindEdges(places)
    findEdges.find()
  }

}
