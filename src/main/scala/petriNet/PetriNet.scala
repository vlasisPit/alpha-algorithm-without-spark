package petriNet

import petriNet.flow.Edge
import petriNet.state.Places

class PetriNet (val places: Places, val events: List[String], val edges: List[Edge]) {

  def getPlaces(): Places = {
    return places
  }

  def getEvents(): List[String] = {
    return events
  }

  def getEdges() : List[Edge] = {
    return edges
  }

  override def toString = s"PetriNet(places = $getPlaces, events = $getEvents, edges = $getEdges)"
}
