package misc

import misc.Directionality.Directionality
import misc.Relation.Relation

/**
  * Directionality means if the current pair is a direct or inverse pair
  * Relation means one of main log relations
  * @param pairNotation
  */
class PairNotation(var pairNotation:(Directionality, Relation)) {

  def getDirectionality(): Directionality = {
    return pairNotation._1
  }

  def getRelation(): Relation = {
    return pairNotation._2
  }

  override def toString = s"PairNotation($getDirectionality, $getRelation)"
}
