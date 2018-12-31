package steps

import misc.CausalGroup

/**
  * extract_2 is deprecated by extract, because it was necessary to reduce the memory amount in order to run it
  * in a real dataset.
  * With the extract solution it is not necessary to compute all possible pair combinations and save them on memory.
  * @param causalGroups
  */
class FindMaximalPairs(val causalGroups: List[CausalGroup[String]]) {

  val causalGroupsList = causalGroups
    .toSet

  /**
    * From all causal groups, keep only the maximal. Example from
    * ({a},{b}) and ({a}, {b,c}) keep only ({a}, {b,c})
    * @return
    */
  def extract(): List[CausalGroup[String]] = {
    return causalGroupsList
      .filter(x=>toBeRetained(x))
      .toList
  }

  def toBeRetained(toCheck: CausalGroup[String]): Boolean = {
    !causalGroupsList.exists(x => x != toCheck && isSubsetOf(toCheck, x))
  }

  /**
    * If true, the group2 must be retained
    * @param group1
    * @param group2
    * @return
    */
  def isSubsetOf(group1 : (CausalGroup[String]), group2 : CausalGroup[String]) : Boolean = {
    return group1.getFirstGroup().subsetOf(group2.getFirstGroup()) && group1.getSecondGroup().subsetOf(group2.getSecondGroup())
  }

}
