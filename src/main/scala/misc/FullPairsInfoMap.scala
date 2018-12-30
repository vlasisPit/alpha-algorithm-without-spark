package misc

/**
  * Contains full info about a pair, both for direct and inverse direction
  * @param map
  */
class FullPairsInfoMap(var map: Map[Pair, (PairNotation, PairNotation)]) {

  def getPairsMap(): Map[Pair, (PairNotation, PairNotation)] = {
    return map
  }
}
