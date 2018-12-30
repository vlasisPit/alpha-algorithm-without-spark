package misc

class PairInfo(var pairInfo:(Pair, PairNotation)) {
  def getPairName(): Pair = {
    return pairInfo._1
  }

  def getPairNotation(): PairNotation = {
    return pairInfo._2
  }

  override def toString = s"PairInfo($getPairName, $getPairNotation)"
}
