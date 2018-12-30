package misc

object Relation extends Enumeration {
  type Relation = Value
  val FOLLOW,
      NOT_FOLLOW,
      CAUSALITY,
      PARALLELISM,
      NEVER_FOLLOW = Value
}
