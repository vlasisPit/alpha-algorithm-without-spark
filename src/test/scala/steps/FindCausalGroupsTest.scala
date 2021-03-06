package steps

import misc.{CausalGroup, Pair, Relation}
import org.scalatest.{BeforeAndAfter, FunSuite}

/**
  * Unit tests to check step 4 of Alpha Algorithm (extract causal groups)
  * Unit tests above are not true unit tests, because a true unit test means you have complete control over every component
  * in the test. There can be no interaction with databases, REST calls, file systems, or even the system clock; everything
  * has to be "doubled" (e.g. mocked, stubbed, etc).
  * In this test, SparkSession is created, but this is necessary to test the proper functionality of dataset operations like
  * groupByKey and mapGroups
  */
class FindCausalGroupsTest extends FunSuite with BeforeAndAfter {

  test("Check FindCausalGroups correct functionality - Log 1") {
    val logRelations = List(
      (new Pair("E", "E"), Relation.NEVER_FOLLOW.toString),
      (new Pair("A", "D"), Relation.NEVER_FOLLOW.toString),
      (new Pair("A", "A"), Relation.NEVER_FOLLOW.toString),
      (new Pair("B", "C"), Relation.PARALLELISM.toString),
      (new Pair("E", "D"), Relation.CAUSALITY.toString),
      (new Pair("B", "D"), Relation.CAUSALITY.toString),
      (new Pair("A", "E"), Relation.CAUSALITY.toString),
      (new Pair("D", "D"), Relation.NEVER_FOLLOW.toString),
      (new Pair("A", "B"), Relation.CAUSALITY.toString),
      (new Pair("C", "C"), Relation.NEVER_FOLLOW.toString),
      (new Pair("C", "E"), Relation.NEVER_FOLLOW.toString),
      (new Pair("B", "E"), Relation.NEVER_FOLLOW.toString),
      (new Pair("B", "B"), Relation.NEVER_FOLLOW.toString),
      (new Pair("A", "C"), Relation.CAUSALITY.toString),
      (new Pair("C", "D"), Relation.CAUSALITY.toString))

    val findCausalGroups: FindCausalGroups = new FindCausalGroups(logRelations)
    val causalGroups = findCausalGroups.extractCausalGroups()

    assert(causalGroups.contains(new CausalGroup[String](Set("E"), Set("D"))))
    assert(causalGroups.contains(new CausalGroup[String](Set("B"), Set("D"))))
    assert(causalGroups.contains(new CausalGroup[String](Set("A"), Set("E"))))
    assert(causalGroups.contains(new CausalGroup[String](Set("A"), Set("B"))))
    assert(causalGroups.contains(new CausalGroup[String](Set("A"), Set("C"))))
    assert(causalGroups.contains(new CausalGroup[String](Set("C"), Set("D"))))
    assert(causalGroups.contains(new CausalGroup[String](Set("A"), Set("B", "E"))))
    assert(causalGroups.contains(new CausalGroup[String](Set("A"), Set("C", "E"))))
    assert(causalGroups.contains(new CausalGroup[String](Set("B", "E"), Set("D"))))
    assert(causalGroups.contains(new CausalGroup[String](Set("C", "E"), Set("D"))))
    assert(causalGroups.size==10)
  }

   test("Check FindCausalGroups correct functionality - Log 2") {
     val logRelations = List(
       (new Pair("A", "D"), Relation.NEVER_FOLLOW.toString),
       (new Pair("B", "D"), Relation.CAUSALITY.toString),
       (new Pair("B", "C"), Relation.NEVER_FOLLOW.toString),
       (new Pair("A", "A"), Relation.NEVER_FOLLOW.toString),
       (new Pair("C", "C"), Relation.NEVER_FOLLOW.toString),
       (new Pair("A", "C"), Relation.CAUSALITY.toString),
       (new Pair("D", "D"), Relation.NEVER_FOLLOW.toString),
       (new Pair("A", "B"), Relation.CAUSALITY.toString),
       (new Pair("B", "B"), Relation.NEVER_FOLLOW.toString),
       (new Pair("C", "D"), Relation.CAUSALITY.toString))

     val findCausalGroups: FindCausalGroups = new FindCausalGroups(logRelations)
     val causalGroups = findCausalGroups.extractCausalGroups()

     assert(causalGroups.contains(new CausalGroup[String](Set("A"), Set("B"))))
     assert(causalGroups.contains(new CausalGroup[String](Set("A"), Set("C"))))
     assert(causalGroups.contains(new CausalGroup[String](Set("B"), Set("D"))))
     assert(causalGroups.contains(new CausalGroup[String](Set("C"), Set("D"))))
     assert(causalGroups.contains(new CausalGroup[String](Set("A"), Set("B", "C"))))
     assert(causalGroups.contains(new CausalGroup[String](Set("B", "C"), Set("D"))))

     //false assertions
     assert(!causalGroups.contains(new CausalGroup[String](Set("B"), Set("I"))))
     assert(!causalGroups.contains(new CausalGroup[String](Set("B", "R"), Set("D"))))

     assert(causalGroups.size==6)
   }

  test("Check FindCausalGroups correct functionality - Log 3") {
    val logRelations = List(
      (new Pair("A", "D"), Relation.NEVER_FOLLOW.toString),
      (new Pair("A", "E"), Relation.NEVER_FOLLOW.toString),
      (new Pair("A", "F"), Relation.NEVER_FOLLOW.toString),
      (new Pair("A", "G"), Relation.NEVER_FOLLOW.toString),
      (new Pair("A", "H"), Relation.NEVER_FOLLOW.toString),
      (new Pair("B", "C"), Relation.NEVER_FOLLOW.toString),
      (new Pair("B", "F"), Relation.NEVER_FOLLOW.toString),
      (new Pair("B", "G"), Relation.NEVER_FOLLOW.toString),
      (new Pair("B", "H"), Relation.NEVER_FOLLOW.toString),
      (new Pair("C", "D"), Relation.NEVER_FOLLOW.toString),
      (new Pair("C", "E"), Relation.NEVER_FOLLOW.toString),
      (new Pair("C", "F"), Relation.NEVER_FOLLOW.toString),
      (new Pair("C", "H"), Relation.NEVER_FOLLOW.toString),
      (new Pair("D", "C"), Relation.NEVER_FOLLOW.toString),
      (new Pair("D", "G"), Relation.NEVER_FOLLOW.toString),
      (new Pair("D", "H"), Relation.NEVER_FOLLOW.toString),
      (new Pair("E", "G"), Relation.NEVER_FOLLOW.toString),
      (new Pair("E", "H"), Relation.NEVER_FOLLOW.toString),
      (new Pair("F", "G"), Relation.NEVER_FOLLOW.toString),
      (new Pair("D", "E"), Relation.PARALLELISM.toString),
      (new Pair("A", "B"), Relation.CAUSALITY.toString),
      (new Pair("A", "C"), Relation.CAUSALITY.toString),
      (new Pair("B", "D"), Relation.CAUSALITY.toString),
      (new Pair("B", "E"), Relation.CAUSALITY.toString),
      (new Pair("C", "G"), Relation.CAUSALITY.toString),
      (new Pair("D", "F"), Relation.CAUSALITY.toString),
      (new Pair("E", "F"), Relation.CAUSALITY.toString),
      (new Pair("F", "H"), Relation.CAUSALITY.toString),
      (new Pair("G", "H"), Relation.CAUSALITY.toString))

    val findCausalGroups: FindCausalGroups = new FindCausalGroups(logRelations)
    val causalGroups = findCausalGroups.extractCausalGroups()

    assert(causalGroups.contains(new CausalGroup[String](Set("A"), Set("B"))))
    assert(causalGroups.contains(new CausalGroup[String](Set("A"), Set("C"))))
    assert(causalGroups.contains(new CausalGroup[String](Set("A"), Set("B","C"))))
    assert(causalGroups.contains(new CausalGroup[String](Set("B"), Set("D"))))
    assert(causalGroups.contains(new CausalGroup[String](Set("B"), Set("E"))))
    assert(causalGroups.contains(new CausalGroup[String](Set("C"), Set("G"))))
    assert(causalGroups.contains(new CausalGroup[String](Set("F","G"), Set("H"))))
    assert(causalGroups.contains(new CausalGroup[String](Set("D"), Set("F"))))
    assert(causalGroups.contains(new CausalGroup[String](Set("E"), Set("F"))))
    assert(causalGroups.contains(new CausalGroup[String](Set("F"), Set("H"))))
    assert(causalGroups.contains(new CausalGroup[String](Set("G"), Set("H"))))

    //false assertions
    assert(!causalGroups.contains(new CausalGroup[String](Set("B"), Set("I"))))
    assert(!causalGroups.contains(new CausalGroup[String](Set("B", "R"), Set("D"))))

    assert(causalGroups.size==11)
  }

  test("Check FindCausalGroups correct functionality - Log 4") {
    val logRelations = List(
      (new Pair("A", "C"), Relation.NEVER_FOLLOW.toString),
      (new Pair("A", "D"), Relation.NEVER_FOLLOW.toString),
      (new Pair("A", "E"), Relation.NEVER_FOLLOW.toString),
      (new Pair("B", "D"), Relation.NEVER_FOLLOW.toString),
      (new Pair("E", "D"), Relation.NEVER_FOLLOW.toString),
      (new Pair("A", "B"), Relation.CAUSALITY.toString),
      (new Pair("B", "C"), Relation.CAUSALITY.toString),
      (new Pair("C", "E"), Relation.CAUSALITY.toString),
      (new Pair("C", "D"), Relation.CAUSALITY.toString),
      (new Pair("E", "B"), Relation.CAUSALITY.toString))

    val findCausalGroups: FindCausalGroups = new FindCausalGroups(logRelations)
    val causalGroups = findCausalGroups.extractCausalGroups()

    assert(causalGroups.contains(new CausalGroup[String](Set("C"), Set("E"))))
    assert(causalGroups.contains(new CausalGroup[String](Set("C"), Set("D"))))
    assert(causalGroups.contains(new CausalGroup[String](Set("E"), Set("B"))))
    assert(causalGroups.contains(new CausalGroup[String](Set("A"), Set("B"))))
    assert(causalGroups.contains(new CausalGroup[String](Set("B"), Set("C"))))
    assert(causalGroups.contains(new CausalGroup[String](Set("A","E"), Set("B"))))
    assert(causalGroups.contains(new CausalGroup[String](Set("C"), Set("E","D"))))

    //false assertions
    assert(!causalGroups.contains(new CausalGroup[String](Set("B"), Set("I"))))
    assert(!causalGroups.contains(new CausalGroup[String](Set("B", "R"), Set("D"))))

    assert(causalGroups.size==7)
  }

  test("Check FindCausalGroups correct functionality - Log 5") {
    val logRelations = List(
      (new Pair("A", "C"), Relation.NEVER_FOLLOW.toString),
      (new Pair("A", "E"), Relation.NEVER_FOLLOW.toString),
      (new Pair("A", "F"), Relation.NEVER_FOLLOW.toString),
      (new Pair("A", "G"), Relation.NEVER_FOLLOW.toString),
      (new Pair("A", "H"), Relation.NEVER_FOLLOW.toString),
      (new Pair("B", "D"), Relation.NEVER_FOLLOW.toString),
      (new Pair("B", "E"), Relation.NEVER_FOLLOW.toString),
      (new Pair("B", "F"), Relation.NEVER_FOLLOW.toString),
      (new Pair("B", "G"), Relation.NEVER_FOLLOW.toString),
      (new Pair("B", "H"), Relation.NEVER_FOLLOW.toString),
      (new Pair("C", "D"), Relation.NEVER_FOLLOW.toString),
      (new Pair("C", "F"), Relation.NEVER_FOLLOW.toString),
      (new Pair("C", "G"), Relation.NEVER_FOLLOW.toString),
      (new Pair("C", "H"), Relation.NEVER_FOLLOW.toString),
      (new Pair("D", "B"), Relation.NEVER_FOLLOW.toString),
      (new Pair("D", "C"), Relation.NEVER_FOLLOW.toString),
      (new Pair("D", "F"), Relation.NEVER_FOLLOW.toString),
      (new Pair("D", "G"), Relation.NEVER_FOLLOW.toString),
      (new Pair("D", "H"), Relation.NEVER_FOLLOW.toString),
      (new Pair("E", "H"), Relation.NEVER_FOLLOW.toString),
      (new Pair("F", "G"), Relation.PARALLELISM.toString),
      (new Pair("A", "B"), Relation.CAUSALITY.toString),
      (new Pair("A", "D"), Relation.CAUSALITY.toString),
      (new Pair("B", "C"), Relation.CAUSALITY.toString),
      (new Pair("C", "E"), Relation.CAUSALITY.toString),
      (new Pair("D", "E"), Relation.CAUSALITY.toString),
      (new Pair("E", "F"), Relation.CAUSALITY.toString),
      (new Pair("E", "G"), Relation.CAUSALITY.toString),
      (new Pair("F", "H"), Relation.CAUSALITY.toString),
      (new Pair("G", "H"), Relation.CAUSALITY.toString)
      )

    val findCausalGroups: FindCausalGroups = new FindCausalGroups(logRelations)
    val causalGroups = findCausalGroups.extractCausalGroups()

    assert(causalGroups.contains(new CausalGroup[String](Set("A"), Set("B"))))
    assert(causalGroups.contains(new CausalGroup[String](Set("A"), Set("D"))))
    assert(causalGroups.contains(new CausalGroup[String](Set("A"), Set("B","D"))))
    assert(causalGroups.contains(new CausalGroup[String](Set("B"), Set("C"))))
    assert(causalGroups.contains(new CausalGroup[String](Set("C"), Set("E"))))
    assert(causalGroups.contains(new CausalGroup[String](Set("D"), Set("E"))))
    assert(causalGroups.contains(new CausalGroup[String](Set("C","D"), Set("E"))))
    assert(causalGroups.contains(new CausalGroup[String](Set("E"), Set("F"))))
    assert(causalGroups.contains(new CausalGroup[String](Set("E"), Set("G"))))
    assert(causalGroups.contains(new CausalGroup[String](Set("F"), Set("H"))))
    assert(causalGroups.contains(new CausalGroup[String](Set("G"), Set("H"))))

    //false assertions
    assert(!causalGroups.contains(new CausalGroup[String](Set("B"), Set("I"))))
    assert(!causalGroups.contains(new CausalGroup[String](Set("B", "R"), Set("D"))))

    assert(causalGroups.size==11)
  }

  test("Check if a NeverFollow Relation exists. All relations are never follow") {
    val logRelations = List(
      (new Pair("A", "D"), Relation.NEVER_FOLLOW.toString),
      (new Pair("B", "E"), Relation.NEVER_FOLLOW.toString),
      (new Pair("A", "A"), Relation.NEVER_FOLLOW.toString),
      (new Pair("C", "E"), Relation.NEVER_FOLLOW.toString),
      (new Pair("D", "D"), Relation.NEVER_FOLLOW.toString),
      (new Pair("B", "C"), Relation.NEVER_FOLLOW.toString))

    val groupEvents: Set[String] = Set("B", "C", "E")
    val findCausalGroups: FindCausalGroups = new FindCausalGroups(logRelations)

    val allRelationsAreNeverFollow = findCausalGroups.allRelationsAreNeverFollow(groupEvents)
    assert(allRelationsAreNeverFollow==true)
  }

  test("Check if a NeverFollow Relation exists. There is one PARALLELISM relation") {
    val logRelations = List(
      (new Pair("A", "D"), Relation.NEVER_FOLLOW.toString),
      (new Pair("B", "E"), Relation.PARALLELISM.toString),
      (new Pair("A", "A"), Relation.NEVER_FOLLOW.toString),
      (new Pair("C", "E"), Relation.NEVER_FOLLOW.toString),
      (new Pair("D", "D"), Relation.NEVER_FOLLOW.toString),
      (new Pair("B", "C"), Relation.NEVER_FOLLOW.toString))

    val groupEvents: Set[String] = Set("B", "C", "E")
    val findCausalGroups: FindCausalGroups = new FindCausalGroups(logRelations)

    val allRelationsAreNeverFollow = findCausalGroups.allRelationsAreNeverFollow(groupEvents)
    assert(allRelationsAreNeverFollow==false)
  }

  test("Check if a NeverFollow Relation exists. All relations are never follow for 4 event") {
    val logRelations = List(
      (new Pair("A", "B"), Relation.NEVER_FOLLOW.toString),
      (new Pair("A", "C"), Relation.NEVER_FOLLOW.toString),
      (new Pair("A", "D"), Relation.NEVER_FOLLOW.toString),
      (new Pair("B", "C"), Relation.NEVER_FOLLOW.toString),
      (new Pair("B", "D"), Relation.NEVER_FOLLOW.toString),
      (new Pair("C", "D"), Relation.NEVER_FOLLOW.toString))

    val groupEvents: Set[String] = Set("A", "B", "C", "D")
    val findCausalGroups: FindCausalGroups = new FindCausalGroups(logRelations)

    val allRelationsAreNeverFollow = findCausalGroups.allRelationsAreNeverFollow(groupEvents)
    assert(allRelationsAreNeverFollow==true)
  }

  /**
    * a # b,
    * c # d,
    * a->c,
    * a->d,
    * b->c,
    * b->d
    */
  test("2 groups with 2 places each") {
    val logRelations = List(
      (new Pair("A", "D"), Relation.CAUSALITY.toString),
      (new Pair("A", "C"), Relation.CAUSALITY.toString),
      (new Pair("B", "D"), Relation.CAUSALITY.toString),
      (new Pair("B", "C"), Relation.CAUSALITY.toString),
      (new Pair("A", "B"), Relation.NEVER_FOLLOW.toString),
      (new Pair("C", "D"), Relation.NEVER_FOLLOW.toString))

    val findCausalGroups: FindCausalGroups = new FindCausalGroups(logRelations)
    val causalGroups = findCausalGroups.extractCausalGroups()

    assert(causalGroups.contains(new CausalGroup[String](Set("A", "B"), Set("C", "D"))))
    assert(causalGroups.contains(new CausalGroup[String](Set("A", "B"), Set("C"))))
    assert(causalGroups.contains(new CausalGroup[String](Set("A", "B"), Set("D"))))

    assert(causalGroups.contains(new CausalGroup[String](Set("A"), Set("C", "D"))))
    assert(causalGroups.contains(new CausalGroup[String](Set("A"), Set("C"))))
    assert(causalGroups.contains(new CausalGroup[String](Set("A"), Set("D"))))

    assert(causalGroups.contains(new CausalGroup[String](Set("B"), Set("C", "D"))))
    assert(causalGroups.contains(new CausalGroup[String](Set("B"), Set("C"))))
    assert(causalGroups.contains(new CausalGroup[String](Set("B"), Set("D"))))

    assert(causalGroups.size==9)
  }

}
