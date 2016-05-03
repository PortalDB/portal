package edu.drexel.cs.dbgroup.temporalgraph

import org.scalatest.{BeforeAndAfter, FunSuite}

class QuantificationSuite extends FunSuite with BeforeAndAfter {

  test("always") {
    val always = Always()
    assert(always.keep(1))
    assert(!always.keep(0.5))
    assert(!always.keep(0.1))
  }

  test("exists"){
    val exists = Exists()
    assert(exists.keep(1))
    assert(exists.keep(0.5))
    assert(exists.keep(0.1))
  }

  test("most"){
    val most = Most()
    assert(most.keep(1))
    assert(most.keep(0.6))
    assert(most.keep(0.51))
    assert(!most.keep(0.5))
    assert(!most.keep(0.1))
  }

  test("atleast"){
    val atleast50 = AtLeast(0.5)
    assert(atleast50.keep(1))
    assert(atleast50.keep(0.6))
    assert(atleast50.keep(0.51))
    assert(atleast50.keep(0.5))
    assert(!atleast50.keep(0.49))
    assert(!atleast50.keep(0.1))

    val atleast20 = AtLeast(0.2)
    assert(atleast20.keep(1))
    assert(atleast20.keep(0.6))
    assert(atleast20.keep(0.51))
    assert(atleast20.keep(0.5))
    assert(atleast20.keep(0.2))
    assert(!atleast20.keep(0.19))
    assert(!atleast20.keep(0.1))
  }
}
