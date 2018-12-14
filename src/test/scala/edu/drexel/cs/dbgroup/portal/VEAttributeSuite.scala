package edu.drexel.cs.dbgroup.portal

import java.sql.Date

import org.scalatest._

class VEAttributeSuite extends FunSuite with BeforeAndAfter{

  test("add, size"){
    var ve = new VEAttribute()
    ve.add("id", 1)
    assert(ve.size===1)
    ve.add("name", "mike")
    assert(ve.size===2)
    var dob = new Date(2012,12,1)
    ve.add("dob", dob)
    assert(ve.size===3)
  }

  test("exists"){
    var ve = new VEAttribute()
    ve.add("id", 1)
    ve.add("name", "mike")
    var dob = new Date(2012,12,1)
    ve.add("dob", dob)

    assert(ve.exists("id")===true)
    assert(ve.exists("name")===true)
    assert(ve.exists("dob")===true)
    assert(ve.exists("address")===false)
    assert(ve.exists("height")===false)
  }


  test("drop"){
    var ve = new VEAttribute()
    ve.add("id", 1)
    ve.add("name", "mike")
    var dob = new Date(2012,12,1)
    ve.add("dob", dob)

    //dropping dob
    ve.drop("dob")
    assert(ve.exists("dob")=== false);
    assert(ve.size === 2);

    //dropping name
    ve.drop("name")
    assert(ve.exists("name")=== false);
    assert(ve.size === 1);

    //id should still exist
    assert(ve.exists("id")=== true);
  }

  test("apply"){
    var ve = new VEAttribute()
    ve.add("id", 1)
    ve.add("name", "mike")
    var dob = new Date(2012,12,1)
    ve.add("dob", dob)

    assert(ve.apply("id")=== 1);
    assert(ve.apply("name")=== "mike");
    assert(ve.apply("dob")=== dob);
  }

  test("++"){
    var ve = new VEAttribute()
    ve.add("id", 1)
    ve.add("name", "mike")

    var ve2 = new VEAttribute()
    ve2.add("address", "Hartford")
    var dob = new Date(2012,12,1)
    ve2.add("dob", dob)

    ve ++ ve2

    assert(ve.apply("id")=== 1);
    assert(ve.apply("name")=== "mike");
    assert(ve.apply("address")=== "Hartford");
    assert(ve.apply("dob")=== dob);
  }

  test("keys"){
    var ve = new VEAttribute()
    ve.add("id", 1)
    ve.add("name", "mike")
    var dob = new Date(2012,12,1)
    ve.add("dob", dob)

    //sorted keys
    val keys = ve.keys.toArray.sorted;
    assert(keys.size === 3);
    assert(keys(0) === "dob");
    assert(keys(1) === "id");
    assert(keys(2) === "name");
  }



}
