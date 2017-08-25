import slick.driver.MySQLDriver.api._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

object Main extends App {
  val db = Database.forConfig("TestDatabase")
  val peopleTable = TableQuery[People]
  val dropPeopleCmd = DBIO.seq(peopleTable.schema.drop)
  val initPeopleCmd = DBIO.seq(peopleTable.schema.create)

  dropDB

  def dropDB = {
    val dropFuture = Future {
      db.run(dropPeopleCmd)
    }
    Await.result(dropFuture, Duration.Inf).andThen {
      case Success(_) => initialisePeople
      case Failure(error) =>
        println(s"Dropping the table failed due to: ${error.getMessage}")
        initialisePeople
    }
  }

  def initialisePeople = {
    val setupFuture = Future {
      db.run(initPeopleCmd)
    }
    Await.result(setupFuture, Duration.Inf).andThen {
      case Success(_) => runQuery
      case Failure(error) => println(s"Initialising the table failed due to: ${error.getMessage}")
    }
  }

  def runQuery = {
    val insertPeople = Future {
      val query = peopleTable ++= Seq(
        (10, "Tim", "Wood", 36, "1 Test St"),
        (20, "Jack", "Brown", 24, "2 Test St"),
        (30, "John", "Brown", 48, "3 Test St"),
        (40, "Jack", "Wood", 36, "4 Test St"),
        (50, "Tim", "Brown", 24, "5 Test St"),
        (60, "Jack", "Brown", 48, "6 Test St"),
        (70, "Jack", "Brown", 48, "6 Test St"),
        (80, "John", "Brown", 48, "6 Test St"),
        (90, "John", "Brown", 48, "6 Test St"),
        (100, "Jack", "Brown", 48, "6 Test St")
      )
      println(query.statements.head)
      db.run(query)
    }
    Await.result(insertPeople, Duration.Inf).andThen {
      case Success(_) => updatePeople(1, "Jack", "Wood", 46, "4 Test St")
      case Failure(error) => println(s"Inserting people failed due to: ${error.getMessage}")
    }
  }

  def listPeople = {
    val queryFuture = Future {
      db.run(peopleTable.result).map(_.foreach {
        case (id, fName, lName, age, address) => println(s"$id $fName $lName $age $address")
      })
    }
    Await.result(queryFuture, Duration.Inf).andThen {
      case Success(_) => //db.close()
      case Failure(error) => println(s"Listing people failed due to: ${error.getMessage}")
    }
  }

  def updatePeople(searchId: Int, fname: String, lname: String, age: Int, address: String) = {
    listPeople

    val updateFuture = Future {
      val query = peopleTable.filter(_  .id === searchId).update(searchId, fname, lname, age, address)
      println(query.statements.head)
      db.run(query)
    }
    Await.result(updateFuture, Duration.Inf).andThen {
      case Success(_) => deletePeople(1)
      case Failure(error) => println(s"Updating person $searchId failed due to: ${error.getMessage}")
    }
  }

  def deletePeople(searchId: Int) = {
    listPeople

    val deleteFuture = Future {
      val query = peopleTable.filter(_.id === searchId).delete
      println(query.statements.head)
      db.run(query)
    }
    Await.result(deleteFuture, Duration.Inf).andThen {
      case Success(_) => countPeople
      case Failure(error) => println(s"Deleting person $searchId failed due to: ${error.getMessage}")
    }
  }

  def searchPeople(searchId: Int) = {
    listPeople

    val queryFuture = Future {
      db.run(peopleTable.result).map(_.foreach {
        case (id, fName, lName, age, address) if id == searchId => println(s"$id $fName $lName $age $address")
      })
    }
    Await.result(queryFuture, Duration.Inf).andThen {
      case Success(_) => listPeople
      case Failure(error) => println(s"Searching people failed due to: ${error.getMessage}")
    }
  }

  def countPeople = {
    listPeople

    val countFuture = Future {
      val query = peopleTable.countDistinct.result
      println(query.statements.head)
      db.run(query)
    }
    Await.result(countFuture, Duration.Inf).andThen {
      case Success(res) =>
        println(res)
        avgAgePeople
      case Failure(error) => println(s"Counting people failed due to: ${error.getMessage}")
    }
  }

  def avgAgePeople = {
    listPeople

    val avgFuture = Future {
      val query = peopleTable.map(_.age).avg.result
      println(query.statements.head)
      db.run(query)
    }
    Await.result(avgFuture, Duration.Inf).andThen {
      case Success(res) =>
        println(res)
        commonFnamePeople
      case Failure(error) => println(s"Selecting people failed due to: ${error.getMessage}")
    }
  }

  def commonFnamePeople = {
    listPeople

    val commonFnameFuture = Future {
      val query = peopleTable.groupBy(_.fName)
        .map{ case (fname, group) => (fname, group.map(_.fName).length) }
        .sortBy(_._2.desc)
        .result.head

      println(query.statements.head)
      db.run(query)
    }
    Await.result(commonFnameFuture, Duration.Inf).andThen {
      case Success(res) =>
        println(res)
        commonLnamePeople
      case Failure(error) => println(s"Selecting people failed due to: ${error.getMessage}")
    }
  }

  def commonLnamePeople = {
    listPeople

    val commonLnameFuture = Future {
      val query = peopleTable.groupBy(_.lName)
        .map{ case (lName, group) => (lName, group.map(_.lName).length) }
        .sortBy(_._2.desc)
        .result.head

      println(query.statements.head)
      db.run(query)
    }
    Await.result(commonLnameFuture, Duration.Inf).andThen {
      case Success(res) =>
        println(res)
        commonAddressPeople
      case Failure(error) => println(s"Selecting people failed due to: ${error.getMessage}")
    }
  }

  def commonAddressPeople = {
    listPeople

    val commonAddressFuture = Future {
      val query = peopleTable.groupBy(p => p.address)
        .map{ case (address, group) => (address, group.map(_.address).length) }
        .sortBy(_._2.desc)
        .result.head

      println(query.statements.head)
      db.run(query)
    }
    Await.result(commonAddressFuture, Duration.Inf).andThen {
      case Success(res) => println(res)
      case Failure(error) => println(s"Selecting people failed due to: ${error.getMessage}")
    }
  }
}