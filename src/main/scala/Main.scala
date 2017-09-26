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
        (10, "Tim", "Wood", 36, "1", "Testing Ln", "Testville"),
        (20, "Jack", "Brown", 24, "2", "Test St", "Testington"),
        (30, "John", "Brown", 48, "3", "Testing Ln", "Testville"),
        (40, "Jack", "Wood", 36, "4", "Test St", "Testington"),
        (50, "Tim", "Brown", 24, "5", "Test St", "Testington"),
        (60, "Jack", "Brown", 48, "6", "Testing Ln", "Testville"),
        (70, "Jack", "Brown", 48, "6", "Test St", "Testington"),
        (80, "John", "Brown", 48, "6", "Testing Ln", "Testville"),
        (90, "John", "Brown", 48, "6", "Test St", "Testington"),
        (100, "Jack", "Brown", 48, "6", "Test St", "Testington")
      )
      println(query.statements.head)
      db.run(query)
    }
    Await.result(insertPeople, Duration.Inf).andThen {
      case Success(_) => updatePeople(1, "Jack", "Wood", 46, "4", "Test St", "Testington")
      case Failure(error) => println(s"Inserting people failed due to: ${error.getMessage}")
    }
  }

  def listPeople = {
    val queryFuture = Future {
      db.run(peopleTable.result).map(_.foreach {
        case (id, fName, lName, age, house, street, city) => println(s"$id $fName $lName $age $house $street $city")
      })
    }
    Await.result(queryFuture, Duration.Inf).andThen {
      case Success(_) => //db.close()
      case Failure(error) => println(s"Listing people failed due to: ${error.getMessage}")
    }
  }

  def updatePeople(searchId: Int, fname: String, lname: String, age: Int, house: String,  street: String, city: String) = {
    listPeople

    val updateFuture = Future {
      val query = peopleTable.filter(_  .id === searchId).update(searchId, fname, lname, age, house, street, city)
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
        case (id, fName, lName, age, house, street, city) if id == searchId => println(s"$id $fName $lName $age $house $street $city")
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
        commonCityPeople
      case Failure(error) => println(s"Selecting people failed due to: ${error.getMessage}")
    }
  }

  def commonCityPeople = {
    listPeople

    val commonCityFuture = Future {
      val query = peopleTable.groupBy(p => p.city)
        .map{ case (city, group) => (city, group.map(_.city).length) }
        .sortBy(_._2.desc)
        .result.head

      println(query.statements.head)
      db.run(query)
    }
    Await.result(commonCityFuture, Duration.Inf).andThen {
      case Success(res) => println(res)
      case Failure(error) => println(s"Selecting people failed due to: ${error.getMessage}")
    }
  }
}