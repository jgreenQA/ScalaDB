import slick.driver.MySQLDriver.api._

class People(tag: Tag) extends Table[(Int, String, String, Int, String, String, String)](tag, "PEOPLE"){
  def id = column[Int]("PER_ID", O.PrimaryKey, O.AutoInc)
  def fName = column[String]("PER_FNAME")
  def lName = column[String]("PER_LNAME")
  def age = column[Int]("PER_AGE")
  def house = column[String]("PER_HOUSE")
  def street = column[String]("PER_STREET")
  def city = column[String]("PER_CITY")
  def * = (id, fName, lName, age, house, street, city)
}
