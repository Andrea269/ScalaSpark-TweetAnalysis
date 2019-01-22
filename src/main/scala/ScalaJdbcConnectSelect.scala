
import java.sql.{Connection, DriverManager}

/**
  * A Scala JDBC connection example by Alvin Alexander,
  * http://alvinalexander.com
  */
object ScalaJdbcConnectSelect {
  val jdbcHostname = "localhost"
  val jdbcPort = 3306
  val jdbcDatabase = "mysql"

  // Create the JDBC URL without passing in the user and password parameters.

  val driver = "com.mysql.jdbc.Driver"
  val url = s"jdbc:mysql://$jdbcHostname:$jdbcPort/$jdbcDatabase"
  val username = "root"
  val password = ""

  def main(args: Array[String]) {
    // connect to the database named "mysql" on the localhost

    // there's probably a better way to do this
    var connection:Connection = null

    try {
      // make the connection
      Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)

      // create the statement, and run the select query
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("SELECT user FROM user")
      while ( resultSet.next() ) {
        val user = resultSet.getString("user")
        println("host, user = " + user)
      }
    } catch {
      case e: Throwable => e.printStackTrace()
    }
    connection.close()
  }

}
