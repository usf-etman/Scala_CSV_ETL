import java.sql.{Connection, DriverManager}
import java.util.{Calendar, Date}

object Load {
  ///////////////Database connection/////////////////
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/proddb"
  val username = "postgres"
  val password = "postgre$ql"
  Class.forName(driver)
  val connection: Option[Connection] = try {
    Some(DriverManager.getConnection(url, username, password))
  } catch {
    case _: Exception =>
      println("Can't connect to database")
      None
  }

  ////////////////pass column number and get it's datatype from metadata///////////////
  val getDataTypes: (Map[String, Map[String, String]], Int) => String = (metaMap, i) => {
    val keys = metaMap.keys.toList
    metaMap(keys(i))("datatypes")
  }

  ////////////////create table using metadata/////////////
  val createTable: (String, Map[String, Map[String, String]], String) => Unit = (appendOrWrite,metadata, tablename) => {
    val sb = new StringBuilder
    appendOrWrite match{
      case "append" => sb.append(s"CREATE TABLE IF NOT EXISTS ${tablename}(")
        Load.insertlogs("table is appended")
      case "write" => sb.append(s"DROP TABLE IF EXISTS ${tablename};CREATE TABLE ${tablename}(")
        Load.insertlogs("table is created")
    }
    metadata.keys.toList.foreach(key => {
      sb.append(key+" "+metadata(key)("datatype")+",")
    })
    sb.deleteCharAt(sb.toString.length -1).append(");")
    connection.get.createStatement.executeUpdate(sb.toString())
    Load.insertlogs("table is created")
  }

  /////////////insert data into database//////////////
  val insertData: (List[String],String) => Unit =(data,tablename)=> {
    data.foreach{line => {
      //val value:String
      val sql = s"""
                   |INSERT INTO $tablename VALUES(
                   |${line}
                   |)
                   |""".stripMargin
      connection.get.createStatement.executeUpdate(sql)
    }}
  }

  ///////////////function that takes a message to log////////////////
  val insertlogs: (String) => Unit = (word)=>{
    val sql = s"""
                 |INSERT INTO Logs VALUES(
                 |'${Calendar.getInstance().getTime()}','$word'
                 |)
                 |""".stripMargin
    connection.get.createStatement.executeUpdate(sql)
  }
}