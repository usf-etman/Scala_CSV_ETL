import java.io.File
import java.util.Calendar
object ETL {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Usage: java -jar ETL.jar <table directory path> <metadata file path>")
    }
    else {
      val tablePath = args(0)
      val metaFile = args(1)
      try {
        val inputData: List[String] = Extract.getTable(Extract.getListOfFiles(tablePath))
        val tableName = tablePath.split('/').last
        println(s"Reading table $tableName ...")
        Load.insertlogs(s"Reading table $tableName")

        println(s"Read ${inputData.length} rows from table $tableName \n")
        Load.insertlogs(s"Read ${inputData.length} rows from table $tableName")

        val metaDict = Extract.metaDict(Extract.getMetaData(metaFile)._2)
        val appendOrWrite: String = Extract.getMetaData(metaFile)._1
        val outputData: List[String] = Transform.transform(inputData, metaDict)
        println(s"\nSuccessfully transformed ${inputData.length} rows\n")
        appendOrWrite match {
          case "append" => println(s"Appending data to table $tableName")
          case "write" => println(s"Overwriting table $tableName")
        }
        Load.createTable(appendOrWrite, metaDict, tableName)
        Load.insertData(outputData, tableName)
        println("Finished ETL Successfully!")
      }
      catch {
        case e: NullPointerException =>
          println("Table not found, Available tables are:")
          new File(tablePath+"\\..").listFiles.foreach(path => {
            println(path.toString.split('\\').toList.last)
          })
          System.exit(0)
      }
    }
  }
}