//Import necessary packages
import java.io.File
import scala.io.{BufferedSource, Source}
import org.apache.poi.ss.usermodel.{Sheet, WorkbookFactory}
import collection.JavaConversions._

object Extract {
  /////////////////get list of files from the table directory/////////////////
  val getListOfFiles: String => List[BufferedSource] = (dirPath: String) => {
    val listoffiles = new File(dirPath).listFiles.map(file => Source.fromFile(file.toString)).toList
    val l=listoffiles.length
    Load.insertlogs("numbers of files are "+l)
    listoffiles
  }

  /////////////////Read the content of files and concatenate them/////////////////
  val getTable: List[BufferedSource] => List[String] = (files: List[BufferedSource]) => {
    files.flatMap(file => {
      val lines = file.getLines.toList
      file.close()
      lines
    })//.reduce((l1, l2) => l1 ++ l2)

  }

  //////////////get if table is append or write & return metadata sheet//////////////
  val getMetaData: String => (String, Sheet) = (metaFile: String) => {
    val file = new File(metaFile)
    val sheet = WorkbookFactory.create(file).getSheetAt(0)
    val appendORwrite = sheet.getRow(0).getCell(3).getStringCellValue
    sheet.removeRow(sheet.getRow(0))
    (appendORwrite, sheet)
  }

  /////////////////build dictionary with column names as key/////////////////
  ////////////////and their data types and functions as value/////////////////
  // {
  //    column1: {
  //        "datatype": type
  //        "funsToApply": fn1,fn2,...
  //    }
  //    column2: { ...
  // }
  val metaDict: Sheet => Map[String, Map[String, String]] = (metadata: Sheet) => {
    metadata.map(row => {
      row.getCell(0).getStringCellValue -> {
        Map("datatype" -> row.getCell(1).getStringCellValue,
          "funsToApply" -> row.getCell(2).getStringCellValue)
      }
    }).toMap
  }
}