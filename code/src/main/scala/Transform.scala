import java.util.Calendar

object Transform {
  ///////////////functions for products//////////////////
  val lower:String=>String=str=>str.toLowerCase
  val discount:Int => Int=i=>i-i*10/100
  val check:Boolean => Int = i => if(i == true)  1 else 0
  var counter:Int=0
  val count:Unit=>Int = output => {
    counter=counter+1
    counter
  }

  ///////////////functions for customers/////////////////
  val plus1000: Int => Int = i => i+1000
  val capitalize: String => String = str => str.capitalize
  val mask: String => String = str => {
    val r = new scala.util.Random
    val sb = new StringBuilder
    for (_ <- 1 to str.length) {
      var char = r.nextPrintableChar()
      while(!char.isLetter){
        char = r.nextPrintableChar()
      }
      sb.append(char)
    }
    sb.toString
  }
  val pos: Int => Int = i => i.abs

  /////////////pass the column number and get the functions to apply on it///////////
  val getFunsToApply: (Map[String, Map[String, String]], Int) => List[String] = (metaMap, i) => {
    val keys = metaMap.keys.toList
    metaMap(keys(i))("funsToApply").split(',').toList
  }

  //////////////apply the functions on each cell/////////////////
  val applyFuns: (String, Map[String, Map[String, String]]) => String = (input, metaMap) => {
    val values = input.split(',').toList
    values.zipWithIndex.map {
      case(value, i) =>
        var result = value
        getFunsToApply(metaMap, i).foreach{ function => function match {
          case "plus1000" => result = plus1000(result.toInt).toString
          case "capitalize" => result = capitalize(result)
          case "mask" => result = mask(result)
          case "pos" => result = pos(result.replace('\uFEFF', ' ').trim.
            replace("ï»¿", "").toInt).toString
          case "lower" => result = lower(result)
          case "discount" => result = discount(result.toInt).toString
          case "check" => result = check(result.toBoolean).toString
          case "count" => result = count().toString
          case _ =>
            Load.insertlogs(s"function $function isn't implemented")
        }
      }
      if(metaMap(metaMap.keys.toList(i))("datatype").contains("varchar")){
        result="'"+result+"'" //single quotes around strings for SQL insertion
      }
      result
    }.reduce((s1, s2) => s1+","+s2)
  }

  val transform: (List[String], Map[String, Map[String, String]]) => List[String] = (lines, metaMap) => {
    lines.zipWithIndex.map{ case (line,i) =>
      Load.insertlogs(s"Successfully transformed line $i")
      applyFuns(line, metaMap)
    }
  }
}