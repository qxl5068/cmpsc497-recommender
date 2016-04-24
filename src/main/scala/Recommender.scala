/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable.ListBuffer

object Recommender {
  def main(args: Array[String]) {
    val logFile = "/home/liang/Workspace/ydata-ymusic-user-artist-ratings-v1_0.txt" // Should be some file on your system
    val conf = new SparkConf().setAppName("Recommender")
      .set("spark.driver.memory", "6g")
      .set("spark.executor.memory", "6g")
    val sc = new SparkContext(conf)
    val ratings = sc.textFile(logFile,3)
    val names = sc.textFile("/home/liang/Workspace/ydata-ymusic-artist-names-v1_0.txt",3)

    val nameMap = names.map{line =>
      val fields = line.split("\t")
      (fields(0).toInt, fields(1))
    }.collect().groupBy(_._1).map{ case (k,v) => (k, v.map(_._2))}

    val userVec = ratings.map { line =>
      val fields = line.split("\t")
      (fields(0).toInt, (fields(1).toInt, fields(2).toDouble))
    }.groupByKey(numPartitions = 12)

    val itemIds = ratings.map{ line =>
      val fields = line.split("\t")
      fields(1).toInt
    }.distinct().sortBy(x => x).collect()

    val coocurance = userVec.values.map{ x=>
      val tmp = x.map(xs => xs._1).toList
      val combination_list = ListBuffer.empty[(Int, Int)]

      for (i <- 0 until tmp.length){
        for (j <- 0 until tmp.length){
          combination_list += ((tmp(i), tmp(j)))
        }
      }

      combination_list
    }.flatMap(x => x)
      .map(x => (x,1))
      .reduceByKey(_ + _, numPartitions = 12)
      .collect()
      .groupBy(_._1).map{ case (k,v) => (k, v.map(_._2))}


    val coMatrix = List.tabulate(itemIds.length, itemIds.length){(i,j) =>
      try{
        coocurance(itemIds(i),itemIds(j)).toList(0)
      } catch{
        case e:NoSuchElementException => 0
      }
    }

    val recommend = userVec.map{x =>
      val itemMap = x._2.groupBy(_._1).map{ case (k,v) => (k, v.map(_._2))}
      val fullVec = List.tabulate(itemIds.length){i =>

        try {
          itemMap(itemIds(i)).toList(0)
        } catch {
          case e: NoSuchElementException => 0
        }
      }

      val recommendVec = List.tabulate(itemIds.length){ i =>
        ((coMatrix(i) zip fullVec).map{ case (x,y) => x * y} :\0.0) {_ + _}
      }

      (x._1, recommendVec)
    }.sortBy(x => x._1)

    recommend.saveAsTextFile("output")

    val userCount = userVec.count()
    val itemCount = itemIds.length

    println("Reading ratings from %d users on %d items".format(userCount,itemCount))

    val userItems = recommend.first()._2.zip(itemIds.toList).sortBy(x => -x._1)

    println("Recommendation for user 1 is:")
    for (i <- 0 until 5){
      println("%s\t%f".format(nameMap(userItems(i)._2)(0), userItems(i)._1))
    }






  }
}
