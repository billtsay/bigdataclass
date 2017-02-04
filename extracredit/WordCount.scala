

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object WordCount {
  def main(args: Array[String]) {

    var conf = new SparkConf().setAppName("Spark Count")

    // for local test without using spark-submit, I need to set up master.
    if (args.length > 1)
      conf.setMaster("local[*]")

    val sc = new SparkContext(conf)

    // read in text file into lines
    val lines = sc.textFile(args(0))

    // split each line into words, the separator of words in a sentence is space.
    var words = lines.flatMap(_.split(" "))

    // count the occurrence of each word
    val counts = words.map((_, 1)).reduceByKey(_ + _)

    counts.foreach(println);
  }
}
