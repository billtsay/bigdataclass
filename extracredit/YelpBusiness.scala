import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import scala.util.parsing.json._

object YelpBusiness {
  def main(args: Array[String]) {

    var conf = new SparkConf().setAppName("Spark Count")

    // for local test without using spark-submit, I need to set up master.
    if (args.length > 2)
      conf.setMaster("local[*]")

    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    
    // parse first and second arguments and register them to table name business and review each.
    val business = sqlContext.read.json(args(0))
    business.registerTempTable("business")
    val review = sqlContext.read.json(args(1))
    review.registerTempTable("review")
    
    val result = sqlContext.sql("SELECT distinct R.business_id, B.name AS BusinessName, B.stars AS StarRating, R.date AS MaxDate, R.user_id AS UserId, R.stars AS UserStarRating from review as R, business as B WHERE R.business_id=B.business_id ORDER BY MaxDate DESC" )

    result.show()
    
  }  
}