package template.template

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Q4A5 {
  def main(args: Array[String]) {
     val conf = new SparkConf().setAppName("Simple Application")
     val sc = new SparkContext(conf)
     sc.setLogLevel("WARN")
     val userFile = sc.textFile("hdfs:///yelpdatafall/user/user.csv")
     val reviewFile = sc.textFile("hdfs:///yelpdatafall/review/review.csv")
     val outputFile = "hdfs:///user/axv143730/assignment5/spark_jar/q4/output" 
     val mapResult1 = userFile.map(x => x.split("\\^")).map(x => (x(0),x(1)))
     val mapResult2 = reviewFile.map(x => x.split("\\^")).map(x => (x(1),x(3).toDouble))
     val reduceResult1 = mapResult2.mapValues((_, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues{ case (sum, count) => (1.0 * sum)/count}.collectAsMap()
     val topNUsers =  sc.parallelize(reduceResult1.toList).sortBy(_._2,false).take(10)
     val joinResult = mapResult1.join(sc.parallelize(topNUsers.toList))
     val result = sc.parallelize(joinResult.collect())
     result.coalesce(1,true).saveAsTextFile(outputFile)
  }
}