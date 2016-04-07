package template.template

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Q1A5 {
  def main(args: Array[String]) {
     val conf = new SparkConf().setAppName("Simple Application")
     val sc = new SparkContext(conf)
     sc.setLogLevel("WARN")
     val businessFile = sc.textFile("hdfs:///yelpdatafall/business/business.csv")
     val reviewFile = sc.textFile("hdfs:///yelpdatafall/review/review.csv")
     val outputFile = "hdfs:///user/axv143730/assignment5/spark_jar/q1/output2" 
     val mapResult1 = businessFile.map(x => x.split("\\^")).map(x => (x(0),x(1)+"\\^"+x(2)))
     val mapResult2 = reviewFile.map(x => x.split("\\^")).map(x => (x(2),x(3).toDouble))
     val reduceResult1 = mapResult2.mapValues((_, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues{ case (sum, count) => (1.0 * sum)/count}.collectAsMap()
     val topNbusinessId =  sc.parallelize(reduceResult1.toList).sortBy(_._2,false).take(10)
     val joinResult = mapResult1.join(sc.parallelize(topNbusinessId))
     val result = sc.parallelize(joinResult.collect())
     result.coalesce(1,true).saveAsTextFile(outputFile)
  }
}