package template.template

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Q3A5 {
  def main(args: Array[String]) {
     val conf = new SparkConf().setAppName("Simple Application")
     val sc = new SparkContext(conf)
     sc.setLogLevel("WARN")
     val businessFile = sc.textFile("hdfs:///yelpdatafall/business/business.csv")
     val reviewFile = sc.textFile("hdfs:///yelpdatafall/review/review.csv")
     val outputFile = "hdfs:///user/axv143730/assignment5/spark_jar/q3/output2" 
     val mapResult1 = businessFile.filter(x => x.toLowerCase().contains("stanford")).map(x => x.split("\\^")).map(x => (x(0),null))
     val mapResult2 = reviewFile.map(x => x.split("\\^")).map(x => (x(2),x(1)+"^"+x(3)))
     val joinResult = mapResult1.join(mapResult2)
     val result = sc.parallelize(sc.parallelize(joinResult.collect()).map(x => x.toString().split(",")).map(x => x(2).split("\\^")).map (x => (x(0),x(1))).collect())
     result.coalesce(1,true).saveAsTextFile(outputFile)
  }
}