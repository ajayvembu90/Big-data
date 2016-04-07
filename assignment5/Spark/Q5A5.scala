package template.template

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Q5A5 {
  def main(args: Array[String]) {
     val conf = new SparkConf().setAppName("Simple Application")
     val sc = new SparkContext(conf)
     sc.setLogLevel("WARN")
     val businessFile = sc.textFile("hdfs:///yelpdatafall/business/business.csv")
     val reviewFile = sc.textFile("hdfs:///yelpdatafall/review/review.csv")
     val outputFile = "hdfs:///user/axv143730/assignment5/spark_jar/q5/output7" 
     val mapResult1 = businessFile.filter(x => x.toLowerCase().contains("texas")).map(x => x.split("\\^")).map(x => (x(0),null))
     val mapResult2 = reviewFile.map(x => x.split("\\^")).map(x => (x(2),1))
     val reduceResult1 = mapResult2.reduceByKey(_+_).sortBy(_._2,false)
     val joinResult = mapResult1.join(reduceResult1)
     val result = sc.parallelize(joinResult.collect())
     val mapResult3 = result.map(x => x.toString().split(",")).map(x => (x(0),x(2)))
     mapResult3.coalesce(1,true).saveAsTextFile(outputFile)
  }
}