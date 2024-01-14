package com.spark.session;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
/**
 * Program to print the number of occurrence of a character.
 * */
public class HelloSparkSession {
  static SparkSession spark = null;

  private static final String[] data = {"X","A","B","A","a","1","A","D","#","Z","D","A","1","X","A","#"};

  public static void main( String[] args )throws Exception {
    spark = SparkSession.builder()
	.master("local")
	.appName("MongoSparkConnectorIntro")
	.getOrCreate();

	System.out.println("Created spark session.");
	JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
	//Converting array to JavaRDD
	JavaRDD<String> javaRDD = jsc.parallelize(Arrays.asList(data));
	//Flattening above RDD by splitting each line by space
	JavaRDD<String> flatRdd = javaRDD.flatMap(content -> Arrays.asList(content.split(" ")).iterator());
	//Creating key value pair like abc,1   xyz,1    abc,1
	JavaPairRDD<String, Number> pairRDD = flatRdd.mapToPair(str->new Tuple2<>(str, 1));
	//Reducing pari rdd like abc, 1+1
	pairRDD = pairRDD.reduceByKey((x, y)-> x.intValue() + y.intValue());
	//printing group RDD
	pairRDD.foreach(tuple->System.out.println(tuple._1 + "   " + tuple._2));
	jsc.close();
	spark.close();
  }
}
