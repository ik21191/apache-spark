package com.javardd.common;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

/**
 * Hello world!
 *
 */
public class EmptyJavaRDD {
	public static void main(String[] args) throws Exception {
		SparkSession spark = SparkSession.builder().master("local").appName("MongoSparkConnectorIntro")
				.config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/testdata.updated")
				.config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/testdata.updated").getOrCreate();
		System.out.println("Created spark session.");
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

		/* Start Example: Read data from MongoDB ************************/
		JavaRDD<Document> rdd = jsc.emptyRDD();
		/* End Example **************************************************/
		System.out.println("Total count : " + rdd.count());
		jsc.close();
	}

}
