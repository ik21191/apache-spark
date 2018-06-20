package com.spark;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import com.comparator.DocumentComparator;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.WriteConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

import scala.Tuple2;

public class ReadAndSortByComparator {
	static String databaseName = "testdata";
	static String collectionName = "testUsers";
	static String mongoDBUrl = "mongodb://127.0.0.1/";
	static SparkSession spark = null;
	static List<Document> allDocuments = new ArrayList<>();
    public static void main( String[] args ) {
    	spark = SparkSession.builder().master("local").appName("MongoSparkConnectorIntro")
				.config("spark.mongodb.input.uri", mongoDBUrl + databaseName + "." + collectionName)
				.config("spark.mongodb.output.uri", mongoDBUrl + databaseName + "." + collectionName).getOrCreate();
		
			    System.out.println("Created spark session.");
    		    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
    		    
    		    /*Start Example: Read data from MongoDB************************/
    		    JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);
    		    
    		    /*End Example**************************************************/
    		    
    		    System.out.println("Total RDD count loaded form MongoDb : " + rdd.count());

    		    JavaPairRDD<Document, Integer> javaPairRdd = rdd.mapToPair(document->{
    		    	Document doc = new Document();
    		    	doc.append("Name", document.getString("Name")).append("City", document.getString("City"))
    		    	.append("Marks",  document.getInteger("Marks"));
		    		return new Tuple2<>(document, 1);
    		    });
    		    System.out.println("Total JavaPairRDD count : " + javaPairRdd.count());
    		    Comparator<Document> comparator = new DocumentComparator();
    		    JavaPairRDD<Document, Integer> javaPairRddSorted = javaPairRdd.sortByKey(comparator);
    		    
    		    JavaRDD<Document> javaRddToSave = javaPairRddSorted.map(tuple->tuple._1);
    		    
    		    System.out.println("Total RDD count after group: " + javaRddToSave.count());
    		    // Create a custom WriteConfig
    		    Map<String, String> writeOverrides = new HashMap<>();
    		    writeOverrides.put("database", "testdata");
    		    writeOverrides.put("collection", "sorted");
    		    writeOverrides.put("writeConcern.w", "majority");
    		    WriteConfig writeConfig = WriteConfig.create(jsc).withOptions(writeOverrides);
    		    MongoSpark.save(javaRddToSave, writeConfig);
    		    jsc.close();
    }
}
