package com.spark;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

//import static spark.Spark.get;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.WriteConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

import scala.Tuple2;

public class ReadAndGroupByAndSortBy {
	static String databaseName = "ksv_ieee_201709";
	static String collectionName = "counter_20170905_reconcile09_1st";
	static String mongoDBUrl = "mongodb://127.0.0.1/";
	static SparkSession spark = null;
	static List<Document> allDocuments = new ArrayList<>();
    @SuppressWarnings("serial")
	public static void main( String[] args )throws Exception {
    	spark = SparkSession.builder().master("local").appName("MongoSparkConnectorIntro")
				.config("spark.mongodb.input.uri", mongoDBUrl + databaseName + "." + collectionName)
				.config("spark.mongodb.output.uri", mongoDBUrl + databaseName + "." + collectionName).getOrCreate();
		
			    System.out.println("Created spark session.");
    		    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
    		    
    		    /*Start Example: Read data from MongoDB************************/
    		    JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);
    		    /*End Example**************************************************/
    		    
    		    System.out.println("Total RDD count before group: " + rdd.count());

    		    /**Using anonymous inner class**/
    		    /*JavaPairRDD<Document, Integer> javaPairRdd = rdd.mapToPair(new PairFunction<Document, Document, Integer>() {
    		    	@Override
    		    	public Tuple2<Document, Integer> call(Document document) throws Exception {
    		    		Document doc = new Document();
    		    		doc.append("journal_id", document.getString("journal_id")).append("article_id", document.getString("article_id"))
    		    		.append("page_type",  document.getInteger("page_type")).append("institution_id", document.getString("institution_id"));
    		    		return new Tuple2<>(doc, 1);
    		    	}
				});*/
    		    /**Using lambda expression**/
    		    JavaPairRDD<Document, Integer> javaPairRdd = rdd.mapToPair(document->{
    		    	Document doc = new Document();
    		    	doc.append("institution_id", document.getString("institution_id")).append("journal_id", document.getString("journal_id"))
		    		.append("article_id",  document.getString("article_id")).append("page_type", document.getInteger("page_type"));
		    		return new Tuple2<>(doc, 1);
    		    });
    		    
    		    
    		    /**Using anonymous inner class**/
    		    /*JavaPairRDD<Document, Integer> javaPairRdd1 = javaPairRdd.reduceByKey(new Function2<Integer, Integer, Integer>() {
					@Override
					public Integer call(Integer val1, Integer val2) throws Exception {
						return val1 + val2;
					}
				});*/
    		    
    		    /**Using lambda expression**/
    		    JavaPairRDD<Document, Integer> javaPairRdd1 = javaPairRdd.reduceByKey((val1, val2)-> val1 + val2);
    		    
    		    JavaPairRDD<Integer, Document> javaPairRdd2 = javaPairRdd1.mapToPair(tuple->new Tuple2<Integer, Document>(tuple._2, tuple._1));
    		    
    		    JavaRDD<Document> javaRddToSave = javaPairRdd2.sortByKey(false).map(tuple->{
    		    	Document newDoc = tuple._2;
    		    	newDoc.append("count", tuple._1);
    		    	return newDoc;
    		    });
    		    
    		    System.out.println("Total RDD count after group: " + javaRddToSave.count());
    		    
    		     // Create a custom WriteConfig
    		    Map<String, String> writeOverrides = new HashMap<>();
    		    writeOverrides.put("database", "testdata");
    		    writeOverrides.put("collection", "countercount");
    		    writeOverrides.put("writeConcern.w", "majority");
    		    WriteConfig writeConfig = WriteConfig.create(jsc).withOptions(writeOverrides);
    		    MongoSpark.save(javaRddToSave, writeConfig);
    		    jsc.close();
    }
    
}
