package com.spark.dataframe.common;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.collections.bag.SynchronizedSortedBag;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;



import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;

import scala.Option;
import scala.Tuple1;
import scala.Tuple2;

public class ScalaCollection1 {
	final static Logger logger = Logger.getLogger(ScalaCollection1.class);
	
	static scala.collection.immutable.HashMap map = new scala.collection.immutable.HashMap<>();
	static long count;
	
	static String databaseName = "ieee_feeder";
	static String collectionName = "201801_feed_articles";
	static String mongoDBUrl = "mongodb://10.31.1.109/";
	static SparkSession spark = null;
	@SuppressWarnings("unchecked")
	public static void main( String[] args )throws Exception {
    	spark = SparkSession.builder().master("local").appName("MongoSparkConnectorIntro")
				.config("spark.mongodb.input.uri", mongoDBUrl + databaseName + "." + collectionName)
				.config("spark.mongodb.output.uri", mongoDBUrl + databaseName + "." + collectionName).getOrCreate();
		
			    System.out.println("Created spark session.");
			    
    		    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
    		    
    		    HashMap<String, String> readOverrides = new HashMap<>();
    	    	ReadConfig readConfig = null;
    	    	readOverrides.put("database", "groupcheck");
        	    readOverrides.put("collection", "student");
        	    readOverrides.put("uri", "mongodb://127.0.0.1/");
        	    readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
    		    /*Start Example: Read data from MongoDB************************/
        	    Dataset<Row> dataSet1 = MongoSpark.load(jsc, readConfig).toDF();
    		    
        	    dataSet1.foreach(row-> {
        	    	Document d = new Document().append("name", "Imran Khan");
        	    	Tuple2<Long, Document> tuple = new Tuple2<>(count++, d);
        	    	map = map.$plus(tuple);
        	    }
        	    );
        	    
        	    System.out.println("Scala map size : " + map.size());
        	    
        	    Option<Document> doc =  map.get(5);
        	    if(doc.isEmpty()) {
        	    	System.out.println("Document not found");
        	    } else {
        	    	Document doc1 = doc.get();
        	    	System.out.println("finding value : " + doc1);
        	    }
        	    
        	    
        	    
        	    //filterDataSet.show();
    		    jsc.close();
    }
    
}
