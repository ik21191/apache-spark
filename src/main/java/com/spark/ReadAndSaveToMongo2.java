package com.spark;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

//import static spark.Spark.get;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.config.WriteConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

import helper.ToInteger;
import scala.Tuple2;

public class ReadAndSaveToMongo2 {
	public static final Logger log = Logger.getLogger(ReadAndSaveToMongo2.class); 
	static String databaseName = "testdata";
	static String collectionName = "sorted1";
	static String mongoDBUrl = "mongodb://127.0.0.1/";
	static SparkSession spark = null;
	
    public static void main( String[] args )throws Exception {
    	spark = SparkSession.builder().master("local").appName("MongoSparkConnectorIntro")
				.config("spark.mongodb.input.uri", mongoDBUrl + databaseName + "." + collectionName)
				.config("spark.mongodb.output.uri", mongoDBUrl + databaseName + "." + collectionName).getOrCreate();
		
			    System.out.println("Created spark session.");
			    System.out.println("Loading...");
    		    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
    		    System.out.println("Loaded.");
    		    /*Start Example: Read data from MongoDB************************/
    		    
    		    System.out.println("Saving...");
    		    JavaRDD<Document> rdd = MongoSpark.load(jsc, readConfig(jsc));
    		    System.out.println("Saved.");
    		    /*End Example**************************************************/
    		    
    		    MongoSpark.save(rdd, writeConfig(jsc));
    		    jsc.close();
    }
    
    private static ReadConfig readConfig(JavaSparkContext jsc) {
    	HashMap<String, String> readOverrides = new HashMap<>();
    	readOverrides.put("database", "groupcheck");
	    readOverrides.put("collection", "collection1");
	    readOverrides.put("uri", "mongodb://127.0.0.1");
	    return ReadConfig.create(jsc).withOptions(readOverrides);
    }
    
    private static WriteConfig writeConfig(JavaSparkContext jsc) {
    	Map<String, String> writeOverrides = new HashMap<>();
	    writeOverrides.put("database", "groupcheck");
	    writeOverrides.put("collection", "temp1");
	    writeOverrides.put("uri", "mongodb://127.0.0.1");
	    return WriteConfig.create(jsc).withOptions(writeOverrides);
    }
    
}
