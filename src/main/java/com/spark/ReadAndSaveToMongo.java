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

public class ReadAndSaveToMongo {
	public static final Logger log = Logger.getLogger(ReadAndSaveToMongo.class); 
	static String databaseName = "testdata";
	static String collectionName = "sorted1";
	static String mongoDBUrl = "mongodb://127.0.0.1/";
	static SparkSession spark = null;
	
    public static void main( String[] args )throws Exception {
    	//spark = SparkSession.builder().master("local").appName("MongoSparkConnectorIntro")
    	spark = SparkSession.builder().appName("MongoSparkConnectorIntro")
				.config("spark.mongodb.input.uri", mongoDBUrl + databaseName + "." + collectionName)
				.config("spark.mongodb.output.uri", mongoDBUrl + databaseName + "." + collectionName).getOrCreate();
		
			    log.info("Created spark session.");
			    log.info("Loading...");
    		    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
    		    log.info("Loaded.");
    		    /*Start Example: Read data from MongoDB************************/
    		    
    		    log.info("Saving.     ..");
    		    JavaRDD<Document> rdd = MongoSpark.load(jsc, readConfig(jsc));
    		    /*End Example**************************************************/
    		    MongoSpark.save(rdd, writeConfig(jsc));
    		    log.info("Saved.");
    		    jsc.close();
    }
    
    private static ReadConfig readConfig(JavaSparkContext jsc) {
    	HashMap<String, String> readOverrides = new HashMap<>();
    	readOverrides.put("database", "counterdata");
	    readOverrides.put("collection", "filter_1");
	    readOverrides.put("uri", "mongodb://127.0.0.1");
	    return ReadConfig.create(jsc).withOptions(readOverrides);
    }
    
    private static WriteConfig writeConfig(JavaSparkContext jsc) {
    	Map<String, String> writeOverrides = new HashMap<>();
	    writeOverrides.put("database", "counterdata");
	    writeOverrides.put("collection", "filter_test");
	    writeOverrides.put("uri", "mongodb://127.0.0.1");
	    return WriteConfig.create(jsc).withOptions(writeOverrides);
    }
    
}
