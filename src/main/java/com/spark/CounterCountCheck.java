package com.spark;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
/**
 * Hello world!
 *
 */
public class CounterCountCheck 
{
    public static void main( String[] args )throws Exception
    {
    	SparkSession spark = SparkSession.builder()
    		      .master("local")
    		      .appName("MongoSparkConnectorIntro")
    		      .config("spark.mongodb.input.uri", "mongodb://insight_1.macmill.com:27017/filter_ieee.filter_20171109")
    		      .config("spark.mongodb.output.uri","mongodb://insight_1.macmill.com:27017/filter_ieee.filter_20171109")
    		      .getOrCreate();
    			System.out.println("Created spark session.");
    		    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
    		    
    		    /*Start Example: Read data from MongoDB************************/
    		    HashMap<String, String> readOverrides = new HashMap<>();
    	    	ReadConfig readConfig = null;
    	    	readOverrides.put("database", "ieee_filter");
        	    readOverrides.put("collection", "filter_20171109");
        	    readOverrides.put("uri", "mongodb://insight_1.macmill.com:27017");
        	    readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
    		    JavaMongoRDD<Document> rdd1 = MongoSpark.load(jsc, readConfig);
    		    /*End Example**************************************************/
    		    List<String> metricTypeList = new ArrayList<>();
    	    	metricTypeList.add("Regular");
    	    	metricTypeList.add("TDM");
    	    	
    	    	BasicDBObject basicDBOObject = new BasicDBObject("$match", new BasicDBObject().append("access_method", new BasicDBObject("$in", metricTypeList))
    	    			.append("page_metric_id", new BasicDBObject("$ne", 0)));
    		    
    		    JavaMongoRDD<Document> finalRDD = rdd1.withPipeline(Collections.singletonList(basicDBOObject));
    		    System.out.println(finalRDD.count());
    		    
    		    jsc.close();
    }
    
}
