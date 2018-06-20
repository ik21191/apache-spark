package com.spark;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;

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
public class ReadFromMongoWithConnectionClose 
{
    public static void main( String[] args )throws Exception
    {
    	SparkSession spark = SparkSession.builder()
    		      .master("local")
    		      .appName("MongoSparkConnectorIntro")
    		      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/testdata.data1")
    		      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/testdata.data1")
    		      .getOrCreate();
    			System.out.println("Created spark session.");
    		    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
    		    
    		    /*Start Example: Read data from MongoDB************************/
    		    HashMap<String, String> readOverrides = new HashMap<>();
    	    	ReadConfig readConfig = null;
    	    	readOverrides.put("database", "testdata");
        	    readOverrides.put("collection", "data1");
        	    readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
    		    JavaMongoRDD<Document> rdd1 = MongoSpark.load(jsc, readConfig);
    		    /*End Example**************************************************/
    		    
    		    rdd1.foreach(doc->{
    		    	System.out.println(doc.toString());
    		    });
    		    
    		    System.out.println("Closing connection");
    		    jsc.close();
    		    System.out.println("Closing connection done");
    		    
    		    //jsc = new JavaSparkContext(spark.sparkContext());
    		    
    		    readOverrides = new HashMap<>();
    	    	ReadConfig readConfig2 = null;
    	    	readOverrides.put("database", "testdata");
        	    readOverrides.put("collection", "sorted1");
    		    
        	    readConfig2 = ReadConfig.create(jsc).withOptions(readOverrides);
    		    JavaMongoRDD<Document> rdd2 = MongoSpark.load(jsc, readConfig2);
    		    rdd2.foreach(doc->{
    		    	System.out.println(doc.toString());
    		    });
    		    System.out.println("Closing connection");
    		    jsc.close();
    }
    
}
