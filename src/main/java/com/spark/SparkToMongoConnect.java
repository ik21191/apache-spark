package com.spark;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import com.comparator.DocumentComparator;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.WriteConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

import scala.Tuple2;

public class SparkToMongoConnect {
	static String databaseName = "testdata";
	static String collectionName = "testUsers";
	static String mongoDBUrl = "mongodb://127.0.0.1/";
	//static SparkSession spark = null;
	static List<Document> allDocuments = new ArrayList<>();
    public static void main( String[] args ) {
    	
    		System.out.println("Created spark session.");
    		    JavaSparkContext jsc = getJavaSparkContext();
    		    
    		    /*Start Example: Read data from MongoDB************************/
    		    JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);
    		    /*End Example**************************************************/
    		    
    		    System.out.println("Total RDD count loaded form MongoDb : " + rdd.count());
    		    
    		    
    		    SparkConf conf = jsc.getConf();
    		    
    		    String host = conf.get("spark.mongodb.input.uri").replace("mongodb://", "");
    		    host = host.substring(0, host.indexOf("/"));
    		    
    		    System.out.println("host is             : " + host);
    		    MongoClient mongo = new MongoClient(host, 27017);
    	        DB db = mongo.getDB(databaseName);
    	        DBCollection collection = db.getCollection(collectionName);
    	        
    	        collection.remove(new BasicDBObject());
    	        mongo.close();
      		    jsc.close();
    }
    
    
public static JavaSparkContext getJavaSparkContext(){
    	
	String sparkMaster = "local";
	String sparkAppName = "MongoSparkConnectorIntro";
	String sparkMongodbInputUri = mongoDBUrl + databaseName + "." + collectionName;
	String sparkMongodbOutputUri = mongoDBUrl + databaseName + "." + collectionName;
    	SparkConf sConf = null;
    	JavaSparkContext jsc = null;
    	try{
    		System.out.println("SparkConf Generation : Start");
    		sConf = new SparkConf();
    		
    		System.out.println("SparkConf sparkMaster : " + sparkMaster);
	        sConf.setMaster(sparkMaster);
	        
	        
	        System.out.println("SparkConf sparkAppName : " + sparkAppName);
	        sConf.setAppName(sparkAppName);
	        
	        
	        System.out.println("SparkConf spark.mongodb.input.uri : " + sparkMongodbInputUri);
	        sConf.set("spark.mongodb.input.uri", sparkMongodbInputUri);
	        
	        
	        System.out.println("SparkConf spark.mongodb.output.uri : " + sparkMongodbOutputUri);
	        sConf.set("spark.mongodb.output.uri", sparkMongodbOutputUri);
	        
	        
	        System.out.println("SparkConf Generation : Done");
    		
	        //
	        System.out.println("JavaSparkContext Generation : Start");
	    	jsc = new JavaSparkContext(sConf);
	    	
	    	System.out.println("JavaSparkContext Generation : Done");
    	}
    	catch(Exception e){
    		System.out.println("getJavaSparkContext : " + " : " +  e.toString());
    	} 
        return jsc;
    }
}
