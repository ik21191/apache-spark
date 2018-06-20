package com.spark;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
public class QueryToMongo1 {
	static DBCollection collection = null;
	static String databaseName = "ieee_201710";
	static String collectionName = "counter_20171009";
	static String mongoDBUrl = "mongodb://10.66.0.216/";
	static SparkSession spark = null;
	public static void main( String[] args ) throws Exception {
    	MongoClient mongo = new MongoClient("10.66.0.216", 27017);
    	
        DB db = mongo.getDB(databaseName);
        collection = db.getCollection(collectionName);
        spark = SparkSession.builder()
  		      .master("local")
  		      .appName("MongoSparkConnectorIntro")
  		      .config("spark.mongodb.input.uri", mongoDBUrl + databaseName +"." + collectionName)
  		      .config("spark.mongodb.output.uri", mongoDBUrl + databaseName +"." + collectionName)
  		      .getOrCreate();
  			System.out.println("Created spark session.");
  			
  		    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
  		    BasicDBObject andQuery = new BasicDBObject();
  		    List<BasicDBObject> obj = new ArrayList<>();
  		    obj.add(new BasicDBObject("page_type", 10));
  		    obj.add(new BasicDBObject("long_ip_address", "1035135459"));
  		    obj.add(new BasicDBObject("institution_id", "APAC1"));
  		    
  		    /**Uncomment to query from filter based on regex*/
  		    //BasicDBObject regexQuery = new BasicDBObject();
		    //regexQuery.put("institution_details", new BasicDBObject("$regex", "^.*APAC1.*").append("$options", "i"));
		    //obj.add(regexQuery);
  		    
  		    andQuery.put("$and", obj);

  		System.out.println(andQuery.toString());

  		DBCursor cursor = collection.find(andQuery);
  		int count = 0;
  		while (cursor.hasNext()) {
  			System.out.println(cursor.next());
  			++count;
  		}
  		System.out.println("Total result : " + count);
  		    jsc.close();
  		    mongo.close();
    }
	
    
}
