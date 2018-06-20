package com.spark;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
/**
 * Hello world!
 *
 */
public class QueryToMongoUsingRDD 
{
    public static void main( String[] args )throws Exception
    {
    	SparkSession spark = SparkSession.builder()
    		      .master("local")
    		      .appName("MongoSparkConnectorIntro")
    		      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/testdata.updated")
    		      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/testdata.updated")
    		      .getOrCreate();
    			System.out.println("Created spark session.");
    		    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
    		    
    		    /*Start Example: Read data from MongoDB************************/
    		    JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);
    		    /*End Example**************************************************/
    		    List<Integer> inList = new ArrayList<>();
    			inList.add(65);
    			inList.add(100);
    			inList.add(78);
    			BasicDBObject andQuery = new BasicDBObject();
    		    List<BasicDBObject> whereConditionList = new ArrayList<BasicDBObject>();
    		    whereConditionList.add(new BasicDBObject("Name", "Imran"));
    		    //whereConditionList.add(new BasicDBObject("MyMarks", 65));
    		    whereConditionList.add(new BasicDBObject("MyMarks", new BasicDBObject("$in", inList)));
    		    //whereConditionList.add(new BasicDBObject("City", new BasicDBObject("$ne", inList)));
    		    whereConditionList.add(new BasicDBObject("City", new BasicDBObject("$ne", "Mumbai")));
    		    andQuery.put("$and", whereConditionList);
		  
    		    /*BasicDBObject bdo = new BasicDBObject("$match", new BasicDBObject().append("Name", "Imran").append("MyMarks", new BasicDBObject("$in", inList)
    		    		).append("City", new BasicDBObject("$ne", "Mumbai")));*/
    		    
    		    BasicDBObject bdo = new BasicDBObject("$match", new BasicDBObject().append("MyMarks", new BasicDBObject("$in", inList)
    		    		).append("City", new BasicDBObject("$ne", "Mumbai")));
    		    
    		    
    		    //bdo.append("$match", new BasicDBObject().append("City", "New Delhi").append("Name", p)).
    		    //append("Marks", new BasicDBObject("$gt", 0));
    		    JavaRDD<Document> rdd1 = rdd.withPipeline(Collections.singletonList(bdo));
    		    // Analyze data from MongoDB
    		    System.out.println(rdd1.count());
    		    //System.out.println(rdd.first().toJson());
    		    List<Document> list = rdd1.collect();
    		    for(Document document : list) {
    		    	System.out.println(document.toJson());
    		    }
    		    
    		    jsc.close();
    }
    
}
