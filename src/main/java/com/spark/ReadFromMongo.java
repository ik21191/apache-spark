package com.spark;

import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

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
public class ReadFromMongo 
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
    		    Pattern p = Pattern.compile("^Vinay.*");
    		    BasicDBObject bdo = new BasicDBObject("$match", new BasicDBObject().append("City", "New Delhi").append("Name", p).
    		    		append("MyMarks", new BasicDBObject("$eq", 101)));
		  
    		    
    		    
    		    
    		    //bdo.append("$match", new BasicDBObject().append("City", "New Delhi").append("Name", p)).
    		    //append("Marks", new BasicDBObject("$gt", 0));
    		    JavaMongoRDD<Document> rdd1 = rdd.withPipeline(Collections.singletonList(bdo));
    		    // Analyze data from MongoDB
    		    System.out.println(rdd1.count());
    		    //System.out.println(rdd.first().toJson());
    		    List<Document> list = rdd1.collect();
    		    for(Document document : list) {
    		    	//document.replace("name", "imran khan");//to update value of specific column in Document
    		    	String dflt = (String)document.getOrDefault("Name", "Default Value");
    		    	System.out.println(dflt);
    		    	System.out.println(document.getObjectId("_id"));
    		    	System.out.println(document.toJson());
    		    }
    		    
    		    jsc.close();
    }
    
}
