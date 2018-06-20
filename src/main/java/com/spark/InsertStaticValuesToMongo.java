package com.spark;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
//import static spark.Spark.get;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.WriteConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import com.weblog.parser.MyDocument;
import com.weblog.parser.WebLogParser;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;

import org.bson.Document;
import static java.util.Arrays.asList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
/**
 * Hello world!
 *
 */
public class InsertStaticValuesToMongo 
{
	static DBCollection collection = null;
	static String databaseName = "testdata";
	static String collectionName = "testUsers";
	static SparkSession spark = null;
    @SuppressWarnings("serial")
	public static void main( String[] args )throws Exception {
    	MongoClient mongo = new MongoClient("localhost");
        DB db = mongo.getDB(databaseName);
        collection = db.getCollection(collectionName);
        System.out.println("Inserting document.......");
        insertTestData() ;
        System.out.println("Document insertion done.");
        
        spark = SparkSession.builder()
  		      .master("local")
  		      .appName("MongoSparkConnectorIntro")
  		      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/" + databaseName +"." + collectionName)
  		      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/" + databaseName +"." + collectionName)
  		      .getOrCreate();
  			System.out.println("Created spark session.");
  		    //JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
  		    
  		    /*Start Example: Read data from MongoDB************************/
  		    //JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);
  		    /*End Example**************************************************/

  		    
  		    //printRddUsingMap(rdd);
  		    
  		    //sortAndSave(rdd);
  		    //printRddUsingList(rdd);
  		    //jsc.close();
  		    mongo.close();
  		  
    }
	
    private static void printRddUsingList(JavaMongoRDD<Document> rdd) {
		List<Document> list = rdd.collect();
		    for(Document document : list) {
		    	//document.replace("name", "imran khan");//to update value of specific column in Document
		    	System.out.println(document.toJson());
		    }
		
	}
	
	private static void printRddUsingMap(JavaRDD<Document> rdd) {
		JavaRDD<Document> rddTest = rdd.map(doc->{System.out.println(doc.toString());return doc;});
		System.out.println(rddTest.count());
	}
	
	private static void sortAndSave(JavaRDD<Document> rdd) {
		JavaRDD<Document> documents = rdd.sortBy(doc->doc.getString("Name") + doc.getString("City"), true, 1);
		Map<String, String> writeOverrides = new HashMap<String, String>();
	    writeOverrides.put("collection", "sortedData");
	    writeOverrides.put("writeConcern.w", "majority");
	    WriteConfig writeConfig = WriteConfig.create(spark).withOptions(writeOverrides);
	    MongoSpark.save(documents, writeConfig);
	}
	private static void insertTestData() {
		BasicDBObject basicDBObject1 = new BasicDBObject();
		BasicDBObject basicDBObject2 = new BasicDBObject();
		BasicDBObject basicDBObject3 = new BasicDBObject();
		BasicDBObject basicDBObject4 = new BasicDBObject();
		BasicDBObject basicDBObject5 = new BasicDBObject();
		BasicDBObject basicDBObject6 = new BasicDBObject();
		BasicDBObject basicDBObject7 = new BasicDBObject();
		
		basicDBObject1.append("Name", "Imman").append("City", "New Delhi").append("Marks", 100);
		basicDBObject2.append("Name", "Vinay").append("City", "Punjab").append("Marks", 90);
		basicDBObject3.append("Name", "Rajesh").append("City", "Agra").append("Marks", 88);
		basicDBObject4.append("Name", "Ravesh").append("City", "Mumbai").append("Marks", 78);
		basicDBObject5.append("Name", "Imran").append("City", "Nasik").append("Marks", 65);
		basicDBObject6.append("Name", "Imran").append("City", "New Delhi").append("Marks", 100);
		basicDBObject7.append("Name", "Rakesh").append("City", "New Delhi").append("Marks", 100);
		
		collection.insert(basicDBObject1);
		collection.insert(basicDBObject2);
		collection.insert(basicDBObject3);
		collection.insert(basicDBObject4);
		collection.insert(basicDBObject5);
		collection.insert(basicDBObject6);
		collection.insert(basicDBObject7);
	}
}
