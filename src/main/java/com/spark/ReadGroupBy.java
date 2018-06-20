package com.spark;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
public class ReadGroupBy {
	static DBCollection collection = null;
	static String databaseName = "test_201710";
	static String collectionName = "counter_20171009";
	static String mongoDBUrl = "mongodb://localhost/";
	static SparkSession spark = null;
	public static void main( String[] args ) throws Exception {
    	MongoClient mongo = new MongoClient("localhost", 27017);
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
  		    PrintWriter out1 = new PrintWriter(new FileWriter(new File("e://export1.csv")));
  		    //PrintWriter out2 = new PrintWriter(new FileWriter(new File("e://export1.txt")));
  		    AggregationOutput output = groupByUse();
  		    int count =0;
  		    for (DBObject result : output.results()) {
  		    	//System.out.println(result);
  		    	//Remove curly brackets
  		    	String processString = result.get("_id").toString().replace("{ \"", "").replace("}", "").replace("\"", "");
  		    	String[] commaSeparated = processString.split(",");
  		    	String[] cityStr = commaSeparated[0].split(":");
  		    	String[] marksStr = commaSeparated[1].split(":"); 
  		    	out1.println(cityStr[1].trim() + "||" + "'" + marksStr[1].trim() + "||" + result.get("totalCount"));
  		    	//out2.println(result);
  		    	++count;
  		    }
  		    out1.close();
  		    //out2.close();
  		    System.out.println("count is " + count);
  		    jsc.close();
  		    mongo.close();
    }
	
    private static AggregationOutput groupByUse() {
    	DBObject match1 = new BasicDBObject("$match", new BasicDBObject("institution_id", "APAC1"));
    	DBObject match2 = new BasicDBObject("$match", new BasicDBObject("page_type", 10));
    	BasicDBObject groupFields = new BasicDBObject();
    	Map<String, String> map = new HashMap<>();
    	map.put("institution_id", "$institution_id");
    	map.put("long_ip_address", "$long_ip_address");
    	Set<String> keySet = map.keySet();
    	//for(String key : keySet) {
    		groupFields.append("institution_id", "$institution_id");
    		groupFields.append("long_ip_address", "$long_ip_address");
    	//}
		
    	BasicDBObject groupByFunction = new BasicDBObject("_id", groupFields);
		DBObject group = new BasicDBObject("$group", groupByFunction.append("totalCount", new BasicDBObject("$sum", 1)));
		return collection.aggregate(match1, match2, group);
	
    	
    	/*DBObject match = new BasicDBObject("$match", new BasicDBObject("Marks", 100));
    	
    	DBObject fields = new BasicDBObject("City", 1);
    	fields.put("Marks", 1);
    	fields.put("_id", 0);
    	DBObject project = new BasicDBObject("$project", fields );
    	
    	// Now the $group operation
    	DBObject groupFields = new BasicDBObject( "_id", "$City");
    	//groupFields.put("_id", "$Marks");
    	groupFields.put("average", new BasicDBObject( "$sum", 1));
    	DBObject group = new BasicDBObject("$group", groupFields);

    	// run aggregation
    	return collection.aggregate( project, group );*/
    
    }
}
