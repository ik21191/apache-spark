package com.spark.dataframe.common;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;

public class MapAndFlatMap {
	final static Logger logger = Logger.getLogger(MapAndFlatMap.class);
	
	static String databaseName = "ksv_ieee_201709";
	static String collectionName = "counter_20170905_reconcile09_1st";
	static String mongoDBUrl = "mongodb://127.0.0.1/";
	static SparkSession spark = null;
	static List<Document> allDocuments = new ArrayList<>();
    public static void main( String[] args )throws Exception {
    	spark = SparkSession.builder().master("local").appName("MongoSparkConnectorIntro")
				.config("spark.mongodb.input.uri", mongoDBUrl + databaseName + "." + collectionName)
				.config("spark.mongodb.output.uri", mongoDBUrl + databaseName + "." + collectionName).getOrCreate();
		
			    System.out.println("Created spark session.");
			    
    		    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
    		    
    		    //Create a custom WriteConfig
    		    HashMap<String, String> readOverrides = new HashMap<>();
    	    	ReadConfig readConfig = null;
    	    	readOverrides.put("database", "groupcheck");
        	    readOverrides.put("collection", "student");
        	    readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
    		    /*Start Example: Read data from MongoDB************************/
        	    Dataset<Row> dataSet1 = MongoSpark.load(jsc, readConfig).toDF().sort("marks");
    		    
        	    /** Example of dataset map function
        	    Dataset<Integer> years = dataSet1.map((MapFunction<Row, Integer>) row -> row.<Integer>getAs("marks"), Encoders.INT());
        	    years.foreach(val->System.out.println(val.intValue()));
        	    */
        	    
        	    /** Example dataset map function with MapFunction interface
        	    Dataset<Document> years = dataSet1.map((MapFunction<Row, Document>) row -> {
        	    	Document d = new Document();
        	    	d.append("name", "Imran Khan").append("marks", 60);
        	    	return d;
        	    }, Encoders.kryo(Document.class));
        	    years.foreach(doc->System.out.println(doc.toString()));
        	    */
        	    
        	    /** Example of dataset map function
        	    Dataset<Integer> years = dataSet1.map((MapFunction<Row, Integer>) row -> row.<Integer>getAs("marks"), Encoders.INT());
        	    years.foreach(val->System.out.println(val.intValue()));
        	    */
        	    
        	    /** Example dataset map function with lambda expression*/
        	    
        	    
        	    Dataset<Document> datasetMap = dataSet1.map(row -> {
        	    	Document d = new Document();
        	    	d.append("id", row.getAs("id")).append("name", row.getAs("name").toString()).append("subject", row.getAs("subject")).append("marks", row.getAs("marks"));
        	    	return d;
        	    }, Encoders.kryo(Document.class));
        	    
        	    datasetMap.foreach(doc->System.out.println("map: " + doc.toString()));
        	    
        	    
        	    Dataset<Document> datasetFlatMap = dataSet1.flatMap(row -> {
        	    	List<Document> list = new ArrayList<>();
        	    	Document d = new Document();
        	    	d.append("id", row.getAs("id")).append("name", row.getAs("name").toString()).append("subject", row.getAs("subject")).append("marks", row.getAs("marks"));
        	    	list.add(d);
        	    	list.add(d);
        	    	return list.iterator();
        	    }, Encoders.kryo(Document.class));
        	    
        	    datasetFlatMap.foreach(doc->System.out.println("flatMap: " + doc.toString()));
        	    
        	    Dataset<Row> filteredDataset = dataSet1.filter(row->row.getAs("name").toString().equalsIgnoreCase("Imran"));
        	    filteredDataset.show();
        	    
        	    
        	    //filterDataSet.show();
    		    jsc.close();
    }
    
}
