package com.spark.dataframe.common;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

public class DataFrameWithFilterString {
	final static Logger logger = Logger.getLogger(DataFrameWithFilterString.class);
	
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
        	    Dataset<Row> dataSet1 = MongoSpark.load(jsc, readConfig).toDF();
    		    List<String> numList = new ArrayList<>();
    		    numList.add("Imran");
    		    numList.add("Vinay");
    		    numList.add("Raj");
    		    //Dataset<Row> filterDataSet = dataSet1.filter(dataSet1.col("Marks").isin(127, 200));
    		    //Using list in isin() method
    		    Dataset<Row> filterDataSet = dataSet1.filter(dataSet1.col("name").isin(numList.stream().toArray()));
        	    filterDataSet.show();
    		    jsc.close();
    }
    
}
