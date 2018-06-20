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

public class GroupWithSum {
	final static Logger log = Logger.getLogger(GroupWithSum.class);
	
	static String databaseName = "";
	static String collectionName = "";
	static String mongoDBUrl = "mongodb://127.0.0.1/";
	static SparkSession spark = null;
	static List<Document> allDocuments = new ArrayList<>();
    public static void main( String[] args )throws Exception {
    	log.info("Application started ............");
    	spark = SparkSession.builder().master("local").appName("MongoSparkConnectorIntro")
				.config("spark.mongodb.input.uri", mongoDBUrl + databaseName + "." + collectionName)
				.config("spark.mongodb.output.uri", mongoDBUrl + databaseName + "." + collectionName).getOrCreate();
		
    			log.info("Created spark session.");
			    
    		    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
    		    
    		    //Create a custom WriteConfig
    		    HashMap<String, String> readOverrides = new HashMap<>();
    	    	ReadConfig readConfig = null;
    	    	readOverrides.put("uri", "mongodb://127.0.0.1/");
    	    	readOverrides.put("database", "groupcheck");
        	    readOverrides.put("collection", "student");
        	    
        	    readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
    		    
        	    Dataset<Row> dataSet1 = MongoSpark.load(jsc, readConfig).toDF();
    		    
    		    Dataset<Row> afterGroupBy = dataSet1.groupBy("id", "name").sum("marks").withColumnRenamed("sum(marks)", "marksSum");
    		    
    		    /**
    		    Dataset<Row> afterGroupBy1 = dataSet1.select("id", "name").distinct();
    		    Dataset<Row> afterGroupBy = afterGroupBy1.groupBy("id").count();
    		    */
    		    afterGroupBy.show();
    		    jsc.close();
    }
    
}
