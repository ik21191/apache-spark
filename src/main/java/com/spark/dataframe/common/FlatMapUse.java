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
//import org.apache.spark.sql.functions;

public class FlatMapUse {
	final static Logger log = Logger.getLogger(FlatMapUse.class);
	
	static String databaseName = "ksv_ieee_201709";
	static String collectionName = "counter_20170905_reconcile09_1st";
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
    	    	readOverrides.put("database", "groupcheck");
        	    readOverrides.put("collection", "collection1");
        	    readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
    		    /*Start Example: Read data from MongoDB************************/
        	    Dataset<Row> dataSet1 = MongoSpark.load(jsc, readConfig).toDF();
    		    
    		    /*End Example**************************************************/
    		    
    		    //System.out.println("Total RDD count before group: " + rddSorted1.count());
    		    //System.out.println("Total RDD count before group: " + rddSorted2.count());
    		    
        	    //For all columns
    		    //Dataset<Row> afterGroupBy = dataSet1.groupBy("Name", "Marks").count().
    		    	//	withColumnRenamed("count", "NameCount").orderBy(org.apache.spark.sql.functions.col("NameCount").desc());
    		    
    		    //For selected columns
    		    Dataset<Row> filter = dataSet1.filter(dataSet1.col("IMRAN").isNotNull());
    		    Dataset<Row> afterGroupBy = filter.groupBy("Name", "Marks").count().
    		    		withColumnRenamed("count", "NameCount").
    		    		orderBy(org.apache.spark.sql.functions.col("NameCount").desc())
    		    		.select("Name", "NameCount").limit(2);
        	    afterGroupBy.show();
    		    jsc.close();
    }
    
}
