package com.spark.dataframe.common;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.bson.Document;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
//import org.apache.spark.sql.functions;

public class DataFrameGroupBy4 {
	final static Logger logger = Logger.getLogger(DataFrameGroupBy.class);
	
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
        	    readOverrides.put("collection", "collection1");
        	    readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
    		    /*Start Example: Read data from MongoDB************************/
        	    
        	    
        	    JavaRDD<Document> testMongoRDD = MongoSpark.load(jsc, readConfig);
        	    
        	    System.out.println("Count before flatMap : " + testMongoRDD.count());
        	   
        	    
        	    JavaRDD<MyDocument> javaRddMyDoc = testMongoRDD.flatMap(document->{
        	    	List<MyDocument> list = new ArrayList<>();
        	    	MyDocument md1 = new MyDocument();
        	    	md1.setName(document.getString("Name"));
        	    	md1.setCity(document.getString("City"));
        	    	md1.setMarks(document.getInteger("Marks"));
        	    	md1.setTest("Test1");
        	    	
        	    	MyDocument md2 = new MyDocument();
        	    	md2.setName(document.getString("Name"));
        	    	md2.setCity(document.getString("City"));
        	    	md2.setMarks(document.getInteger("Marks"));
        	    	md2.setTest("Test2");
        	    	
        	    	list.add(md1);
        	    	list.add(md2);
        	    	return list.iterator();
        	    }); 
        	    
        	    
        	    Dataset<Row> dataSet1 = spark.createDataFrame(javaRddMyDoc, MyDocument.class);
        	    
        	    dataSet1.foreach(row->{
        	    	System.out.println(row.getAs("test").toString());
        	    });
        	    
        	    System.out.println("---------------------" + dataSet1.count());
        	    //For selected columns
    		    Dataset<Row> afterGroupBy = dataSet1.groupBy("Name", "Marks").count().
    		    		withColumnRenamed("count", "NameCount").
    		    		orderBy(org.apache.spark.sql.functions.col("NameCount").desc())
    		    		.select("Name", "NameCount").limit(2);
        	    System.out.println(afterGroupBy.count());
    		    jsc.close();
    }
}
