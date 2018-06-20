package com.spark.dataframe.common;
import java.io.Serializable;
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

public class MapAndFlatMapWithBean {
	final static Logger logger = Logger.getLogger(MapAndFlatMapWithBean.class);
	
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
    		    
        	    Dataset<Student> datasetMap = dataSet1.map(row -> {
        	    	Student student = new Student();
        	    	student.setId(row.getAs("id"));
        	    	student.setName(row.getAs("name").toString());
        	    	student.setSubject(row.getAs("subject"));
        	    	student.setMarks(row.getAs("marks"));
        	    	
        	    	return student;
        	    }, Encoders.bean(Student.class));
        	    
        	    datasetMap.foreach(student->System.out.println("map: " + student.toString()));
        	    
        	    String[] cols = datasetMap.columns();
        	    for(int i = 0; i < cols.length; i++) {
        	    	System.out.println("columns are: " + cols[i]);	
        	    }
        	    
        	    System.out.println("Schema: " + datasetMap.schema());
        	    Dataset<Row> groupCheck = datasetMap.groupBy("name").count();
        	    groupCheck.show();
        	    
        	    jsc.close();
    }
}