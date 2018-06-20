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

public class DataFrameGroupBy5 {
	final static Logger logger = Logger.getLogger(DataFrameGroupBy.class);
	
	static String databaseName = "astm_feeder";
	static String collectionName = "201711_accounts";
	static String mongoDBUrl = "mongodb://10.50.8.220/";
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
    	    	readOverrides.put("database", "astm_feeder");
        	    readOverrides.put("collection", "201711_accounts");
        	    readOverrides.put("uri", "mongodb://10.50.8.220/");
        	    readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
    		    /*Start Example: Read data from MongoDB************************/
        	    
        	    
        	    Dataset<Row> dataSet = MongoSpark.load(jsc, readConfig).toDF();
        	    
        	    System.out.println("Count before flatMap : " + dataSet.count());
        	   
      		    jsc.close();
    }
    
    
    private static StructType buildSchema() {
        StructType schema = new StructType(
            new StructField[] {
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("City", DataTypes.StringType, true),
                DataTypes.createStructField("Marks", DataTypes.IntegerType, true),
                
            });
        return (schema);
    }
    
}
