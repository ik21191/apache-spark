package com.spark;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

//import static spark.Spark.get;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.config.WriteConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

import scala.Tuple2;

public class JavaRddJoin2 {
	static String databaseName = "ksv_ieee_201709";
	static String collectionName = "counter_20170905_reconcile09_1st";
	static String mongoDBUrl = "mongodb://127.0.0.1/";
	static SparkSession spark = null;
	static List<Document> allDocuments = new ArrayList<>();
    @SuppressWarnings("serial")
	public static void main( String[] args )throws Exception {
    	spark = SparkSession.builder().master("local").appName("MongoSparkConnectorIntro")
				.config("spark.mongodb.input.uri", mongoDBUrl + databaseName + "." + collectionName)
				.config("spark.mongodb.output.uri", mongoDBUrl + databaseName + "." + collectionName).getOrCreate();
		
			    System.out.println("Created spark session.");
			    
    		    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
    		    
    		    //Create a custom WriteConfig
    		    HashMap<String, String> readOverrides = new HashMap<>();
    	    	ReadConfig readConfig = null;
    	    	readOverrides.put("database", "testdata");
        	    readOverrides.put("collection", "sorted1");
        	    readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
    		    /*Start Example: Read data from MongoDB************************/
    		    JavaMongoRDD<Document> rddSorted1 = MongoSpark.load(jsc, readConfig);
    		    
    		    
    		    readOverrides.put("database", "testdata");
        	    readOverrides.put("collection", "sorted3");
        	    readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
    		    /*Start Example: Read data from MongoDB************************/
    		    JavaMongoRDD<Document> rddSorted2 = MongoSpark.load(jsc, readConfig);
    		    
    		    readOverrides.put("database", "testdata");
        	    readOverrides.put("collection", "sorted4");
        	    readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
    		    /*Start Example: Read data from MongoDB************************/
    		    JavaMongoRDD<Document> rddSorted3 = MongoSpark.load(jsc, readConfig);
    		    
    		    
    		    /*End Example**************************************************/
    		    
    		    System.out.println("Total RDD count before group: " + rddSorted1.count());
    		    System.out.println("Total RDD count before group: " + rddSorted2.count());
    		    
    		    
    		    Dataset<Row> dataSet1 = rddSorted1.toDF();
    		    Dataset<Row> dataSet2 = rddSorted2.toDF();
    		    Dataset<Row> dataSet3 = rddSorted3.toDF();
    		    
    		    Dataset<Row> dataSetAfterJoin = dataSet1.join(dataSet2, "Name");
    		    
    		    
    		    
    		    JavaRDD<Row> javaRDD = dataSetAfterJoin.toJavaRDD();
    		    
    		    javaRDD.filter(row->{
    		    	System.out.println();
    		    	for(int i = 0; i < row.length(); i++) {
    		    		//System.out.println(row.schema());
    		    		System.out.print(row.get(i) + "\t");
    		    	}
    		    	return false;
    		    }).count();
    		    
    		    jsc.close();
    }
    
}
