package com.spark.dataframe.join;
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

public class DataFrameJoin1 {
	final static Logger logger = Logger.getLogger(DataFrameJoin1.class);
	
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
    	    	readOverrides.put("database", "testdata");
        	    readOverrides.put("collection", "sorted1");
        	    readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
    		    /*Start Example: Read data from MongoDB************************/
    		    JavaMongoRDD<Document> rddSorted1 = MongoSpark.load(jsc, readConfig);
    		    
    		    readOverrides = new HashMap<>();
    		    readOverrides.put("database", "testdata");
        	    readOverrides.put("collection", "sorted2");
        	    readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
    		    /*Start Example: Read data from MongoDB************************/
    		    JavaMongoRDD<Document> rddSorted2 = MongoSpark.load(jsc, readConfig);
    		    
    		    readOverrides = new HashMap<>();
    		    readOverrides.put("database", "testdata");
        	    readOverrides.put("collection", "sorted3");
        	    readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
    		    /*Start Example: Read data from MongoDB************************/
    		    JavaMongoRDD<Document> rddSorted3 = MongoSpark.load(jsc, readConfig);
    		    
    		    
    		    /*End Example**************************************************/
    		    
    		    //System.out.println("Total RDD count before group: " + rddSorted1.count());
    		    //System.out.println("Total RDD count before group: " + rddSorted2.count());
    		    
    		    
    		    //Dataset<Row> dataSet1 = rddSorted1.toDF().select("Name", "City").filter("Name in ('Imran')");
    		    //Dataset<Row> dataSet1 = rddSorted1.toDF().select("Name", "City").filter("Name=='Vinay'");
    		    Dataset<Row> dataSet1 = rddSorted1.toDF().select("Name", "City");
    		    System.out.println("dataSet1 count " + dataSet1.count());
    		    Dataset<Row> dataSet2 = rddSorted2.toDF().select("Name", "City");
    		    System.out.println("dataSet2 count " + dataSet2.count());
    		    
    		    Dataset<Row> dataSet3 = rddSorted3.toDF();
    		    
    		    Dataset<Row> dataSetAfterJoin = dataSet1.join(dataSet2, "Name");
    		    
    		    //Joint using explicit column
    		    //Dataset<Row> dataSetAfterJoin = dataSet1.join(dataSet2, dataSet1.col("Name").equalTo(dataSet2.col("City")));
    		    
    		    dataSetAfterJoin.show();
    		    
    		    dataSetAfterJoin.foreach(row->{
    		    	logger.error("Name : " + row.get(0) + "\t City : " + row.get(1));
    		    	/**To display all fields of a row
    		    	for(int i = 0; i < row.length(); i++) {
    		    		logger.error("Name : " + row.get(0) + "\t City : " + row.get(1));
    		    	}
    		    	*/
    		    	
    		    });
    		    jsc.close();
    }
    
}
