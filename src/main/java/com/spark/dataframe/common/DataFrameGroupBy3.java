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

public class DataFrameGroupBy3 {
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
        	    
        	    
        	    JavaMongoRDD<Document> testMongoRDD = MongoSpark.load(jsc, readConfig);
        	    
        	    System.out.println("Count before flatMap : " + testMongoRDD.count());
        	    /*JavaRDD<Document> testRDD = testMongoRDD.flatMap(document->{
        	    	
        	    	
        	    	
        	    	
        	    	List<Document> list = new ArrayList<>();
        	    	list.add(document.append("test1", "val1"));
        	    	list.add(document.append("test2", "val2"));
        	    	return list.iterator();
        	    	
        	    	//return null;
        	    });*/
        	    
        	    
        	    
        	    /*testRDD.foreach(document->{
        	    	System.out.println("xx :  " + document);
        	    });*/
        	    
        	    
        	    
        	    //System.out.println("Count is " + testRDD.count());
        	    
        	    
        	    
        	    //Dataset<Row> dataSet1 = spark.createDataset(testRDD, Encoders.bean(Row.class));
        	    //Dataset<Row> dataSet1 = spark.createDataFrame(testRDD, Row.class);
        	    //Encoder<Document> personEncoder = Encoders.bean(Document.class);
        	    
        	    
        	    Dataset<Row> dataSet1 = testMongoRDD.toDF();
        	    
        	    StructType schema = dataSet1.schema();
        	    Dataset<Row> dataSet2 = dataSet1.flatMap((FlatMapFunction<Row, Row>) row -> {
        	    	Row newRow = RowFactory.create(row.get(0), row.get(1), row.get(2));
        	    	
        	    	//System.out.println(row.getSeq(0));
        	    	List<Row> list = new ArrayList<>();
        	    	list.add(newRow);
        	    	return list.iterator();

        	    }, Encoders.bean(Row.class));
        	    
        	    
        	    //dataSet2.toDF("Name", "City", "Marks");
        	    
        	    //Dataset<Row> dataSet1 = spark.createDataFrame(testMongoRDD, Document.class);
        	    
        	    
        	    dataSet2.foreach(row->{
        	    	//for(int i = 0; i < row.length(); i++) {
        	    		System.out.print(row.getAs("Name").toString() + "\t " + row.getAs("City").toString() + "\t" + row.getAs("Marks"));
        	    //	}
        	    	System.out.println();
        	    });
        	    
        	    System.out.println("dataSet1 count is : " + dataSet1.count());
        	    
        	    
        	    
        	   // Dataset<Row> dataSet1 = ((JavaMongoRDD<Document>)testRDD).toDF();
        	    
        	    
        	    
    		    
    		    /*End Example**************************************************/
    		    
    		    //System.out.println("Total RDD count before group: " + rddSorted1.count());
    		    //System.out.println("Total RDD count before group: " + rddSorted2.count());
    		    
        	    //For all columns
    		    //Dataset<Row> afterGroupBy = dataSet1.groupBy("Name", "Marks").count().
    		    	//	withColumnRenamed("count", "NameCount").orderBy(org.apache.spark.sql.functions.col("NameCount").desc());
    		    
    		    //For selected columns
    		    Dataset<Row> afterGroupBy = dataSet1.groupBy("Name", "Marks").count().
    		    		withColumnRenamed("count", "NameCount").
    		    		orderBy(org.apache.spark.sql.functions.col("NameCount").desc())
    		    		.select("Name", "NameCount").limit(2);
        	    afterGroupBy.show();
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
