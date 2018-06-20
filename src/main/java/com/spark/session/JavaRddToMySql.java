package com.spark.session;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

public class JavaRddToMySql {
	static String databaseName = "ksv_ieee_201709";
	static String collectionName = "counter_20170905_reconcile09_1st";
	static String mongoDBUrl = "mongodb://127.0.0.1/";
	static SparkSession spark = null;
	static List<Document> allDocuments = new ArrayList<>();
	private static final String MYSQL_USERNAME = "root";
	private static final String MYSQL_PWD = "root";
	private static final String MYSQL_CONNECTION_URL = "jdbc:mysql://localhost:3306/javardd?user=" + MYSQL_USERNAME + "&password=" + MYSQL_PWD;

    public static void main( String[] args )throws Exception {
    	spark = SparkSession.builder().master("local").appName("MongoSparkConnectorIntro")
				.config("spark.mongodb.input.uri", mongoDBUrl + databaseName + "." + collectionName)
				.config("spark.mongodb.output.uri", mongoDBUrl + databaseName + "." + collectionName).getOrCreate();
		
			    System.out.println("Created spark session.");
    		    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
    		    
    		    /*Start Example: Read data from MongoDB************************/
    		    
    		    JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);
    		    SparkSession session = spark.newSession();
    		    /*session.table("abc").write().jdbc(MYSQL_CONNECTION_URL, "abc", );
    		    *
    		    *//*End Example**************************************************/
    		    
    		    Properties properties = new Properties();
    	        properties.put("user", "root");
    	        properties.put("password", "root");
    	        properties.put("driver", "com.mysql.jdbc.Driver");
    	        Dataset<Row> dataSet = session.read().jdbc("jdbc:mysql://localhost:3306/test?useSSL=false",
    	        		 "(select t1.* from test1 t1, test2 t2 where t1.id = t2.id) as t", properties); // join example
    	        Dataset<Row> dataSet1 = dataSet.where(dataSet.col("marks").equalTo(99));
    	        dataSet1.show();
    	        System.out.println("Getting count : " + dataSet.count());
    	        Row row = dataSet.first();
    	        System.out.println(row.toString());
    	        
    	        Dataset<Row> nameColumn = dataSet.select("marks");
    	        Row rowMarks = nameColumn.first();
    	        long firstMarks = Math.round(rowMarks.getDouble(0));
    	        System.out.println("Display first marks : " + firstMarks);
    	        jsc.close();
    }
    
}
