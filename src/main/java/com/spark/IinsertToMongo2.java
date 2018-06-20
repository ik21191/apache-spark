package com.spark;

//import static spark.Spark.get;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.WriteConfig;
import com.weblog.parser.MyDocument;
import com.weblog.parser.WebLogParser;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

import org.bson.Document;
import static java.util.Arrays.asList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
/**
 * Hello world!
 *
 */
public class IinsertToMongo2 
{
    @SuppressWarnings("serial")
	public static void main( String[] args )throws Exception
    {
    	SparkSession spark = SparkSession.builder()
    		      .master("local")
    		      .appName("MongoSparkConnectorIntro")
    		      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/db.testCol")
    		      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/db.testCol")
    		      .getOrCreate();
    			System.out.println("Created spark session.");
    		    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
    		    
    		 // Create a custom WriteConfig
    		    Map<String, String> writeOverrides = new HashMap<>();
    		    writeOverrides.put("collection", "spark2");
    		    writeOverrides.put("writeConcern.w", "majority");
    		    WriteConfig writeConfig = WriteConfig.create(jsc).withOptions(writeOverrides);
    		    
    		    String path = "E:/abc/linescount.txt";
    		    //String path = "E:/spark/myLogs/ieee/w1.access.03Mar2017-0200PM.gz";
    		    JavaRDD<String> lines = jsc.textFile(path.toString());
    		    System.out.println("Lines count: " + lines.count());

    		    // Create a RDD of 10 documents
    		    
    		    /*JavaRDD<Document> documents = jsc.parallelize(asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).map
    		            (new Function<Integer, Document>() {
    		      public Document call(final Integer i) throws Exception {
    		          return Document.parse("{test: " + i + "}");
    		      }
    		    });*/
    		    
    		    /*JavaRDD<Document> documents = lines.map(
    		            (new Function<String, Document>() {
    		      public Document call(final String line) throws Exception {
    		    	  Document document = new Document();
    		    	  document.append("name", line);
    		    	  //String s = "{test: " + line + "}";
    		    	  System.out.println(line);
    		          return document;
    		      }
    		    }));*/
    		    
    		    //flatMap example
    		    JavaRDD<Document> documents = lines.flatMap(
    		            (new FlatMapFunction<String, Document>() {
    		      public Iterator<Document> call(final String line) throws Exception {
    		    	  List<String> list = Arrays.asList(line.split(" "));
    		    	  List<Document> documentList = new ArrayList<>();
    		    	  for(String element : list) {
    		    		  Document document = new Document();
    		    		  document.append("name", element);
    		    		  documentList.add(document);
    		    	  }
    		          return documentList.iterator();
    		      }
    		    }));
    		    
    		    JavaPairRDD<Document, Integer> javaPairRdd = documents.mapToPair(new PairFunction<Document, Document, Integer>() {
    		    	@Override
    		    	public Tuple2<Document, Integer> call(Document document) throws Exception {
    		    		return new Tuple2<>(document, 1);
    		    	}
				});
    		    
    		    JavaPairRDD<Document, Integer> javaPairRdd1 = javaPairRdd.reduceByKey(new Function2<Integer, Integer, Integer>() {
					@Override
					public Integer call(Integer val1, Integer val2) throws Exception {
						return val1 + val2;
					}
				}); 
    		    
    		    List<Tuple2<Document, Integer>> filteredList = javaPairRdd1.collect();
    		    for(Tuple2<Document, Integer> tuple: filteredList) {
    		    	System.out.println(tuple._1.toString() + " : " + tuple._2);
    		    }
    		    //JavaPairRDD<Document, Integer> javaPairRdd1 = documents.map(doc->new Tuple2<Document, Integer>(doc, 1)));
    		    
 /*   		    JavaRDD<Document> documents = lines.map(
    		            (new Function<String, Document>() {
    		      public Document call(final String line) throws Exception {
    		    	  //String s = "{test: " + line + "}";
    		    	  System.out.println(line);
    		          return MyDocument.getDoc(line);
    		      }
    		    }));
 */   		    

    		    //WebLogParser parser = new WebLogParser(1, "E:/spark/Log/w1.access.03Mar2017-0200PM.gz");
    		    //System.out.println(parser.processLogFile("E:/spark/Log/w1.access.03Mar2017-0200PM.gz"));
    		    /*Start Example: Save data from RDD to MongoDB*****************/
    		    //MongoSpark.save(documents, writeConfig);
    		    MongoSpark.save(documents, writeConfig);
    		    /*End Example**************************************************/

    		    jsc.close();
    }
    
}
