package com.spark;

//import static spark.Spark.get;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.WriteConfig;
import com.weblog.parser.MyDocument;
import com.weblog.parser.WebLogParser;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;

import org.bson.Document;
import static java.util.Arrays.asList;

import java.util.HashMap;
import java.util.Map;
/**
 * Hello world!
 *
 */
public class IinsertToMongo 
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
    		    Map<String, String> writeOverrides = new HashMap<String, String>();
    		    writeOverrides.put("collection", "spark2");
    		    writeOverrides.put("writeConcern.w", "majority");
    		    WriteConfig writeConfig = WriteConfig.create(jsc).withOptions(writeOverrides);
    		    
    		    String path = "E:/abc/linescount.txt";
    		    JavaRDD<String> lines1 = jsc.textFile(path);
    		    JavaRDD<String> lines2 = jsc.textFile(path);
    		    
    		    // Create a RDD of 10 documents
    		    
    		    /*JavaRDD<Document> documents = jsc.parallelize(asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).map
    		            (new Function<Integer, Document>() {
    		      public Document call(final Integer i) throws Exception {
    		          return Document.parse("{test: " + i + "}");
    		      }
    		    });*/
    		    
    		    JavaRDD<String> finalLines = null;
    		    
    		    for(int i = 0; i < 3; i++) {
    		    	if(finalLines == null) {
    		    		finalLines = lines1;
    		    	} else {
    		    		finalLines = finalLines.union(lines1);
    		    	}
    		    	
    		    }
    		    
    		    //System.out.println("Lines count: " + finalLines.count());
    		    JavaRDD<Document> documents = finalLines.map(
    		            (new Function<String, Document>() {
    		      public Document call(final String line) throws Exception {
    		    	  //String s = "{test: " + line + "}";
    		    	  System.out.println(line);
    		          //return MyDocument.getDoc(line);
    		    	  Document doc = new Document();
    		    	  doc.append("Name", line);
    		    	  return doc;
    		      }
    		    }));

    		    //WebLogParser parser = new WebLogParser(1, "E:/spark/Log/w1.access.03Mar2017-0200PM.gz");
    		    //System.out.println(parser.processLogFile("E:/spark/Log/w1.access.03Mar2017-0200PM.gz"));
    		    /*Start Example: Save data from RDD to MongoDB*****************/
    		    MongoSpark.save(documents, writeConfig);
    		    /*End Example**************************************************/

    		    jsc.close();
    }
    
}
