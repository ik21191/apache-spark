package com.spark;

//import static spark.Spark.get;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * Hello world!
 *
 */
public class App 
{
    @SuppressWarnings("serial")
	public static void main( String[] args )throws Exception
    {
    	//get("/hello", (req, res) -> "Hello World");
    	System.out.println( "Process started....." );
    	 
        //SparkConf conf = new SparkConf().setAppName("firstSparkProject").setMaster("local[*]");
        //JavaSparkContext sc = new JavaSparkContext(conf);
        //JavaSparkContext sc = new JavaSparkContext("local", "firstSparkProject");
        JavaSparkContext sc = new JavaSparkContext("spark://localhost:7077", "firstSparkProject");
        String path = "E:/spark/Log/ieee/201703/w1.denial.01Mar2017-0200PM.gz";
 
        System.out.println("Trying to open: " + path);
 
        JavaRDD<String> lines = sc.textFile(path.toString());
        //System.out.println("Lines count: " + lines.toString());
        System.out.println("Lines count: " + lines.count());
     // RDD is immutable, let's create a new RDD which doesn't contain empty lines
        // the function needs to return true for the records to be kept
        lines = lines.filter(new Function<String, Boolean>() {
          @Override
          public Boolean call(String s) throws Exception {
        	  //System.out.println(s);
            if(s == null || s.trim().length() < 1) {
              return false;
            }
            if(s.contains("error")) {
            	return false;
            }
            return true;
          }
        });

        System.out.println("Non enpty without error String lines are : " + lines.count());
        //lines.saveAsTextFile("e://pqr/");
        //Thread.sleep(10000);
        sc.stop();
        sc.close();
        System.out.println("Process end.");
    }
    
}
