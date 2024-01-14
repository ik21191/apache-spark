package com.spark.session;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class SparkSessionReadFile {
  static SparkSession spark = null;

  public static void main(String[] args ) {
    String fileName = "D:\\rawData\\2020_lar\\2020_ts.txt";
	if (args.length > 0 ) {
	  fileName = args[0];
	  System.out.println("************************* File has been read from command line input *************************");
	} else {
	  System.out.println("************************* File name has been set from constant value *************************");
	}
	System.out.println("============ FileName: " + fileName);
	System.setProperty("hadoop.home.dir", "D:\\hadoop\\");
	spark = SparkSession.builder()
	    .master("local")
		.appName("MongoSparkConnectorIntro")
		.getOrCreate();

	System.out.println("Created spark session.");
	Dataset<Row> rowDataset= spark.read().format("csv").option("sep", "|").option("inferSchema", "true")
	    .option("header", "true")
		.load(fileName);

	//System.out.println(rowDataset.count());
	System.out.println("Grouping data.................");
	Dataset<Row> afterGroupBy = rowDataset.groupBy("respondent_name").count()
	    .withColumnRenamed("count", "NameCount")
		.orderBy(org.apache.spark.sql.functions.col("NameCount").desc());

	String output_file = "d://output.csv";
	System.out.println("***************** Output file: " + output_file);
	afterGroupBy.printSchema();
	afterGroupBy.show(false);
	// location get client information
	afterGroupBy.repartition(1)
	    .write()
		.mode(SaveMode.Overwrite)
		.format("csv")
		.option("header", true)
		.save(output_file);

	afterGroupBy.show();
  }
}
