package com.weblog.parser;


import static java.util.Arrays.asList;

import java.util.ArrayList;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
//import java.nio.file.Files;
import java.util.HashMap;
import java.util.zip.GZIPInputStream;

import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import com.mongodb.spark.MongoSpark;

import org.bson.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

public class WebLogParser implements Runnable{
	private String filePath = "";
	private String fileName = "";
	private int status = -2;
	private long recordsAffected = 0;
	private int threadIndex = -2;
	
	private String rootPath = "";
	private String appConfigPath = "";
	private MyLogger myLogger = null;
	private Methods methods = null;
	private static final String[] patternStack = new String[]{" ", " - ", " [", "] \"", "\" ", " ", " \"", "\" \"", "\" ", " "};
	private HashMap<String, String> monthMap = new HashMap<String, String>();
	
	private String driver = "";
	private String url = "";
	private String user = "";
	private String password = "";
	
	private String databaseName = "insight";
	private String collectionName ="web_log_201703_2";
	private MongoClientURI mongoClientURI = new MongoClientURI("mongodb://127.0.0.1:27017");
	private MongoClient mongoClient = null;
	private DB mongoDB = null;
	

	//constructor
	public WebLogParser(int threadIndex, String filePath) throws Exception{
		
		try {
			this.threadIndex = threadIndex;
			this.filePath = filePath;
			methods = new Methods();
			myLogger = new MyLogger("webparser" + threadIndex);
			
		} catch (Exception e) {
			throw e;
		}
	}

	//initializing the class objects
	private void initialize() throws Exception{
		try {
			methods = new Methods();
			myLogger = new MyLogger("ieee" + threadIndex);
			
			File file = new File(this.filePath);
			if(!file.exists()){
				throw new Exception("File Not Found : " + this.filePath);
			}
			this.fileName = file.getName();
			
			rootPath = methods.getClassPath(this);
			myLogger.log("rootPath : " + rootPath);
			
			appConfigPath = rootPath + File.separator + "config" + File.separator;
			myLogger.log("appConfigPath : " + appConfigPath);
			
			mongoClientURI = new MongoClientURI("mongodb://127.0.0.1:27017");
			
			monthMap.put("JAN", "01");
			monthMap.put("FEB", "02");
			monthMap.put("MAR", "03");
			monthMap.put("APR", "04");
			monthMap.put("MAY", "05");
			monthMap.put("JUN", "06");
			monthMap.put("JUL", "07");
			monthMap.put("AUG", "08");
			monthMap.put("SEP", "09");
			monthMap.put("OCT", "10");
			monthMap.put("NOV", "11");
			monthMap.put("DEC", "12");
		} catch (Exception e) {
			myLogger.exception("initialize() : " + e.toString());
			throw e;
		}
	}
	
	//run method override for multithreaidng
	@Override
	public void run() {
		
		//
		try {
			myLogger.log("run() : START..");
			//calling log processor
			processLogFile(this.filePath);
			
		}catch (Exception e) {
			myLogger.exception("run() : " + e.toString());
		}
	}

	
	//
	public long processLogFile(String filePath) throws Exception{
		File gunZipFile;
		InputStream fileInputStream = null;
		GZIPInputStream gzipInputStream=null;
	    BufferedInputStream bufferedInputStream = null;
		BufferedReader bufferedReader = null;
		long recordsAffected = 0;
		
		databaseName = "insight";
		collectionName = "web_log_201703_2";
		
		String domain = "";
		String ipAddress = "";
		String userID = "";
		String requestDateTime = "";
		String gmtOffSet = "";
		String url = "";
		String requestMethod = "";
		String httpVersion = "";
		String statusCode = "";
		String refererUrl = "";
		String userAgent = "";
		String fileId = "";
		String resourceType = "";
		String institutionId = "";
		String institutionIdOnly = "";
		String sessionId = "";
		String tmp = "";
		ArrayList<DBObject> dbDocList = new ArrayList<DBObject>();
		DBObject dbDocument = null;		
		double iTracker = 0.0;
		try {
			//
			iTracker = 1.0;
			myLogger.log("Processing Web Log File : " + filePath);
			
			//
			iTracker = 2.0;
			initialize();
			
			//
			iTracker = 3.0;
			//mongoClientURI = new MongoClientURI("mongodb://NOD-WIN-W5029.macmill.com:27017");
			mongoClient = new MongoClient(mongoClientURI);
			
			iTracker = 4.0;
			/*getting database from mongoDB */
			mongoDB = mongoClient.getDB(databaseName);
			
			iTracker = 5.0;
			/* getting collection from MongoDB */
			DBCollection dbCollection = mongoDB.getCollection(collectionName);
			
			iTracker = 6.0;
			//check for file if exists
			gunZipFile = new File(filePath);
			if(!gunZipFile.exists()){
				throw new FileNotFoundException("No file found at path : " + gunZipFile);
			}
			iTracker = 7.0;
			//getting file input stream
			
			iTracker = 8.0;
			//fileInputStream = Files.newInputStream(gunZipFile.toPath());
			fileInputStream = new FileInputStream(gunZipFile);
			iTracker = 9.0;
			//getting buffereed input stream
			bufferedInputStream = new BufferedInputStream(fileInputStream, 65535);
			iTracker = 10.0;
			//getting gun zipp input dtream
			gzipInputStream = new GZIPInputStream(bufferedInputStream);
			//
			iTracker = 11.0;
			bufferedReader = new BufferedReader(new InputStreamReader(gzipInputStream));
			
			long lineNo = 0;
			String line = "";
			while((line =  bufferedReader.readLine()) != null){
				dbDocument = null;
				lineNo++;
				iTracker = 13.0;
				//check for blank line
				if(line.trim().equalsIgnoreCase("")){continue;}
				iTracker = 14.0;
				if(lineNo == 1 && line.contains("format=")){
					continue;
				}
				iTracker = 15.0;
				tmp="";domain = "";ipAddress = "";userID = "";requestDateTime = "";gmtOffSet = "";url = "";
				url = "";requestMethod = "";httpVersion = "";statusCode = "";refererUrl = "";userAgent = "";fileId = "";
				fileId = "";resourceType = "";institutionId = "";institutionIdOnly = "";sessionId = "";
				tmp = line;
				String value = "";
				//myLogger.log("Line = " + line);
				
				iTracker = 17.0;
				//processing for all patterns
				int rowIndex = 0; //myDataTable.getRowCount();
				int patternNo = 0;
				int startIndex = 0;
				int endIndex = -1;
				iTracker = 20.0;
				for(patternNo = 0 ; patternNo < patternStack.length; patternNo++){
					value = "";
					endIndex = -1;
					try {
						iTracker = 21.0;
						//operation without substring
						endIndex = line.indexOf(patternStack[patternNo], startIndex);
						iTracker = 22.0;
						if(endIndex >= 0){
							iTracker = 23.0;
							value =  line.substring(startIndex, endIndex);
							iTracker = 24.0;
							startIndex = endIndex + patternStack[patternNo].length(); 
						}else{
							iTracker = 25.0;
							startIndex = startIndex + patternStack[patternNo].length();
						}
						
						iTracker = 26.0;
						//switch case for 
						switch(patternNo){
							//domain
							case 0:{
								iTracker = 50.0;
								domain = value.trim();
								iTracker = 50.1;
								break;
							}
							//ip address
							case 1:{
								iTracker = 51.0;
								ipAddress = value.trim();
								iTracker = 51.1;
								institutionIdOnly = "-";
								iTracker = 51.2;
								if(institutionId.trim().contains("~")){
									iTracker = 51.4;
									String tmpInstitute[] = institutionId.split("~");
									iTracker = 51.5;
									if(tmpInstitute.length > 2){
										iTracker = 51.6;
										institutionIdOnly = tmpInstitute[1];
									}else{
										iTracker = 51.7;
										institutionIdOnly = tmpInstitute[1];
									}
									iTracker = 51.8;
								}else{
									iTracker = 51.9;	
								}
								iTracker = 51.10;
								break;
							}
							//userId
							case 2:{
								iTracker = 52.0;
								userID = value.trim();
								iTracker = 52.1;
								break;
							}
							//request date time and GMT offset
							case 3:{
								iTracker = 53.0;
								String[] tmpTime = value.trim().split(" ");
								iTracker = 53.1;
								String[] tmpData = tmpTime[0].replaceAll("/", ":") .split(":");
								iTracker = 53.2;
								requestDateTime = tmpData[2]+ "-" + monthMap.get(tmpData[1].trim().toUpperCase()) + "-" + tmpData[0] + " " + tmpData[3] + ":" +tmpData[4] + ":" + tmpData[5];  
								iTracker = 53.3;
								gmtOffSet = tmpTime[1];
								iTracker = 53.4;
								tmpTime = null;
								iTracker = 53.5;
								break;
							}
							//url, request type and Http version
							case 4:{
								iTracker = 54.0;
								String[] tmpUrl = value.trim().split(" ");
								iTracker = 54.1;
								requestMethod = tmpUrl[0].trim().toUpperCase();
								iTracker = 54.2;
								url = tmpUrl[1].trim();
								iTracker = 54.3;
								httpVersion = tmpUrl[2].trim();
								iTracker = 54.4;
								resourceType = "";
								tmpUrl = null;
								iTracker = 54.5;
								break;
							}
							//status Code
							case 5:{
								iTracker = 55.0;
								statusCode = value.trim();
								iTracker = 55.1;
								break;
							}
							//bytesSent
							case 6:{
								iTracker = 56.0;
								//userID = value.trim();
								iTracker = 56.1;
								break;
							}
							//refererUrl
							case 7:{
								iTracker = 57.0;
								refererUrl = value.trim();
								iTracker = 57.1;
								break;
							}
							//userAgent
							case 8:{
								iTracker = 58.0;
								userAgent = value.trim().replaceAll("'", "''");
								iTracker = 58.1;
								break;
							}
							//session id
							case 9:{
								iTracker = 59.0;
								sessionId = value.trim();
								iTracker = 59.1;
								break;
							}
							//default case
							default:{
								iTracker = 60.0;
								break;
							}
						}//end switch Case
						
						iTracker = 70.0;
						
					} catch (Exception e) {
						myLogger.log("Line No=" + lineNo + " : Pattern No=" + patternNo + " : pattern=" + patternStack[patternNo] + " : startIndex=" + startIndex + " : endIndex=" + endIndex + " : iTacker=" + iTracker + " : " + e.toString() +  " : Line = " + line);
					}
				}
				
				iTracker = 80.0;
				dbDocument = new BasicDBObject("domin", domain)
							.append("ip_address", ipAddress)
							.append("institution_details", new BasicDBObject("institution_id", institutionId).append("institution_id_only", institutionIdOnly))
							.append("user_id", userID)
							.append("request_date_time", requestDateTime)
							.append("gmt_offset", gmtOffSet)
							.append("requested_method", requestMethod)
							.append("url", url)
							.append("http_version", httpVersion)
							.append("resource_type", resourceType)
							.append("status_code", statusCode)
							.append("referel_url", refererUrl)
							.append("user_agent", userAgent)
							.append("session_id", sessionId)
							.append("ip_address", ipAddress);
				iTracker = 81.0;
				
				//bulkWriteOperation.insert(dbDocument);
				dbDocList.add(dbDocument);
				
				iTracker = 82.0;
				//MyDataTable code bulk update for 1000 records
				if(lineNo%5000 == 0){
					try {
						iTracker = 83.0;
						WriteResult writeResult = dbCollection.insert(dbDocList, WriteConcern.NORMAL);
						iTracker = 84.0;
						recordsAffected = recordsAffected + writeResult.getN();
						iTracker = 85.0;
						myLogger.info("LineNo=" + lineNo + " : " + recordsAffected + " : Records affected");
					} catch (Exception e) {
						myLogger.exception("LineNo=" + lineNo + " : iTracker=" + iTracker + " : " + e.toString());
					}
					iTracker = 86.0;
					dbDocList = null;
					dbDocList = new ArrayList<DBObject>();
					System.gc();
				}
				
			}//end while loop for lines
			
			
			try {
				iTracker = 90.0;
			//	WriteResult writeResult = dbCollection.insert(dbDocList, WriteConcern.NORMAL);
				iTracker = 91.0;
			//	recordsAffected = recordsAffected + writeResult.getN();
				iTracker = 92.0;
				myLogger.info("LineNo=" + lineNo + " : " + recordsAffected + " : Records affected");
			} catch (Exception e) {
				myLogger.exception("LineNo=" + lineNo + " : iTracker=" + iTracker + " : " + e.toString());
			}
			iTracker = 95.0;
			dbDocList = null;
			System.gc();
			dbDocList = new ArrayList<DBObject>();
			
			return recordsAffected;
		} catch (Exception e) {
			myLogger.exception("processLogFile : " + recordsAffected + " : iTracker=" + iTracker + " : " + e.toString());
			throw e;
		}finally{
			myLogger.info(recordsAffected + " : Records affected for file : " + filePath);
			
			try {fileInputStream.close();} catch (Exception e2) {}
			try {bufferedInputStream.close();} catch (Exception e2) {}
			try {gzipInputStream.close();} catch (Exception e2) {}
			try {bufferedReader.close();} catch (Exception e2) {}
			
			try{
				String movePath = "";
				File f = new File(filePath);
				if(recordsAffected > 0){
					movePath = f.getParent() + File.separator + "processed" + File.separator + fileName; 
				}else{
					movePath = f.getParent() + File.separator + "error" + File.separator + fileName;
				}
				methods.Move(filePath, movePath, Methods.FILE_COPY_WITH_TIMESTAMP);
			}catch(Exception e){
				myLogger.exception("Finally : moving file : " + this.filePath + " : " + e.toString());
			}
			
			//
			try{mongoClient.close();}catch(Exception e){}
			System.gc();
		
		
		}
	}
	
}
