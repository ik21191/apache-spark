package com.weblog.parser;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.net.URLDecoder;
import java.text.SimpleDateFormat;
import java.util.Date;

public class MyLogger {

	private String user = "";
	private String logFolderPath = "";
	private String logFilepath = "";
	private String logType = "";
	private String logFileName = "";
	private String date ="yyyymmdd";
	private boolean enablePrintln = true;
	
	//default constructor
	public MyLogger(){
		initialize();
	}
	
	//Parameterized constructor for user name
	public MyLogger(String user) {
		this.user = user;
		initialize();
	}
	
	public String getLogFolderPath() {
		return logFolderPath;
	}

	public void setLogFolderPath(String logFolderPath) {
		this.logFolderPath = logFolderPath;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}
	
	
	//method to initialize 
	private void initialize(){
		
		try {
			if(this.user == null){
				this.user = "";
			}else{
				this.user = this.user.trim();
			}
			//getting root class path
			logFolderPath = getClassPath(this) + File.separator + "logs";
			
		} catch (Exception e) {
			
		}
	}
	
	//method to get execution path
	private String getClassPath(Object ClassObject){
	       try {
	           String temp = null;
	           temp = new File(ClassObject.getClass().getProtectionDomain().getCodeSource().getLocation().getPath()).getParent();
	           temp = URLDecoder.decode(temp, "UTF-8");
	           return temp;
	       } catch (Exception e) {return "";}
	   }
	
	//simple log
	public boolean log(String message){
		logType = "";
		return writeLog(message);
	}
	//INFO level log
	public boolean info(String message){
		logType = "INFO";
		return writeLog(message);
	}
	//WARNING level log	
	public boolean warning(String message){		
		logType = "WARNING";
		return writeLog(message);
	}
	//ERROR level log
	public boolean error(String message){
		logType = "ERROR";
		return writeLog(message);
	}
	//EXCEPTION level log
	public boolean exception(String message){
		logType = "EXCEPTION";
		return writeLog(message);
	}
	
	//method to write log
	private boolean writeLog(String message){
		
		try {
			if(user.trim().equals("")){
				logFileName = new SimpleDateFormat("yyyyMMdd").format(new Date()).toString() + ".log";
			}else{
				logFileName = user + "_" +  new SimpleDateFormat("yyyyMMdd").format(new Date()).toString() + ".log";
			}
			logFilepath = logFolderPath + File.separator + logFileName;
			return writeLog(logType + " : " + message, logFilepath, false);
			
		} catch (Exception e) {
			return false;
		}
	}
	
	//method to write log
	public boolean writeLog(String message, String logFilePath, boolean deleteExistingLogFile){
	       BufferedWriter BuffWriter = null;
	       
	       try{
	           
	           //System.out.println(new Date().toString() + " : LogFilePath="  +  sFilePath + " : bDeleteExistingFile=" + bDeleteExistingFile);
	           
	           //check for existing file
	           File f = new File(logFilePath.trim());
	           if(f.exists() && deleteExistingLogFile){
	               try{f.delete();} catch(Exception e){}
	           }

	           //making directory if not exists
	           File Dir = new File(f.getParent());
	           if (!Dir.exists()) { Dir.mkdirs();}

	           //adding time stamp on message text
	           if(enablePrintln){
	        	   System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date()).toString() + " : " + user + " : " + message);
	           }
	           
	           BuffWriter = new BufferedWriter(new FileWriter(logFilePath , true));
	           BuffWriter.write(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date()).toString() + " : " + user + " : " + message);
	           BuffWriter.newLine();
	           
	           return true;
	       }
	       catch(Exception exp){
	           System.out.println(new Date().toString() + "Excception in log : " + exp.toString());
	           exp.printStackTrace();
	           return false;
	       }
	       finally{
	           try{BuffWriter.close();} catch(Exception e){}
	           logType ="";
	       }
	   }
	
}
