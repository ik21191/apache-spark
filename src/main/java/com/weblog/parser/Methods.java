package com.weblog.parser;

import java.io.*;
import java.math.BigInteger;
import java.net.*;
import java.security.MessageDigest;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.text.*;
import java.util.*;
import java.util.zip.*;

public class Methods{
   
   public final static int FILE_DO_NOTHING = 0;
   public final static int FILE_REPLACE = 1;
   public final static int FILE_APPEND = 2;
   public final static int FILE_COPY_WITH_TIMESTAMP = 3;
   public final static int FILE_DELETE = 4;
   
   private String gsLogFolderPath = "";
   private String gsLogFilePath = "";
   private String gsLogFileName = "";
   private String gsRootPath =System.getProperty("user.dir");

   private String user = "";

   public String getUser() {
       return user;
   }

   public void setUser(String user) {
       this.user = user;
   }

//########################### INITIALIZATION/CONSTRUCTOR #########################################################
//################################################################################################################
   private void initialize() throws Exception{
       Properties props = new Properties();
       InputStream isPropFile = getClass().getResourceAsStream("config.properties");
	    try {
           gsRootPath = getClassPath(this);
           
	    }catch (Exception e) {
           throw new Exception("initialize : " + e.toString());
       }finally{
       }
	}

   //constructor
   public Methods(){
       try{
           initialize();
       }catch(Exception e){
           println("Methods() : " + e.toString());
       }
   }

   //constructor with folder log path
   public Methods(String sLogFolderPath){
       try{
           initialize();
           setLogFolderPath(sLogFolderPath);
       }catch(Exception e){
           println("Methods(String sLogFolderPath) : " + e.toString());
       }
   }
   
   public void setLogFolderPath(String sLogFolderPath){
       gsLogFolderPath = null;
       gsLogFolderPath = sLogFolderPath;
   }

   public String getLogFolderPath(){
       return gsLogFolderPath;
   }

   public String getLogFilePath(){
       return gsLogFilePath;
   }
   
   public void setLogFilePath(String sLogFilePath ){
       gsLogFilePath = sLogFilePath;
   }

   public String getLogFileName(){
       return gsLogFileName;
   }
   
   public void setLogFileName(String sLogFileName ){
       gsLogFileName = sLogFileName;
   }
   

//################################# DATABASE RESULTSET METHODS ###################################################
//################################################################################################################
   public int getRowCountFromResultSet(ResultSet rs){
		int iRowCount = -2;
		try{
			if(rs != null || rs.next())
			rs.last();
			iRowCount = rs.getRow();
		}
		catch (Exception e) {
			println("getRowCountFromResultSet : " + iRowCount);
			iRowCount = -2;
		}
		finally{
			try{rs.beforeFirst();} catch(Exception e){}
		}
		return iRowCount;
	}



//########################### GENERIC GLOABL UNIVERSAL METHODS ###################################################
//################################################################################################################

   //method to check if remote host is reachable
   public boolean isHostReachable(String sIPAddress){
       try {
           
           Process p1 = java.lang.Runtime.getRuntime().exec("ping " + sIPAddress);
           int iRetVal = p1.waitFor();
           if(iRetVal == 0){return true;}
           else{return false;}
           
           //InetAddress inet = InetAddress.getByName(sIPAddress);
           //return (inet.isReachable(5000) ? true : false);
       } catch (Exception e) {
           return false;
       }
   }

   
//########################### OBJECT SERIALIZATION ##############################################################
//################################################################################################################

   //method to Serialize an Object
   public boolean SerializeObject(Object object, String sFilePath){
           boolean bRetVal = false;
           File f = null;
           FileOutputStream fos = null;
           ObjectOutputStream oos = null;
           try {
               f = new File(sFilePath);

               if(f.exists()){
                   f.delete();
               }

               File dir = new File(f.getParent());
               dir.mkdirs();
               f.createNewFile();

               fos = new FileOutputStream(f);
               oos = new ObjectOutputStream(fos);

           } catch (Exception e) {
           }finally{
               try{oos.writeObject(object);}catch(Exception e){}
               try{oos.close();}catch(Exception e){}
               try{fos.close();}catch(Exception e){}
               f = null;
               return bRetVal;
           }
       }


   //method to De- Serialize an Object
   public Object DeSerializeObject(String sFilePath){
           File f = null;
           FileInputStream fis = null;
           ObjectInputStream ois = null;
           Object object = null;
           try {
               f = new File(sFilePath);

               if(!f.exists()){
                   throw new Exception("File Not found for " + sFilePath);
               }

               fis = new FileInputStream(f);
               ois = new ObjectInputStream(fis);

               try{object = ois.readObject();}catch(Exception e){}

           } catch (Exception e) {
           }finally{
               try{ois.close();}catch(Exception e){}
               try{fis.close();}catch(Exception e){}
               f = null;
               return object;
           }
       }

   

//########################### STRING/STREAM METHODS ##############################################################
//################################################################################################################
   // convert InputStream to String Array
   public String[] getStringArrayFromInputStream(InputStream is) {

       BufferedReader br = null;
       ArrayList<String> al = new ArrayList<String>();
       String line;
       String[] sa = null;
       try {

           br = new BufferedReader(new InputStreamReader(is));
           while ((line = br.readLine()) != null) {
               al.add(line);
           }
       } catch (Exception e) {}
       finally {
           try { br.close();} catch (Exception e) {}
           br = null;
           sa = al.toArray(new String[al.size()]);
           return sa;
       }
   }

   // convert InputStream to String
   public String getStringFromInputStream(InputStream is) {

       BufferedReader br = null;
       StringBuilder sb = new StringBuilder();

       String line;
       try {

           br = new BufferedReader(new InputStreamReader(is));
           while ((line = br.readLine()) != null) {
               sb.append(line);
           }
       } catch (Exception e) {}
       finally {
           try { br.close();} catch (Exception e) {}
           br = null;
           return sb.toString();
       }
   }

   //method replace all 'n' space & 'tab' characters into single space.
   public String getSingleSpaceString(String sInputString){
       String sOutputString = "";
       try{

           //loop replace all TAB by single space
           while(sInputString.contains("	")){
               sInputString = sInputString.replaceAll("	", " ").trim();
           }

           //loop to replace all double space by single space
           while(sInputString.contains("  ")){
               sInputString = sInputString.replaceAll("  ", " ").trim();
           }

       }catch(Exception e){

       }finally{
           sOutputString = sInputString;
           sInputString = null;
           return sOutputString;
       }
   }

   //method remove/vannish all 'n' space & 'tab' characters
   public String getSpaceFreeString(String sInputString){
       String sOutputString = "";
       try{

           //loop to remove all 'tab's
           while(sInputString.contains("	")){
               sInputString = sInputString.replaceAll("	", "").trim();
           }

           //loop to remove all space space
           while(sInputString.contains(" ")){
               sInputString = sInputString.replaceAll(" ", "").trim();
           }

       }catch(Exception e){

       }finally{
           sOutputString = sInputString;
           sInputString = null;
           return sOutputString;
       }
   }

//########################### GENERIC UTILITY METHODS ############################################################
//################################################################################################################
   //method to get absolute class/jar path for class object passed as parameter
   public String getClassPath(Object ClassObject){
       try {
           String temp = null;
           temp = new File(ClassObject.getClass().getProtectionDomain().getCodeSource().getLocation().getPath()).getParent();
           temp = URLDecoder.decode(temp, "UTF-8");
           return temp;
       } catch (Exception e) {return "";}
   }
   
   //method to sort hashmap by values
   public HashMap sortByValues(HashMap map) { 
      List list = new LinkedList(map.entrySet());
      // Defined Custom Comparator here
      Collections.sort(list, new Comparator() {
           public int compare(Object o1, Object o2) {
              return ((Comparable) ((Map.Entry) (o1)).getValue())
                 .compareTo(((Map.Entry) (o2)).getValue());
           }
      });
      
       List array = new LinkedList();
       if (list.size() >= 3) // Make sure you really have 3 elements
       {
           array.add(list.get(list.size()-1)); // The last
           array.add(list.get(list.size()-2)); // The one before the last
           array.add(list.get(list.size()-3)); // The one before the one before the last
       } else {array = list;}



      // Here I am copying the sorted list in HashMap
      // using LinkedHashMap to preserve the insertion order
      HashMap sortedHashMap = new LinkedHashMap();
      for (Iterator it = array.iterator(); it.hasNext();) {
             Map.Entry entry = (Map.Entry) it.next();
             sortedHashMap.put(entry.getKey(), entry.getValue());
      } 
      return sortedHashMap;
 }

   //method to get exception string with class & method name
   public String getExceptionString(Exception e){
       StringBuffer sb = new StringBuffer();
       int icount = 0;
       try {
           
           sb.append("Exception : ");
           for(int i = e.getStackTrace().length -1 ; i >= 0 ; i--){
               sb.append(e.getStackTrace()[i].getClassName());
               sb.append(".");
               sb.append(e.getStackTrace()[i].getMethodName());
               sb.append(" at Line ");
               sb.append(e.getStackTrace()[i].getLineNumber());
               sb.append(" : " );
           }
           sb.append(e.getMessage().toUpperCase().replaceAll("JAVA.LANG.", ""));
           
           return sb.toString();
       } catch (Exception e1) {
           return (e.toString() + " : " + e1.toString());
       }
   }
   

//################## LOGGING PRINTING/CONSOLE DISPLAY METHODS ####################################################
//################################################################################################################
   public boolean println(String sMessage){
       String sLogFilePath = "";
       String sLogFileName = "";
       String sLogFolderPath = "";
       try{
           
           //check for log file name
           if(gsLogFileName.trim().equalsIgnoreCase("")){
               if(user.trim().equalsIgnoreCase("")){
                   sLogFileName = getDateTime("yyyyMMdd") + ".log";
               }else{
                   sLogFileName = getDateTime("yyyyMMdd") + "_" + user + ".log";
               }
           }else{
               sLogFileName = gsLogFileName;
           }
           //check for user log
           if(user.trim().equalsIgnoreCase("")){
               
           }
           
           ///check for log folder path
           if(gsLogFolderPath.trim().equalsIgnoreCase("")){
               sLogFolderPath = gsRootPath + File.separator + "logs";
           }else{
               sLogFolderPath = gsLogFolderPath;
           }
           
           //check for log file path
           if(gsLogFilePath.trim().equalsIgnoreCase("")){
               sLogFilePath = sLogFolderPath + File.separator + sLogFileName;
           }else{
               sLogFilePath = gsLogFilePath;
           }
           
           //writing log file
           println(sMessage, sLogFilePath, false);
           return true;
       }
       catch (Exception exp) {return false;}
   }

   public boolean println(String sMessage, String sFilePath){
       return println(sMessage, sFilePath , false);
   }

   public boolean println(String sMessage, String sFilePath, boolean bDeleteExistingFile){
       BufferedWriter BuffWriter = null;
       try{
           
           //System.out.println(new Date().toString() + " : LogFilePath="  +  sFilePath + " : bDeleteExistingFile=" + bDeleteExistingFile);
           
           //check for existing file
           File f = new File(sFilePath.trim());
           if(f.exists() && bDeleteExistingFile){
               try{f.delete();} catch(Exception e){}
           }

           //making directory if not exists
           File Dir = new File(f.getParent());
           if (!Dir.exists()) { Dir.mkdirs();}

           //adding time stamp on message text
           System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date()).toString() + " : " + user + " : " + sMessage);
           
           BuffWriter = new BufferedWriter(new FileWriter(sFilePath , true));
           BuffWriter.write(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date()).toString() + " : " + user + " : " + sMessage);
           BuffWriter.newLine();
           
           f = null;
           Dir = null;
           
           return true;
       }
       catch(Exception exp){
           System.out.println(new Date().toString() + "Excception in printandwrite : " + exp.toString());
           exp.printStackTrace();
           return false;
       }
       finally{
           try{BuffWriter.close();} catch(Exception e){}
           BuffWriter = null;
       }
   }





//###################################### File METHODS ############################################################
//################################################################################################################

   //method to get all files from a particular folder path
   public File[] getAllFiles(String sFolderPath){

       File[] files = null;
       ArrayList<File> altemp = new ArrayList<File>();
       try {
           File folder = new File(sFolderPath);
           files = folder.listFiles();

           for(int i = 0 ; i < files.length ; i++){
               if(files[i].isFile()){
                   altemp.add(files[i]);
               }
           }

           files = null;
           files = altemp.toArray(new File[altemp.size()]);

       } catch (Exception e) {
           files =null;
       }
       finally{return files;}
   }

   //method to read complete file in string from classpath inside jar based on class object
   public String ReadFile_FromClassPath(String sFileName){
       String stemp = null;

       try {
           InputStream is = getClass().getClassLoader().getResourceAsStream(sFileName);
           stemp = getStringFromInputStream(is);

       } catch (Exception e) {
           stemp = null;
           throw new Exception("ReadFile_FromClassPath : " + e.toString());
       }
       finally{return stemp;}
   }

   //method to read complete file in string[] array from classpath inside jar based on class object
   public String[] ReadAllFileLines_FromClassPath(String sFileName){
       String[] satemp = null;

       try {
           InputStream is = getClass().getClassLoader().getResourceAsStream(sFileName);
           satemp = getStringArrayFromInputStream(is);

       } catch (Exception e) {
           satemp = null;
           throw new Exception("ReadAllFileLines_FromClassPath : " + e.toString());
       }
       finally{return satemp;}
   }

   //method reads all lines of a files into a String array from a file on HDD
   public String[] ReadAllFileLines(String sFilePath) throws Exception{
       File f = null;
       FileInputStream fstream = null;
       DataInputStream in = null;
       BufferedReader br = null;
       String strLine = "";
       String[] stemp = null;
       StringBuffer strbuff = new StringBuffer();
       try{
               //getting file oject
               f = new File(sFilePath);
               if(f.exists()){
                   //get object for fileinputstream
                   fstream = new FileInputStream(f);
                   // Get the object of DataInputStream
                   in = new DataInputStream(fstream);
                   //get object for bufferreader
                   br = new BufferedReader(new InputStreamReader(in,"UTF-8"));
                   //Read File Line By Line
                   while ((strLine = br.readLine()) != null){
                       strbuff.append(strLine + "##NL##");
                   }

                   stemp = strbuff.toString().split("##NL##");
               }else{
                   throw new Exception("File Not Found!!");
               }

               return stemp;
       }
       catch (Exception e) {
           throw new Exception("ReadAllFileLines : " + e.toString());
       }
       finally{
                //Close the input stream
               try{br.close();} catch (Exception e) {}
               try{fstream.close();} catch (Exception e) {}
               try{in.close();} catch (Exception e) {}
       }
   }

   //method reads all file into a string variable from a file on HDD
   public String ReadFile(String sFilePath) throws Exception{
       File f = null;
       FileInputStream fstream = null;
       String stemp = "";
       try{
           //getting file oject
           f = new File(sFilePath);
           //get object for fileinputstream
           fstream = new FileInputStream(f);
           //getting byte array length
           byte data[] =new byte[fstream.available()];
           //getting file stream data into byte array
           fstream.read(data);
           //storing byte array data into String
           stemp=new String(data);
           return stemp;
       }
       catch (Exception e) {
           throw new Exception("ReadFile : " + e.toString());
       }
       finally{
           try{fstream.close();} catch (Exception e) {}
       }
   }

   //method copies a file from one location to other location
   public int Copy(final String sSourceFile, final String sDestinationtFile, final int EXISITING_FILE_ACTION) throws Exception{
		OutputStream out = null;
		InputStream in = null;
		File fSrc = null;
		File fDest = null;
       File fDestDir = null;
       byte[] buf = null;
		int len = 0;
       int iFileCopied = 0;
       boolean bProcess = false;
		try{
           
           buf = new byte[4096];
           
			//reating source file object
			fSrc = new File(sSourceFile);
           if(!fSrc.exists()){throw new Exception("Source File Does not exists!! :" + sSourceFile);}
           
           //creating output file object
           fDest = new File(sDestinationtFile);
           
           //check for folder/directory
           if(fSrc.isDirectory()){
               File[] fSubFiles = fSrc.listFiles();
               
               //creating destination directory
               if(!fDest.exists()){fDest.mkdirs();}
               
               for(int i = 0 ; i < fSubFiles.length ; i++ ){
                   String sSourceSubFile = sSourceFile + File.separator + fSubFiles[i].getName();
                   String sDestinationtSubFile = sDestinationtFile + File.separator + fSubFiles[i].getName();
                                
                   
                   
                   iFileCopied = iFileCopied + Copy(sSourceSubFile, sDestinationtSubFile, EXISITING_FILE_ACTION);
               }
           
           //check for file 
           }else{
           
               //creating input stream of source file
               in = new FileInputStream(fSrc);

               //check for destination file parent directory
               fDestDir = fDest.getParentFile();
               if(!fDestDir.exists()){fDestDir.mkdirs();}


               //check for exisitng file
               //REPLACE EXISITNG FILE
               if(fDest.exists() && EXISITING_FILE_ACTION == FILE_REPLACE){
                   bProcess = true;
                   out = new FileOutputStream(fDest);

               //APPEND EXISITNG FILE
               }else if(fDest.exists() && EXISITING_FILE_ACTION == FILE_APPEND){//For Append the file.
                   bProcess = true;
                   out = new FileOutputStream(fDest,true);

               //COPY WITH TIMESTAMP WITH EXISITNG FILE
               }else if(fDest.exists() && EXISITING_FILE_ACTION == FILE_COPY_WITH_TIMESTAMP){//For Append the file.
                   bProcess = true;
                   String sTimeStamp = fDest.lastModified() + "_";
                   fDest = new File(fDest.getParent() + File.separator + sTimeStamp + fDest.getName());
                   out = new FileOutputStream(fDest);

               //DO NOTHING EXISITNG FILE
               }else if(fDest.exists() && EXISITING_FILE_ACTION == FILE_DO_NOTHING){//For Append the file.
                   bProcess = false;

               //file does not exists
               }else if(!fDest.exists()){
                   bProcess = true;
                   out = new FileOutputStream(fDest,true);

               }

               //loop to read buffer & copy in output file
               while ((len = in.read(buf)) > 0 && bProcess){
                   out.write(buf, 0, len);
               }

               iFileCopied = iFileCopied + 1;
           }
           
			return iFileCopied;
       }catch(Exception e){
           throw e;
		}finally{
			try{in.close();} catch (Exception e) {}
		    try{out.close();} catch (Exception e) {}
           in = null;
           out = null;
           fSrc = null;
           fDest = null;
           fDestDir = null;
		}
	}

   //method moves a file from one location to other location
   public int Move(String sSourcePath, String sDestinationPath , int EXISTING_FILE_ACTION){
       int iRetVal = 0;
       int iCopyCount = 0;
       int iDeleteCount = 0;
       try{

           //check if source & destination path are same.
           if(sSourcePath.equalsIgnoreCase(sDestinationPath)) iRetVal = 0;

           //copying file for movement
           iCopyCount = Copy(sSourcePath, sDestinationPath, EXISTING_FILE_ACTION);
           if(iCopyCount > 0){
               iDeleteCount = Delete(sSourcePath);
           }
           
           if(iCopyCount == iDeleteCount){
               iRetVal = iCopyCount;
           }else if(iCopyCount > iDeleteCount){
               iRetVal = iDeleteCount;
           }else if(iCopyCount < iDeleteCount){
               iRetVal = iCopyCount;
           }
           
       }catch(Exception exp){
           println("move : " + exp.toString());
           iRetVal = 0;
       }finally{return iRetVal;}
   }

   //method to delete file or folder
   public int Delete(String sTargetPath) throws Exception{
       File ftarget = null;
       int iRetVal = 0;
       try{
           
           ftarget = new File(sTargetPath);
           
           if(ftarget.isDirectory()){
               File[] fsubtargets = ftarget.listFiles();
               for(int i = 0 ; i < fsubtargets.length ; i++){
                   iRetVal = iRetVal + Delete(sTargetPath + File.separator + fsubtargets[i].getName());
               }
               ftarget.delete();;
           }else{
               ftarget.delete();
           }
           
           return iRetVal;
       }catch(Exception e){
           throw e;
       }finally{
           ftarget = null;
       }
   }      
   
   //method to read INI file (WINDOWS format)
   public String ReadINI(String Section, String Key , String FilePath){
		String sValue = "";
		int iRetVal = -2;
		String[] FileLines = null;
		String sTemp = "";
		boolean bKeyFound = false;
		boolean bSectionFound = false;
		int i = 0;
		int counter = 0;
		try{
			FileLines = ReadAllFileLines(FilePath);

			for( i= 0; i < FileLines.length ; i++){
				sTemp = FileLines[i].trim();
				if( sTemp.charAt(0) == '[' && sTemp.charAt(sTemp.length() -1) == ']')
				{
					if (sTemp.substring(1, sTemp.length() -1).equalsIgnoreCase(Section))
					{
						bSectionFound = true;
						counter = i + 1;
						while(!bKeyFound){




						}

					}
				}
				if (bKeyFound) break;
			}

			if(!bKeyFound) throw new Exception(" : Section :" + Section + " : key : " + Key + " :Not found");
		}
		catch (Exception exp) {
			iRetVal = -2;
		}
		return sValue;
	}//end READINI

   //method to get file extension of a file
   public String getFileExtension(File objFile){

           String sFileName = null;
           String sExtension = null;

           try {

               if(!objFile.exists()) throw new Exception("File does not exists");

               sFileName = objFile.getName();
               int i = sFileName.lastIndexOf('.');
               if (i > 0) {
                   sExtension = sFileName.substring(i+1).trim();
               }

           } catch (Exception e) {
               println("Methods.getFileExtension : " + e.toString());
               sExtension = "";
           }
           finally{return sExtension;}
       }

   public boolean WriteFile(String sFilePath , String sContent , boolean bDeleteExistingFile){
       BufferedWriter BuffWriter = null;
       try{

           //check for existing file
           File f = new File(sFilePath.trim());
           if(f.exists() && bDeleteExistingFile){
               try{f.delete();} catch (Exception e) {}
           }

           //making directory if not exists
           File Dir = new File(f.getParent());
           if (!Dir.exists()) { Dir.mkdirs();}

           BuffWriter = new BufferedWriter(new FileWriter(sFilePath , true));
           BuffWriter.write(sContent);
           BuffWriter.newLine();
           return true;
       }
       catch(Exception exp){
           System.out.println(new Date().toString() + "Excception in printfile : " + exp.toString());
           return false;
       }
       finally{
           try{BuffWriter.close();} catch(Exception e){}
           BuffWriter = null;
       }
   }


   //method to sort file object by ites name
   public File[] SortFilesByName(final File[] Files, final boolean bCaseSensitive, final boolean bSortDesecnding){
       
       File[] faFiles = null;
       Comparator<File> comparator = null;
       
       try{
           faFiles = Files.clone();
           if(faFiles == null) throw new Exception("Null File Array Object");
           
           //check if array consists of more than 1 file
           if(faFiles.length > 1){
              
               //creating Comparator
               comparator = new Comparator<File>(){

                   @Override
                   protected Object clone() throws CloneNotSupportedException {
                       return super.clone(); //To change body of generated methods, choose Tools | Templates.
                   }
                   
                   @Override
                   public int compare(File objFile1, File objFile2) {
                       String Str1 = objFile1.getName();
                       String Str2 = objFile2.getName();
                       
                       if(bCaseSensitive){
                           int iRetVal = objFile1.getName().compareTo(objFile2.getName());
                           return iRetVal;
                       }else{
                           int iRetVal = objFile1.getName().compareToIgnoreCase(objFile2.getName());
                           return iRetVal;
                       }
                   }
               };
               
               //Sorting Array
               if(bSortDesecnding){//Descending
                   
               }else{//Ascending
                   Arrays.sort(faFiles, comparator);
               }
           }
           
       }catch(Exception e){faFiles = null;}
       finally{return faFiles;}
       
       
   }
   
   //method to sort file object by ites name
   public File[] SortFilesByDate(final File[] Files, final boolean bSortDesecnding){
       
       File[] faFiles = null;
       Comparator<File> comparator = null;
       
       try{
           faFiles = Files.clone();
           if(faFiles == null) throw new Exception("Null File Array Object");
           
           //check if array consists of more than 1 file
           if(faFiles.length > 1){
              
               //creating Comparator
               comparator = new Comparator<File>(){

                   @Override
                   protected Object clone() throws CloneNotSupportedException {
                       return super.clone(); //To change body of generated methods, choose Tools | Templates.
                   }
                   
                   @Override
                   public int compare(File objFile1, File objFile2) {
                       String Str1 = objFile1.getName();
                       String Str2 = objFile2.getName();
                       if(bSortDesecnding){
                           int iRetVal = objFile1.getName().compareTo(objFile2.getName());
                           return iRetVal;
                       }else{
                           int iRetVal = objFile1.getName().compareToIgnoreCase(objFile2.getName());
                           return iRetVal;
                       }
                   }
               };
               
               //Sorting Array
               Arrays.sort(faFiles, comparator);
           }
           
       }catch(Exception e){faFiles = null;}
       finally{return faFiles;}
       
       
   }
   
   
   
//############################ PROPERTY File METHODS #############################################################
//################################################################################################################
   
    //method reads all property in file in hashmap
   public HashMap<String, String> readPropertyFile(String propertyFilePath) throws Exception {
       File propertyFile = null;
       InputStream inputStream = null;
       Properties properties = null;
       HashMap<String , String> propertyMap= new HashMap<String, String>();
       try{
           
           //creating file object
           propertyFile = new File(propertyFilePath);
           
           //check if property file exists on hard drive
           if(propertyFile.exists()) {
               inputStream = new FileInputStream(propertyFile);
           // check if the file exists in web environment or relative to class path
           }else{
               inputStream = getClass().getClassLoader().getResourceAsStream(propertyFilePath);
           }

           if(inputStream == null){
               throw new Exception("FILE NOT FOUND : inputStream = null : " + propertyFilePath);
           }
           
           properties = new Properties();
           properties.load(inputStream);
           Enumeration enuKeys = properties.keys();
           while (enuKeys.hasMoreElements()) {
        	  
               String key = (String) enuKeys.nextElement();
               String value = properties.getProperty(key);
               
               //System.out.print("key = "+key + " : value = "+value);
               propertyMap.put(key, value);
           }
           if(propertyMap == null){throw new Exception("readPropertyFile : propertyMap = null");}

           return propertyMap;
       }
       catch(Exception e){
           throw new Exception("readPropertyFile : " + e.toString());
       }
       finally{
            try{inputStream.close();} catch(Exception e){}
            try{properties = null;} catch(Exception e){}
       }
   }

   //method to a particular property from a property file
   public String readProperty(String property, String sFilePath) throws Exception{
       try {
           return readPropertyFile(sFilePath).get(property);
       }
       catch (Exception e) {
           throw new Exception("readProperty : " + e.toString());
       }
   }


//##################################### DATE TIME METHODS ########################################################
//################################################################################################################

   //method to get current system date and time in particular format
   public String getCurrentDateTime(String sDateFormat) throws Exception {
		SimpleDateFormat formatter = null;
		String dateNow = "";
		String sDefaultDateFormat = "yyyy:MM:dd:HH:mm";
		try
		{
			if(sDateFormat == null){
				sDateFormat = "";
			}
			if(!sDateFormat.trim().equalsIgnoreCase("")){
				sDefaultDateFormat = sDateFormat;
			}

			Calendar currentDate = Calendar.getInstance();
			formatter = new SimpleDateFormat(sDefaultDateFormat);
		    dateNow = formatter.format(currentDate.getTime());
		    dateNow.trim();
		    return dateNow;
		}
		catch(Exception exp){
                   println("getCurrentDateTime : " +  exp.toString());
                   throw exp;
		}
	}

   //method to get date time format in particular format of a calender refernce
   public String getDateFormated(Calendar objCalendar,  String sDateFormat) throws Exception {
		SimpleDateFormat formatter = null;
		String sDate = "";
		String sDefaultDateFormat = "yyyy:MM:dd HH:mm:ss";
		try
		{
			if(sDateFormat == null){
				sDateFormat = "";
			}
			sDateFormat = sDateFormat.trim();
			if(!sDateFormat.trim().equalsIgnoreCase("")){
				sDefaultDateFormat = sDateFormat;
			}

			formatter = new SimpleDateFormat(sDefaultDateFormat);
			sDate = formatter.format(objCalendar.getTime());
			sDate.trim();
		    return sDate;
		}
		catch(Exception exp){
			println("getCurrentDateTime : " +  exp.toString());
			throw exp;
		}
	}

   //method to get calender object based on datetime provided as parameter
   public Calendar GetCalendar(String sDateTime) throws Exception {
       String YYYY = "" , MM = "", DD = "", hh = "", mm = "", ss= "";
       Calendar calendarInstance = null;
       try{
           //considering deafult date format as YYYY-MM-DD hh:mm:ss (e.g 2012-11-21 23:59:30)
           sDateTime = sDateTime.trim();
           if(sDateTime.equalsIgnoreCase("")) throw new Exception("Blank datetime value");

           YYYY = sDateTime.substring(0,4);
           MM = sDateTime.substring(5,7);
           DD = sDateTime.substring(8,10);
           hh = sDateTime.substring(11,13);
           mm = sDateTime.substring(14,16);
           ss = sDateTime.substring(17,19);
           MM = "" + (Integer.parseInt(MM) -1);
           if(MM.length() < 2) MM = "0" + MM;

           calendarInstance = Calendar.getInstance();
           calendarInstance.set(Integer.parseInt(YYYY), Integer.parseInt(MM),Integer.parseInt(DD), Integer.parseInt(hh),	Integer.parseInt(mm),Integer.parseInt(ss));
           return calendarInstance;
       }
       catch (Exception e) {
           throw new Exception("SetCalendar : " + e.toString());
       }
   }

   //method to differnece between two dates dates in
   public int getDifferenceInDate(String sPreviousDate,String sCurrentDate){
       Calendar calPrevious = null;
       Calendar calCurrent = null;
       String [] arrTempDate = null;
       long longPreviousDate = 0;
       long longCurrentTime = 0;
       int timeDelay = 0;
       try{
           if(sPreviousDate.contains(":") && sCurrentDate.contains(":"))
           {
               arrTempDate = sPreviousDate.split(":");
               calPrevious = Calendar.getInstance();
               calPrevious.set(Integer.parseInt(arrTempDate[0]), (Integer.parseInt(arrTempDate[1])-1),
                               Integer.parseInt(arrTempDate[2]), Integer.parseInt(arrTempDate[3]), Integer.parseInt(arrTempDate[4]));
               arrTempDate = sCurrentDate.split(":");

               calCurrent = Calendar.getInstance();
               calCurrent.set(Integer.parseInt(arrTempDate[0]), (Integer.parseInt(arrTempDate[1])-1),
                               Integer.parseInt(arrTempDate[2]), Integer.parseInt(arrTempDate[3]), Integer.parseInt(arrTempDate[4]));
               longPreviousDate = Long.parseLong(new SimpleDateFormat("yyyyMMddHHmm").format(calPrevious.getTime()));
               longCurrentTime = Long.parseLong(new SimpleDateFormat("yyyyMMddHHmm").format(calCurrent.getTime()));
               ///println("Previous Time In Int= "+longPreviousDate);
               //println("Current Time In Int= "+longCurrentTime);
               if(longCurrentTime > longPreviousDate){
                   while(true){
                       timeDelay++;
                       calCurrent.add(Calendar.MINUTE, -1);
                       longCurrentTime = Long.parseLong(new SimpleDateFormat("yyyyMMddHHmm").format(calCurrent.getTime()));
                       //println("Previous Time In Int= "+longPreviousDate);
                       //println("Current Time In Int= "+longCurrentTime);
                       if(longCurrentTime < longPreviousDate){break;}
                   }
               }
           }
       }
       catch(Exception e){
               println("getDifferenceInDate : Exception : " + e.toString());
       }
       finally{return timeDelay;}
   }

   ///generic metod to get list of YYYYMM between two dates in (yyyy-mm-dd) format
   public ArrayList<String> getYYYYMMList(String dateFrom, String dateTo) throws Exception {

       ArrayList<String> yyyyMMList = new ArrayList<String>();
       try {
           Calendar calendar = Calendar.getInstance();
           calendar.set(Integer.parseInt(dateFrom.split("-")[0]), Integer.parseInt(dateFrom.split("-")[1]) - 1, Integer.parseInt(dateFrom.split("-")[2]));
           
           String yyyyTo = dateTo.split("-")[0];
           String mmTo = Integer.parseInt(dateTo.split("-")[1]) + "";
           if(mmTo.length() < 2){mmTo = "0" + mmTo;}
           String yyyymmTo = yyyyTo + mmTo; 
          
           while(true){
               String yyyy = calendar.get(Calendar.YEAR) + "";
               String mm = (calendar.get(Calendar.MONTH) + 1) + "";
               if(mm.length() < 2){mm = "0" + mm;}
               yyyyMMList.add(yyyy + mm);
               
               if((yyyy + mm).trim().toUpperCase().equalsIgnoreCase(yyyymmTo)){
                   break;
               }
               calendar.add(Calendar.MONTH, 1);   
           }
           return yyyyMMList;
       } catch (Exception e) {
           throw new Exception("getYYYYMMList : dateFrom(yyyy-mm-dd)=" + dateFrom + " : dateTo(yyyy-mm-dd)" + dateTo + " : " + e.toString());
       }
   }
   
   //method to get date in specific format from date in (yyyy-mm-dd) format
   public String getDDMMMYYYYDate(String date) throws Exception{
       HashMap<Integer, String> month = new HashMap<Integer, String>();
       month.put(1, "Jan");
       month.put(2, "Feb");
       month.put(3, "Mar");
       month.put(4, "Apr");
       month.put(5, "May");
       month.put(6, "Jun");
       month.put(7, "Jul");
       month.put(8, "Aug");
       month.put(9, "Sep");
       month.put(10, "Oct");
       month.put(11, "Nov");
       month.put(12, "Dec");
       
       try {
           String[] dtArray = date.split("-");
           return dtArray[1] + "-" + month.get(Integer.parseInt(dtArray[1])) + "-" + dtArray[0];
       } catch (Exception e) {
           throw new Exception("getDDMMMYYYYDate : " + date + " : " + e.toString());
       }
       
   }
   
   //method to convert date to unixtimestamp format
   public long DateStringToUnixTimeStamp(String sDateTime) {
       DateFormat formatter;
       Date date = null;
       long unixtime = 0;
       formatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
       
       try {
           date = formatter.parse(sDateTime);
           unixtime = date.getTime() / 1000L;
       } catch (Exception ex) {
           ex.printStackTrace();
           unixtime = 0;
       }
       
       return unixtime;
   }

   //method to get current GMT seconds
   public int getGMTOffSetSeconds() throws Exception{
       return getGMTOffSetSeconds("");
   }
       
   //method to get GMT seconds for a particular GMT (-04:30. +11:15)
   public int getGMTOffSetSeconds(String GMT) throws Exception{
       int RawOffSet = 0;
       double iTracker = 0.0;
       try {
           
           iTracker = 0.0;
           //check for null & blank GMT,, return server deafult GMT
           if(GMT == null){return RawOffSet = TimeZone.getDefault().getRawOffset()/1000;} //null GMT
           if(GMT.trim().equalsIgnoreCase("")){return RawOffSet = TimeZone.getDefault().getRawOffset()/1000;} //invalid GMT value
               
           
           iTracker = 1.0;
           //check for validations of GMT string
           
           if(!GMT.contains(":")){throw new Exception("Invalid GMT , does noit contains ':' colon ## ");} //invalid GMT value

           iTracker = 2.0;
           String[] saTemp = GMT.trim().toUpperCase().split(":"); //
           if(saTemp.length != 2){throw new Exception("Invalid GMT, : slpit length is not equal to 2 ## ");} //invalid GMT value

           iTracker = 3.0;
           String HH = saTemp[0].trim();
           String MM = saTemp[1].trim();
           boolean isNegativeGMT = false;

           iTracker = 4.0;
           if(HH.contains("-")){isNegativeGMT = true;}
           HH = HH.replaceAll("-", "").trim().toUpperCase();
           HH = HH.replaceAll("[+]", "").trim().toUpperCase();

           iTracker = 5.0;
           int iHH = Integer.parseInt(HH);
           int iMM = Integer.parseInt(MM);

           if(iHH > 11){throw new Exception("invalid GMT : HH > 11 ##");}
           if(iHH < -11){throw new Exception("invalid GMT : HH < -11 ##");}
           
           if(iMM < 0){throw new Exception("invalid GMT : MM < 0 ##");}
           if(iMM > 59){throw new Exception("invalid GMT : MM > 59 ##");}
           
           
           iTracker = 6.0;
           RawOffSet = (iHH * 60 * 60) + (iMM * 60);
           if(isNegativeGMT){RawOffSet = RawOffSet * -1;}
               
           iTracker = 7.0;
           return RawOffSet;
           
       } catch (Exception e) {
           println("getGMTOffSetSeconds : " + e.toString() + " : " + GMT);
           throw new Exception("getGMTOffSetSeconds : " + e.toString());
       }
   
   }//end getGMTOffSetSeconds
   
   //method to get date in YYYYMMDDhhmmss format
   public String getDateTime() throws Exception{
       return getDateTime(null, 0, "");
   }
   
   //methoid to get date time in particular format wrt GMT, default is yyyyMMddHHmmss
   public String getDateTime(String DATE_TIME_FORMAT) throws Exception{
       return getDateTime(null, null, DATE_TIME_FORMAT);
   }
   
   //method to get date in YYYYMMDDhhmmss format
   public String getDateTime(Calendar calendar) throws Exception{
       return getDateTime(calendar, 0, "");
   }
   
   //method to get date in YYYYMMDDhhmmss format
   public String getDateTime(Calendar calendar, String GMT) throws Exception{
       return getDateTime(calendar, GMT, "");
   }
   
   //methoid to get date time in particular format wrt GMT,default is yyyyMMddHHmmss
   public String getDateTime(Calendar calendar, String GMT, String DATE_TIME_FORMAT) throws Exception{
       int GMT_OFFSET_SECONDS = 0;
       try {
           GMT_OFFSET_SECONDS = getGMTOffSetSeconds(GMT);
           return getDateTime(calendar, GMT_OFFSET_SECONDS, DATE_TIME_FORMAT);
       } catch (Exception e) {
           throw new Exception("getDateTime : Invalid GMT :" + GMT + " : " + e.toString());
       }                
   }
   
   //methoid to get date time in particular format wrt GMT, default is yyyyMMddHHmmss
   public String getDateTime(Calendar cal, int  GMT_OFFSET_SECONDS, String DATE_TIME_FORMAT) throws Exception{
   
       String sRetVal = "";
       String DefaultFormat = "yyyyMMddHHmmss";
       int GMT_OFFSET_DIFF = 0;
       try {
           
           //check for valid Calender values
           if(cal == null){cal = Calendar.getInstance();}
           
           //check for valid FORMAT values
           if(DATE_TIME_FORMAT == null){DATE_TIME_FORMAT = DefaultFormat;}
           if(DATE_TIME_FORMAT.trim().toUpperCase().equalsIgnoreCase("")){DATE_TIME_FORMAT = DefaultFormat;}
           
           
           //check GMT RAW OFF SET difference
           int CURR_GMT_OFFSET = TimeZone.getDefault().getRawOffset()/1000;
           //in case Current GMT is GREATER THAN provided GMT
           if(CURR_GMT_OFFSET > GMT_OFFSET_SECONDS && GMT_OFFSET_SECONDS != 0){
               if(GMT_OFFSET_SECONDS < 0){
                   GMT_OFFSET_DIFF = GMT_OFFSET_SECONDS - CURR_GMT_OFFSET;
               }else{
                   GMT_OFFSET_DIFF = GMT_OFFSET_SECONDS - CURR_GMT_OFFSET;
               }
           
           //in case Current GMT is SMALLER THAN provided GMT
           }else if(CURR_GMT_OFFSET < GMT_OFFSET_SECONDS  && GMT_OFFSET_SECONDS != 0){
               if(GMT_OFFSET_SECONDS < 0){
                   GMT_OFFSET_DIFF = GMT_OFFSET_SECONDS - CURR_GMT_OFFSET;
               }else{
                   GMT_OFFSET_DIFF = GMT_OFFSET_SECONDS - CURR_GMT_OFFSET;
               }
           }
           
           if(CURR_GMT_OFFSET == GMT_OFFSET_SECONDS){GMT_OFFSET_DIFF = 0;}
           
           
           //setting calender datetime as per GMT
           cal.add(Calendar.SECOND, GMT_OFFSET_DIFF);
           
           //using SimpleDateFormat class
           sRetVal = new SimpleDateFormat(DATE_TIME_FORMAT).format(cal.getTime());            
           return sRetVal;
                       
       } catch (Exception e) {
           println("getDateTime : " + GMT_OFFSET_SECONDS + " : " + e.toString());
           throw new Exception("getDateTime : " + GMT_OFFSET_SECONDS + " : " + e.toString());
       }finally{}
   
   
   
   
   }
   
   
   
//##################################### Maths calculation METHODS ################################################
//################################################################################################################
   

   //methods checks if a particular string is an integer
   public boolean isInteger(String sIntString){
       int i = 0;
       try {
           i = Integer.parseInt(sIntString);
           return true;
       } catch (Exception e) {
           return false;
       }
   }

   //method checks if a particular string is a decimal value
   public boolean isDecimal(String sDecimalString){
       return true;
   }

   //check if a string is numeric
   public boolean isNumeric(String sNumericString){
       try {
           for (char c : sNumericString.toCharArray()){
               if (!Character.isDigit(c)) return false;
           }
           return true;
       } catch (Exception e) {return false;}
   }

   //methods to convert double variable to string
   public String DoubleToString(double dValue){

       String sValue = "";
       try {
           sValue = String.format("%.4f", dValue);
       }catch (Exception e) {
           sValue = "";
       }finally{ return sValue;}
   }

   //method to convert float variable to string
   public String FloatToString(float dValue){

       String sValue = "";
       try {
           sValue = String.format("%.4f", dValue);
       }catch (Exception e) {
           sValue = "";
       }finally{ return sValue;}
   }
   
   //method to calculate average of integer list
   public double calculateAverage(List <Integer> list) {
       double sum = 0;
       double avg = 0.0;
       try{
           if(list.isEmpty()) {
               throw new Exception("Empty List");
           }
           
           //loop for values to calculate average
           for (Integer mark : list) {
               sum = sum + mark;
           }
           avg = sum / list.size();                        
           
       }catch(Exception e){ avg = 0.0; }
       return avg;
   }
   
   //method to calculate average of double list
   public double calculateAverage(List <Double> list , String s) {
       double sum = 0;
       double avg = 0.0;
       try{
           if(list.isEmpty()) {
               throw new Exception("Empty List");
           }
           
           //loop for values to calculate average
           for (Double mark : list) {
               sum = sum + mark;
           }
           avg = sum / list.size();                        
           
       }catch(Exception e){ avg = 0.0; }
       return avg;
   }

   
//##################################### ENCRYPTION / DECRYPTION METHODS ##########################################
//################################################################################################################
   
   //method to encode string into md5 hash
   public String get_Md5_Hash(String sStringToEncode) throws Exception{
       String sRetval = "";
       StringBuffer sb = new StringBuffer();
       
       try {
           MessageDigest md = MessageDigest.getInstance("MD5");
           byte[] messageDigest = md.digest(sStringToEncode.getBytes("UTF-8"));
           BigInteger number = new BigInteger(1, messageDigest);
           String hashtext = number.toString(16);
           sRetval = hashtext;
       }
       catch (Exception e) {
           throw new Exception("get_Md5_Hash : " + e);
       }
       return sRetval;
   }
   
   
   
  

   

//##################################### Data Export #############################################################
//################################################################################################################

   public boolean ExportToCSV(ResultSet resultset, String sExcelPath){
		int column = 0;
		int row = 0;
		ResultSetMetaData rsMD = null;
		StringBuffer bufftemp = new StringBuffer();
		try{
			rsMD  = (ResultSetMetaData) resultset.getMetaData();
			column = rsMD.getColumnCount();

			//adding column names to CSV string buffer
			for(int i = 0 ; i < column ; i++){

				if(rsMD.getColumnName(i+1).trim().toUpperCase().equalsIgnoreCase("ALARM_COUNT")){
					bufftemp.append("Total_Count,");
				}
				else{
					bufftemp.append(rsMD.getColumnName(i+1).trim() + ",");
				}
			}
			bufftemp.append("\n");
			//adding rows to CSV String buffer
			while(resultset.next()){
				row = row + 1;
				for(int i = 0 ; i < column ; i++){
					bufftemp.append(resultset.getObject(i+1).toString().trim().replaceAll(",", ";") + ",");
				}
				bufftemp.append("\n");
			}
			//creating CSV File
			File f = new File(sExcelPath);
			try{ if (f.exists()) f.delete();} catch(Exception exp){}

			row =row;//total csv row count including column header
			println(bufftemp.toString().replaceAll("Alarm_Count", "" + row), sExcelPath);

			return true;
		}
		catch (Exception e) {
			println("ExportToCSV : " +  e.toString());
			e.printStackTrace();
			return false;
		}
		finally{}
	}

   public boolean ExportToCSV(String[][] data, String sExcelPath){
		int row = 0 ;
		int column = 0;
		StringBuffer bufftemp = new StringBuffer();
		try{
			if( data.length <= 0 || data[0].length <= 0) throw new Exception("invalid Row/Coumn data");

			for(row = 0 ; row < data.length ; row ++){

				for(column =0; column < data[0].length ; column ++){
					bufftemp.append(data[column][row] + ",");
				}
				bufftemp.append("\n");
			}

			File f = new File(sExcelPath);
			try{ if (f.exists()) f.delete();} catch(Exception exp){}
			println(bufftemp.toString(), sExcelPath);
			return true;
		}
		catch (Exception e) {

			return false;
		}
		finally{}
	}
  
   public boolean ExportToExcel(String[] data, String sExcelPath){
		try{

			return true;
		}
		catch (Exception e) {

			return false;
		}
		finally{}
	}


//##################################### Compression METHODS ########################################
//################################################################################################################ 

   //method to zip any file
   public boolean ZipFile(String sSourceFilePath, String sDestinationZipFilePath, boolean bReplaceExisting){
       byte[] buffer = new byte[30720];
       FileInputStream fin = null;
       FileOutputStream fout = null;
       ZipOutputStream zout = null;
       int length;
       String sZipEntryFileName= "";
       File objFile = null;
       try
       {
       	//check for source file
           if(sSourceFilePath.trim().equalsIgnoreCase("")){
               throw new Exception("Invalid Source File : " + sSourceFilePath);
           }
           objFile = new File(sSourceFilePath);
           if(!objFile.exists()) throw new Exception("Source file not found : " + sSourceFilePath);

           //check for destination Zip file
           if(sDestinationZipFilePath.trim().equalsIgnoreCase("") || sDestinationZipFilePath == null){
               String stmp_Path = objFile.getAbsolutePath();
               String stmp_Name = objFile.getName();
               if(stmp_Name.contains(".")){ //check for removing extension
                   int indx = 0;
                   try{indx = stmp_Name.indexOf(".", stmp_Name.length() -5);}catch(Exception e){indx = 0;}
                   if(indx <= 0){indx = stmp_Name.length();}
                           
                   stmp_Name = stmp_Name.substring(0, indx);
                   stmp_Name = stmp_Name + ".zip";
               }
               sDestinationZipFilePath = stmp_Path + File.separator + stmp_Name;
           }
           
           objFile = new File(sDestinationZipFilePath);
           if(objFile.exists()){
           	if(bReplaceExisting){
           		objFile.delete();
           	}else{
           		throw new Exception("Destination ZipFile Already exists : " + sDestinationZipFilePath);
           	}
           }

           //Zipping File
           sZipEntryFileName = sSourceFilePath.substring(sSourceFilePath.lastIndexOf("\\") + 1);
           fout = new FileOutputStream(sDestinationZipFilePath);
           zout = new ZipOutputStream(fout);
           fin = new FileInputStream(sSourceFilePath);
           zout.putNextEntry(new ZipEntry(sZipEntryFileName));
           while((length = fin.read(buffer)) > 0){
               zout.write(buffer, 0, length);
           }
           
           return true;
           
       }catch(Exception exp){
       	println("Src = " + sSourceFilePath + " : Dest = " + sDestinationZipFilePath + " : " + exp.toString());
           return false;
       }finally{
           try{fin.close();} catch(Exception exp){}
           try{zout.closeEntry();} catch(Exception exp){}
           try{zout.close();} catch(Exception exp){}
       }
   }

   
}//End Methods Class

