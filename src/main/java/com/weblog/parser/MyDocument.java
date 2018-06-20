/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.weblog.parser;


import java.io.Serializable;
import org.bson.Document;

/**
 *
 * @author kapil.verma
 */
public class MyDocument implements Serializable{
    private static final String[] patternStack = new String[]{" ", " - ", " [", "] \"", "\" ", " ", " \"", "\" \"", "\" ", " "};
    
    public static Document getFilteredDoc(final Document doc){
       
        Document filterDoc = new Document();
        filterDoc.append("filter_column", "Megha");
        return filterDoc;
    }
    
    public static Document getDoc(final String line){
        Document doc = null;
        String domain = "";
        String ipAddress = "";
        long longIpAddress = 0;
        String userID = "";
        String requestDate = "";
        String requestDateTime = "";
        String gmtOffSet = "";
        String url = "";
        String requestMethod = "";
        String httpVersion = "";
        String statusCode = "";
        String refererUrl = "";
        String userAgent = "";
        String resourceType = "";
        String institutionId = "";
        String sessionId = "";
        
        int patternNo = 0;
        int startIndex = 0;
        int endIndex = -1;
        String value = "";
        
        double iTracker = 0.0;
        
        for (patternNo = 0; patternNo < patternStack.length; patternNo++) {
            value = "";
            endIndex = -1;
            try {
                iTracker = 21.0;
                //operation without substring
                endIndex = line.indexOf(patternStack[patternNo], startIndex);
                iTracker = 22.0;
                if (endIndex >= 0) {
                    iTracker = 23.0;
                    value = line.substring(startIndex, endIndex);
                    iTracker = 24.0;
                    startIndex = endIndex + patternStack[patternNo].length();
                } else {
                    iTracker = 25.0;
                    startIndex = startIndex + patternStack[patternNo].length();
                }

                iTracker = 26.0;
                //switch case for 
                switch (patternNo) {
                    //domain
                    case 0: {
                        iTracker = 50.0;
                        domain = value.trim();
                        iTracker = 50.1;
                        break;
                    }
                    //ip address
                    case 1: {
                        iTracker = 51.0;
                        ipAddress = value.trim();
                        institutionId = "-";
                        iTracker = 51.1;
                        try {
                            longIpAddress = 0;
                        } catch (Exception e) {
                            longIpAddress = -3;
                        }
                        iTracker = 51.10;
                        break;
                    }
                    //userId
                    case 2: {
                        iTracker = 52.0;
                        userID = value.trim();
                        iTracker = 52.1;
                        break;
                    }
                    //request date time and GMT offset
                    case 3: {
                        iTracker = 53.0;
                        String[] tmpTime = value.trim().split(" ");
                        iTracker = 53.1;
                        String[] tmpData = tmpTime[0].replaceAll("/", ":").split(":");
                        iTracker = 53.2;
                        requestDateTime = tmpData[2] + "-" + Constants.MONTH_MAP.get(tmpData[1].trim().toUpperCase()) + "-" + tmpData[0] + " " + tmpData[3] + ":" + tmpData[4] + ":" + tmpData[5];
                        requestDate = tmpData[2] + "-" + Constants.MONTH_MAP.get(tmpData[1].trim().toUpperCase()) + "-" + tmpData[0];
                        iTracker = 53.3;
                        gmtOffSet = tmpTime[1];
                        iTracker = 53.4;
                        tmpTime = null;
                        iTracker = 53.5;
                        break;
                    }
                    //url, request type and Http version
                    case 4: {
                        iTracker = 54.0;
                        String[] tmpUrl = value.trim().split(" ");
                        int length = tmpUrl.length;
                        switch (length) {
                            case 0: {
                                iTracker = 54.01;
                                throw new Exception("tmpUrl.length is Zero");
                            }
                            case 1: {
                                iTracker = 54.11;
                                requestMethod = tmpUrl[0].trim().toUpperCase();
                                break;
                            }
                            case 2: {
                                iTracker = 54.21;
                                requestMethod = tmpUrl[0].trim().toUpperCase();
                                iTracker = 54.22;
                                url = tmpUrl[1].trim();
                                break;
                            }
                            case 3: {
                                iTracker = 54.31;
                                requestMethod = tmpUrl[0].trim().toUpperCase();
                                iTracker = 54.32;
                                url = tmpUrl[1].trim();
                                iTracker = 54.33;
                                httpVersion = tmpUrl[2].trim();
                                break;
                            }
                            default: {
                                iTracker = 54.41;
                                requestMethod = tmpUrl[0].trim().toUpperCase();
                                iTracker = 54.42;
                                for (int i = 1; i < length - 1; i++) {
                                    url = url + " " + tmpUrl[i].trim();
                                }
                                url = url.trim();
                                iTracker = 54.43;
                                httpVersion = tmpUrl[length - 1].trim();
                            }
                        }
                        resourceType = "";
                        tmpUrl = null;
                        iTracker = 54.5;
                        break;
                    }
                    //status Code
                    case 5: {
                        iTracker = 55.0;
                        statusCode = value.trim();
                        iTracker = 55.1;
                        break;
                    }
                    //bytesSent
                    case 6: {
                        iTracker = 56.0;
                        //userID = value.trim();
                        iTracker = 56.1;
                        break;
                    }
                    //refererUrl
                    case 7: {
                        iTracker = 57.0;
                        refererUrl = value.trim();
                        iTracker = 57.1;
                        break;
                    }
                    //userAgent
                    case 8: {
                        iTracker = 58.0;
                        userAgent = value.trim().replaceAll("'", "''");
                        iTracker = 58.1;
                        break;
                    }
                    //session id
                    case 9: {
                        iTracker = 59.0;
                        sessionId = value.trim();
                        iTracker = 59.1;
                        break;
                    }
                    //default case
                    default: {
                        iTracker = 60.0;
                        break;
                    }
                }//end switch Case

                iTracker = 70.0;

            } catch (Exception e) {
                System.out.println("Pattern No=" + patternNo + " : pattern=" + patternStack[patternNo] + " : startIndex=" + startIndex + " : endIndex=" + endIndex + " : iTacker=" + iTracker + " : " + e.toString() + " : Line = " + line);
            }
        }

        //
        doc = new Document("domin", domain)
                .append("ip_address", ipAddress)
                .append("long_ip_address", longIpAddress)
                .append("institution_id", institutionId)
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
                .append("session_id", sessionId);
        
        try {
            
        } catch (Exception e) {
            doc = null;
        }
        return doc;
    }
    
}
