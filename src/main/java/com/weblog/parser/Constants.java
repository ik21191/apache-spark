/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.weblog.parser;

import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author kapil.verma
 */
public class Constants {
    
    public static final ConcurrentHashMap<String, String> MONTH_MAP = new ConcurrentHashMap<String, String>(){{
        put("JAN", "01");
        put("FEB", "02");
        put("MAR", "03");
        put("APR", "04");
        put("MAY", "05");
        put("JUN", "06");
        put("JUL", "07");
        put("AUG", "08");
        put("SEP", "09");
        put("OCT", "10");
        put("NOV", "11");
        put("DEC", "12");
    }};
    
}
