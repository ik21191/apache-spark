package com.comparator;

import java.io.Serializable;

import org.bson.Document;

public class DocumentComparable extends Document implements Comparable<Document>, Serializable {
	
	private static final long serialVersionUID = 1L;

	@Override
	public int compareTo(Document doc1) {
		try {
		String name1 = this.getString("Name");
		String name2 = doc1.getString("Name");
		
		int nameComarare = name1.compareTo(name2);
		if(nameComarare == 0) {
			String city1 = this.getString("city");
			String city2 = doc1.getString("city");
			return city1.compareTo(city2);
		} else { 
			return nameComarare;
		}
		}catch(Exception e) {
			System.out.println("There was some exception.");
			return 0;
		}
	}
}