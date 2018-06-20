package com.comparator;

import java.io.Serializable;
import java.util.Comparator;

import org.bson.Document;

public class DocumentComparator implements Comparator<Document>, Serializable {

	private static final long serialVersionUID = 1L;

	@Override
	public int compare(Document doc1, Document doc2) {
		String name1 = doc1.getString("Name");
		String name2 = doc2.getString("Name");
		int nameCompare = name1.compareTo(name2);
		if (nameCompare == 0) {
			String city1 = doc1.getString("City");
			String city2 = doc2.getString("City");
			int cityCompare = city1.compareTo(city2);
			if (cityCompare == 0) {
				return doc2.getInteger("Marks") - doc1.getInteger("Marks");
			} else {
				return city1.compareTo(city2);
			}
		} else {
			return nameCompare;
		}
	}
}