package com.mongo;
import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class IterateDocument {

	public static void main(String[] args) {

		MongoClient client = new MongoClient("localhost:27017");
		MongoDatabase database = client.getDatabase("testdata");
		MongoCollection<Document> collection = database.getCollection("data1");
		
		
		List<Document> list = collection.find().into(new ArrayList<Document>());
		
		for(Document d : list) {
			Document val = d.get("test_values", new Document());
			System.out.println(val.isEmpty());
			System.out.println(val.get("name", "No Name"));
			System.out.println(val.get("marks", 0));
			System.out.println(val.get("city", "No City"));
			List<String> myList = (List<String>)d.get("my_list");
			System.out.println(myList.contains("Imran "));
		}
		client.close();

	}
}