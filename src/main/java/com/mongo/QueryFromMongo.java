package com.mongo;
import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class QueryFromMongo {

	public static void main(String[] args) {

		MongoClient client = new MongoClient("localhost:27017");
		MongoDatabase database = client.getDatabase("testdata");
		MongoCollection<Document> collection = database.getCollection("updated");

		List<Integer> inList = new ArrayList<>();
		inList.add(65);
		inList.add(100);
		inList.add(78);
		BasicDBObject andQuery = new BasicDBObject();
	    List<BasicDBObject> whereConditionList = new ArrayList<BasicDBObject>();
	    whereConditionList.add(new BasicDBObject("Name", "Imran"));
	    //whereConditionList.add(new BasicDBObject("MyMarks", 65));
	    whereConditionList.add(new BasicDBObject("MyMarks", new BasicDBObject("$in", inList)));
	    //whereConditionList.add(new BasicDBObject("City", new BasicDBObject("$ne", inList)));
	    whereConditionList.add(new BasicDBObject("City", new BasicDBObject("$ne", "Mumbai")));
	    andQuery.put("$and", whereConditionList);
		
		System.out.println((List<Document>) collection.find(andQuery).into(new ArrayList<Document>()));
		client.close();

	}
}