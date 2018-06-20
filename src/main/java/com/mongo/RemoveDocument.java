package com.mongo;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class RemoveDocument {

	public static void main(String[] args) throws Exception {

		System.out.println((new SimpleDateFormat("yyyy M dd hh mm ss").format(new Date())).replace(" ", ""));
		MongoClient client = new MongoClient("localhost:27017");
		MongoDatabase database = client.getDatabase("testdata");
		MongoCollection<Document> collection = database.getCollection("sorted1");
		
		// To delete all documents from the collection
		collection.deleteMany(new BasicDBObject());

		client.close();

	}
}