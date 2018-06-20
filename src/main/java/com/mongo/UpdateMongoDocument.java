package com.mongo;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class UpdateMongoDocument {

	public static void main(String[] args) {

		MongoClient client = new MongoClient("localhost:27017");
		MongoDatabase database = client.getDatabase("testdata");
		MongoCollection<Document> collection = database.getCollection("updated");

		//Bson filter = new Document("_id", new ObjectId("5a0dcf74aef91a1b54c8b61e"));
		Bson filter = new Document("Name", "Vinay Kumar");
		//Bson newValue = new Document("Name", "Vinay Kumar").append("City", "New Delhi").append("MyMarks", 101);
		Document newValue = new Document("Name", "ZZZ");
		Bson updateOperationDocument = new Document("$set", newValue);
		collection.updateOne(filter, updateOperationDocument);

		client.close();

	}
}