package com.mongo;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 * Java MongoDB : Insert a Document
 *
 */
public class InsertToMongo2 {
		public static void main(String[] args) {

		

		MongoClient mongoClient = null;
		try {
			mongoClient = new MongoClient("localhost", 27017);
			MongoDatabase mongoDB = mongoClient.getDatabase("testdata");
			MongoCollection<Document> collection = mongoDB.getCollection("col_with_list");
			
			Document mainDocument = new Document();
			
			Document subDocument = new Document();
			System.out.println("Started");
			subDocument.append("Name", "Imran").
			append("Marks", 100).
			append("City", "New Delhi");
			
			mainDocument.append("myDetails", subDocument);
			System.out.println("Before : " + mainDocument.toJson());
			
			collection.insertOne(mainDocument);
			Document filter = new Document("_id", mainDocument.getObjectId("_id"));
			Document updateOperation = new Document("$set", mainDocument.append("new_val", "This is updated value"));
			collection.updateOne(filter, updateOperation);
			System.out.println("After : " + mainDocument.toJson());
			System.out.println("End");
			
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if(mongoClient != null) {
					mongoClient.close();
				}
				
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}
}