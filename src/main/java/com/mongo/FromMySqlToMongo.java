package com.mongo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.bson.Document;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 * Java MongoDB : Insert a Document
 *
 */
public class FromMySqlToMongo {
	private static final String DRIVER = "com.mysql.jdbc.Driver";
	private static final String URL = "jdbc:mysql://10.31.1.114:3306/pubstats_live";
	private static final String USERNAME = "h.nanda";
	private static final String PASSWORD = "zxcv#1235";

	public static void main(String[] args) {

		Connection connection = null;

		MongoClient client = null;
		try {

			client = new MongoClient("10.50.8.220", 27017);
			MongoDatabase mongoDB = client.getDatabase("astm_feeder");
			MongoCollection<Document> mongoCollection = mongoDB.getCollection("201801_accounts");

			Class.forName(DRIVER);
			System.out.println("Getting database connection ......");
			connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
			System.out.println("Got connection.");
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery("SELECT * FROM accounts WHERE webmart_id=801 AND YEAR=2018 AND MONTH=01");
			ResultSetMetaData metadata = resultSet.getMetaData();
			int columnCount = metadata.getColumnCount();
			// List<String> columns = new ArrayList<>();
			Map<String, String> columnNameType = new HashMap<>();

			for (int i = 1; i <= columnCount; i++) {
				columnNameType.put(metadata.getColumnName(i), metadata.getColumnTypeName(i));
			}

			Set<Entry<String, String>> entrySet = columnNameType.entrySet();

			System.out.println(entrySet);

			// Object obj = metadata.getColumnType(i);
			// System.out.println("ColumnName : " + columnName + "\t" +
			// "ColumnType : " + metadata.getColumnTypeName(i));
			// columns.add(columnName);
			List<Document> docList = new ArrayList<>();
			System.out.println("Inserting to mongoDB start...");
			int count = 0;
			while (resultSet.next()) {
				count++;
				Document document = new Document();
				for (Entry<String, String> entry : entrySet) {
					String column = entry.getKey();
					String type = entry.getValue();
					if (type.equalsIgnoreCase("int")) {
						document.put(column, resultSet.getInt(column));
					} else if (type.equalsIgnoreCase("SMALLINT")) {
						document.put(column, resultSet.getInt(column));
					} else if (type.equalsIgnoreCase("MEDIUMINT")) {
						document.put(column, resultSet.getInt(column));
					} else if (type.equalsIgnoreCase("CHAR")) {
						document.put(column, resultSet.getString(column) != null ? resultSet.getString(column) : "-");
					} else if (type.equalsIgnoreCase("DATETIME")) {
						document.put(column, resultSet.getString(column) != null ? resultSet.getString(column) : "-");
					} else if (type.equalsIgnoreCase("double")) {
						document.put(column, resultSet.getDouble(column));
					} else if (type.equalsIgnoreCase("varchar")) {
						document.put(column, resultSet.getString(column) != null ? resultSet.getString(column) : "-");
					} else {
						document.put(column, resultSet.getString(column) != null ? resultSet.getString(column) : "-");
					}
				}

				docList.add(document);

				System.out.println("Total inserted : " + count);

			}

			mongoCollection.insertMany(docList);
			System.out.println("Insert to MongoDB End.");
		} catch (Exception e) {
			System.out.println(e);
		} finally {
			try {
				connection.close();
				if (client != null) {
					client.close();
				}
			} catch (SQLException e) {
				System.out.println("Problem closing connection: " + e);
			}
		}

	}
}