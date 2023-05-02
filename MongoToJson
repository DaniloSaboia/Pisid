import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

public class MongoToJson {

	private final MongoClient mongoClient;
	private final MongoDatabase database;
	private final MongoCollection<Document> collection;
	private final List<Document> mongoDocList = new ArrayList<>();
	private final String outputFilePath;

	public MongoToJson(String host, int port, String dbName, String collectionName, String outputFilePath) {
		mongoClient = new MongoClient(host, port);
		database = mongoClient.getDatabase(dbName);
		collection = database.getCollection(collectionName);
		this.outputFilePath = outputFilePath;
	}

	public void start() {

		final ClientSession session = null;

		try {
			session.startTransaction();
			TimerTask task = new TimerTask() {
				@Override
				public void run() {
					session.startTransaction();
					try (MongoCursor<Document> cursor = collection.find().iterator()) {
						while (cursor.hasNext()) {
							Document doc = cursor.next();
							mongoDocList.add(doc);
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			};
			Timer timer = new Timer();
			timer.schedule(task, 0, 1000);
		} finally {
			session.close();
		}
	}

	public void close() {
		List<Document> docsToWrite = new ArrayList<>(mongoDocList);
		try (FileWriter writer = new FileWriter(outputFilePath)) {
			for (Document doc : docsToWrite) {
				writer.write(doc.toJson());
				writer.write(System.lineSeparator());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		mongoClient.close();
	}

	public static void main(String[] args) {
		String host = "localhost";
		int port = 27017;
		String dbName = "pisid";
		String collectionName = "temperatura";
		String outputFilePath = "output.json";

		MongoToJson mongoToJson = new MongoToJson(host, port, dbName, collectionName, outputFilePath);
		mongoToJson.start();

		try {
			Thread.sleep(000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		mongoToJson.close();
	}

}
