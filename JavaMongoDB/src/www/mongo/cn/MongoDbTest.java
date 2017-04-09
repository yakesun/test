package www.mongo.cn;

import org.bson.Document;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class MongoDbTest {
	static MongoDatabase db = null;
	
	public static MongoDatabase getDB(){
		MongoClient mongoClient = new MongoClient("192.168.0.17",27017);
		MongoDatabase database = mongoClient.getDatabase("xuexiao");
		return database;
	}
	
	
	/**
	 * 插入集合
	 */
	@Test
	public void test01(){
		MongoDbTest.getDB().createCollection("student");
	}
	
	/**
	 * 插入文档
	 */
	@Test
	public void test02(){
		Document document = new Document("name","zhangsan").append("age", 23).append("sex", "男");
		MongoDbTest.getDB().getCollection("student").insertOne(document);
	}
	
	/**
	 * 查看文档数量
	 */
	@Test
	public void test03(){
		long  count = MongoDbTest.getDB().getCollection("student").count();
		System.out.println("文档数量："+count);
	}
	
	/**
	 * 获取第一条文档 
	 */
	@Test
	public void test04(){
		Document document = MongoDbTest.getDB().getCollection("student").find().first();
		System.out.println(document.toJson());
	}
}
