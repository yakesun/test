package www.mongo.cn;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.WriteResult;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Filters;

/**
 * mongoDb练习
 * @author Administrator
 *
 */
public class TestMongo {
	static MongoClient mongoClient = null;
	public static MongoDatabase getMongoDB(){
		try{
			//连接mongo数据库
			if(mongoClient == null){
				MongoClient mongoClient = new MongoClient("192.168.0.17",27017);
				return mongoClient.getDatabase("xuexiao");
			}
			
		}catch(Exception e){
			System.out.println(e.getClass().getName()+":"+e.getMessage());
		}
		return null;
	}
	
	/**
	 * 连接集合，并遍历集合中的文档
	 */
	@Test
	public void test01(){
		
		MongoCollection<Document>  collections = TestMongo.getMongoDB().getCollection("clazz");
//		MongoCollection<Document>  collections = TestMongo.getMongoDB().getCollection("clazz");
		FindIterable<Document> it = collections.find();
		MongoCursor<Document> mc = it.iterator();
		while(mc.hasNext()){
			Document  doc = mc.next();
			System.out.println(doc);
//			System.out.println(doc.get("name")+":"+doc.get("age"));	
		}
	}
	
	/**
	 * 获取库中集合列表
	 */
	@Test
	public void test02(){
		//获取库中集合列表
		MongoIterable<String> mongoIterable = TestMongo.getMongoDB().listCollectionNames();
		MongoCursor<String>  mongoCursor= mongoIterable.iterator();
		while(mongoCursor.hasNext()){
			System.out.println(mongoCursor.next());
		}
		
	}
	/**
	 *插入集合
	 */
	@Test
	public void test03(){
		//插入班级集合
		TestMongo.getMongoDB().createCollection("clazz");
	}
	/**
	 *向集合中插入文档数据
	 */
	@Test
	public void test04(){
		
		
		MongoCollection<Document> collection = TestMongo.getMongoDB().getCollection("clazz");
		Document document = new Document();
		document.append("clazzName", "一年级").append("banzhuren", "李老师");
		List<Document> documents = new ArrayList<Document>();
		documents.add(document);
		
		collection.insertMany(documents);
		System.out.println("文档插入成功！");
	}
	/**
	 *在集合中删除文档数据
	 */
	@Test
	public void test05(){
		
		Mongo mg = new Mongo("127.0.0.1",27017);
		DBCollection collectio = mg.getDB("xuexiao").getCollection("clazz");
		BasicDBObject document = new BasicDBObject();
		document.put("clazzName", "三年级");
		WriteResult  wr = collectio.remove(document);
		System.out.println(wr);
	}
	

	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}
