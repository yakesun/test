package www.mongo.cn;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
/**
 * 注释。。。
 * @author Administrator
 *
 */
public class MongoDBJDBC {

		public static void main(String arg[]){
			try{
				//连接mongo数据库
				MongoClient mongoClient = new MongoClient("192.168.0.17",27017);
				MongoDatabase database = mongoClient.getDatabase("xuexiao");
				MongoCollection<Document>  collections = database.getCollection("student");
				FindIterable<Document> it = collections.find();
				MongoCursor<Document> mc = it.iterator();
				while(mc.hasNext()){
					Document  doc = mc.next();
					System.out.println(doc.get("name")+":"+doc.get("age"));
					//System.out.println(doc);
				}
			}catch(Exception e){
				System.out.println(e.getClass().getName()+":"+e.getMessage());
			}
		}
}
