package www.mongo.cn;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.junit.internal.matchers.CombinableMatcher;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.TextSearchOptions;
import com.mongodb.client.model.geojson.Point;
import com.mongodb.client.model.geojson.Position;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.*;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

public class MongoDb01 {
	MongoDbTest md = new MongoDbTest();
	MongoCollection<Document> collection = md.getDB().getCollection("clazz");

	/**
	 * 打印聚合结果
	 */

	Block<Document> printBlock = new Block<Document>() {
		@Override
		public void apply(final Document document) {
			System.out.println(document.toJson());
		}
	};

	/**
	 * 向集合中插入文档，每次插入一条
	 */
	@Test
	public void test01() {
		// Document document = new Document("clazzName","初中").append("nianji",
		// "初一").append("counts",50).append("banzhuren", "王老师");
		Document document = new Document("clazzName", "初中")
				.append("nianji", "初一")
				.append("counts", 50)
				.append("banzhuren", "王老师")
				.append("info",
						new Document("数学老师", "张三").append("语文老师", "李四").append(
								"英语老师", "马老师"));
		collection.insertOne(document);
		System.out.println("成功向集合中插入文档");
	}

	/**
	 * 向集合中插入文档，每次插入多条
	 */
	@Test
	public void test02() {
		List<Document> documents = new ArrayList<Document>();
		for (int i = 0; i < 20; i++) {
			Document document = new Document("i", i);
			documents.add(document);
		}
		collection.insertMany(documents);
		System.out.println("成功向集合中插入文档");
	}

	/**
	 * 遍历集合
	 */
	@Test
	public void test03() {
		MongoCursor<Document> cursor = collection.find().iterator();
		System.out.println("集合中文档数量：" + collection.count());
		while (cursor.hasNext()) {
			Document document = cursor.next();
			System.out.println(document.toJson());
		}
	}

	/**
	 * 根据条件检索集合 i == 10 匹配条件，使用Filters的eq方法
	 */
	@Test
	public void test04() {
		Bson filter = Filters.eq("i", 10);
		Document document = collection.find(filter).first();
		System.out.println(document.toJson());
	}

	/**
	 * 根据条件检索集合 i >= 10 匹配条件，使用Filters的gte方法
	 */
	@Test
	public void test05() {
		Bson filter = Filters.gte("i", 10);
		FindIterable<Document> findIterable = collection.find(filter);
		MongoCursor<Document> documents = findIterable.iterator();
		while (documents.hasNext()) {
			System.out.println(documents.next());
		}
	}

	/**
	 * 根据条件检索集合 i >= 10 匹配条件，使用Filters的gte方法 使用collection.forEach(Block)遍历
	 */
	@Test
	public void test06() {
		Block<Document> block = new Block<Document>() {
			@Override
			public void apply(Document document) {
				System.out.println(document.toJson());
			}
		};
		Bson filter = Filters.gte("i", 10);
		FindIterable<Document> findIterable = collection.find(filter);
		findIterable.forEach(block);
	}

	/**
	 * 根据条件检索集合10<=i<=15 匹配条件 使用collection.forEach(Block)遍历
	 */
	@Test
	public void test07() {
		Block<Document> block = new Block<Document>() {
			@Override
			public void apply(Document document) {
				System.out.println(document.toJson());
			}
		};

		FindIterable<Document> findIterable = collection.find((Filters.and(
				Filters.gte("i", 10), Filters.lte("i", 15))));
		findIterable.forEach(block);
	}

	/**
	 * 更新数据
	 */
	@Test
	public void test08() {
		UpdateResult result = collection.updateOne(Filters.eq("i", 10),
				new Document("$set", new Document("i", 100)));
		System.out.println(result.toString());
	}

	/**
	 * 删除一条数据
	 */
	@Test
	public void test09() {
		DeleteResult result = collection.deleteOne(Filters.eq("i", 100));
		System.out.println("删除数据条数：" + result.getDeletedCount());
	}

	/**
	 * 删除多条数据
	 */
	@Test
	public void test10() {
		DeleteResult result = collection.deleteMany(Filters.gte("i", 10));
		System.out.println("删除数据条数：" + result.getDeletedCount());
	}

	/**
	 * 判断字段是否存在,如果存在就把存在这个字段的文档遍历出来
	 */
	@Test
	public void test11() {
		Bson bson = Filters.exists("clazzName");
		FindIterable<Document> fit = collection.find(bson);
		MongoCursor<Document> cursor = fit.iterator();
		while (cursor.hasNext()) {
			System.out.println(cursor.next());
		}
	}

	/**
	 * 获取除i==5的所有值，不存在i字段的也取出来
	 */
	@Test
	public void test12() {
		FindIterable<Document> it = collection.find(Filters.ne("i", 5));
		MongoCursor<Document> cursor = it.iterator();
		while (cursor.hasNext()) {
			System.out.println(cursor.next());
		}
	}

	/**
	 * 查询 3<=i<=9
	 */
	@Test
	public void test13() {
		// 方式一：
		FindIterable<Document> it = collection.find(new Document("i",
				new Document("$gte", 3).append("$lte", 9)));
		MongoCursor<Document> cursor = it.iterator();
		while (cursor.hasNext()) {
			System.out.println(cursor.next().toJson());
		}

		// 方式二：使用Filters助手方法指定相同的过滤条件
		FindIterable<Document> it1 = collection.find(and(gte("i", 3),
				lte("i", 9)));
		MongoCursor<Document> cursor1 = it1.iterator();
		while (cursor1.hasNext()) {
			System.out.println(cursor1.next().toJson());
		}
	}

	/**
		 * 
		 */
	@Test
	public void test14() {
		// 使用Filters助手方法指定相同的过滤条件
		FindIterable<Document> it1 = collection.find(and(gte("stars", 2),
				lte("stars", 9)));
		MongoCursor<Document> cursor1 = it1.iterator();
		while (cursor1.hasNext()) {
			System.out.println(cursor1.next().toJson());
		}
	}

	/**
	 * 插入一条新文档
	 */
	@Test
	public void test15() {
		Document document = new Document("name", "李四")
				.append("contact",
						new Document("phone", "228-555-0149").append("email",
								"cafeconleche@example.com").append("location",
								Arrays.asList(-73.92502, 40.8279556)))
				.append("stars", 3)
				.append("categories",
						Arrays.asList("Bakery", "Coffee", "Pastries"));

		collection.insertOne(document);
		System.out.println("insertOne成功插入一条数据");
	}

	/**
	 * 插入多条新文档
	 */
	@Test
	public void test16() {
		Document doc1 = new Document("name", "Amarcord Pizzeria")
				.append("contact",
						new Document("phone", "264-555-0193").append("email",
								"amarcord.pizzeria@example.net")
								.append("location",
										Arrays.asList(-73.88502, 40.749556)))
				.append("stars", 2)
				.append("categories",
						Arrays.asList("Pizzeria", "Italian", "Pasta"));

		Document doc2 = new Document("name", "Blue Coffee Bar")
				.append("contact",
						new Document("phone", "604-555-0102").append("email",
								"bluecoffeebar@example.com").append("location",
								Arrays.asList(-73.97902, 40.8479556)))
				.append("stars", 5)
				.append("categories", Arrays.asList("Coffee", "Pastries"));

		List<Document> documents = new ArrayList<Document>();
		documents.add(doc1);
		documents.add(doc2);

		collection.insertMany(documents);
		System.out.println("insertMany成功插入一条数据");
	}

	/**
	 * 更新单个文档 该updateOne()方法最多只能更新一个文档，即使过滤条件匹配集合中的多个文档。
	 * 更新其_id字段等于的文档ObjectId("57506d62f57802807471dd41")
	 * Updates.set设置的值stars字段1和contact.phone字段"228-555-9999"，和
	 * Updates.currentDate将lastModified字段修改为当前日期
	 * 。如果lastModified字段不存在，操作员将该字段添加到文档。
	 */
	@Test
	public void test17() {
		/**
		 * { "_id" : { "$oid" : "58de5d90039e9f1638e9084a" }, "name" :
		 * "Café Con Leche", "contact" : { "phone" : "228-555-0149", "email" :
		 * "cafeconleche@example.com", "location" : [-73.92502, 40.8279556] },
		 * "stars" : 3, "categories" : ["Bakery", "Coffee", "Pastries"] } {
		 * "_id" : { "$oid" : "58de6485039e9f18c0fd977e" }, "name" :
		 * "Amarcord Pizzeria", "contact" : { "phone" : "264-555-0193", "email"
		 * : "amarcord.pizzeria@example.net", "location" : [-73.88502,
		 * 40.749556] }, "stars" : 2, "categories" : ["Pizzeria", "Italian",
		 * "Pasta"] } { "_id" : { "$oid" : "58de6485039e9f18c0fd977f" }, "name"
		 * : "Blue Coffee Bar", "contact" : { "phone" : "604-555-0102", "email"
		 * : "bluecoffeebar@example.com", "location" : [-73.97902, 40.8479556]
		 * }, "stars" : 5, "categories" : ["Coffee", "Pastries"] }
		 */
		UpdateResult result = collection.updateOne(
				eq("_id", new ObjectId("58de5d90039e9f1638e9084a")),
				combine(set("stars", 1), set("contact.phone", "228-555-9999"),
						currentDate("lastModified")));
		System.out.println(result);
	}

	/**
	 * 更新多个文档 Updates.set将stars字段的值设置为0，和
	 * Updates.currentDate将lastModified字段修改为当前日期
	 * 。如果lastModified字段不存在，操作员将该字段添加到文档。
	 */
	@Test
	public void test18() {
		UpdateResult result = collection.updateMany(eq("stars", 2),
				combine(set("stars", 0), currentDate("lastModified")));
		System.out.println(result);
	}

	/**
	 * 替换文档 要替换文档，请将新文档传递给该replaceOne方法。 重要
	 * 替换文档可以具有与原始文档不同的字段。在替换文档中，您可以省略该_id字段，因为该_id字段是不可变的;
	 * 但是，如果您包含该_id字段，则不能为该字段指定其他值_id。
	 */
	@Test
	public void test19() {
		collection.replaceOne(
				eq("_id", new ObjectId("58de5d90039e9f1638e9084a")),
				new Document("name", "Green Salads Buffet").append("contact",
						"TBD").append("categories",
						Arrays.asList("Salads", "Health Foods", "Buffet")));
	}

	/**
	 * 删除文件 要删除集合中的文档，可以使用 deleteOne和deleteMany方法。
	 * 
	 * 过滤器
	 * 您可以将过滤器文档传递给方法以指定要删除的文档。过滤器文档规范与读取操作相同。为了方便创建过滤器对象，Java驱动程序提供了Filters助手。
	 * 
	 * 要指定一个空过滤器（即匹配集合中的所有文档），请使用空Document对象。
	 * 
	 * 删除单个文档 该deleteOne方法最多删除单个文档，即使过滤条件匹配集合中的多个文档。
	 */
	@Test
	public void test20() {
		collection
				.deleteOne(eq("_id", new ObjectId("58de6485039e9f18c0fd977e")));
		// 遍历一下，查看结果
		new MongoDb01().test03();
	}

	/**
	 * 删除多个集合
	 */
	@Test
	public void test21() {
		collection.deleteMany(and(gt("i", 0), lte("i", 5)));
		// 遍历一下，查看结果
		new MongoDb01().test03();
	}

	/**
	 * "name" : "Green Salads Buffet" 文本搜索 先创建索引 再按iltes的text方法查找
	 */
	@Test
	public void test22() {
		// 给name字段创建索引
		String index = collection.createIndex(Indexes.text("name"));
		System.out.println(index);
		// 输出：name_text
	}

	@Test
	public void test23() {
		// 结合22实例，进行文本搜索
		long count = collection.count(text("Coffee Pizzeria"));
		System.out.println(count);
		// 打印聚合结果
		collection.find(text("Coffee Pizzeria")).forEach(printBlock);

	}

	/**
	 * 文字分数 
	 * 对于每个匹配文档，文本搜索分配一个分数，表示文档与指定的文本搜索查询过滤器的相关性。要按分数返回并排序，
	 * 请使用$meta投影文档中的运算符和排序表达式。
	 * 注解：根据匹配度来打分，匹配度越高得分也越高，排序也越靠前
	 */
	@Test
	public void test24() {
		collection.find(text("Blue Coffee 张三  王麻子 Pizzeria"))
				.projection(Projections.metaTextScore("score"))
				.sort(Sorts.metaTextScore("score")).forEach(printBlock);

	}
	/**
	 * 指定搜索语言
	 */
	@Test
	public void test25() {
		long matchCountEnglish = collection.count(text("张三", new TextSearchOptions().language("english")));
		FindIterable<Document> find = collection.find(text("张三", new TextSearchOptions().language("english")));
		find.forEach(printBlock);
		System.out.println("Text search matches (english): " + matchCountEnglish);
		
	}
	
	/**
	 * ????????????????????????还不太明白
	 *地理空间搜索
		为了支持地理空间查询，MongoDB提供了各种地理空间索引以及地理空间查询算子。
	 */
	@Test
	public void test26() {
		MongoClient client = new MongoClient("192.168.0.17",27017);
		MongoDatabase database = client.getDatabase("test");
		MongoCollection<Document> collection2 = database.getCollection("restaurants");
		
		System.out.println(collection2.count());
		/*
		 * 创建2dsphere索引
		要创建2dsphere索引，请使用Indexes.geo2dsphere 帮助器创建2dsphere索引的规范并传递给MongoCollection.createIndex()方法。
		以下示例在集合2dsphere的"contact.location"字段上创建一个索引restaurants。
		 */
		collection2.createIndex(Indexes.geo2dsphere("contact.location"));
		
		/*
		 * 查询GeoJSON点附近的位置
			MongoDB提供了各种地理空间查询算子。为了便于创建地理空间查询过滤器，Java驱动程序提供了Filters类和com.mongodb.client.model.geojson包。
			以下示例返回距离指定的GeoJSON点至少1000米com.mongodb.client.model.geojson.Point，距离距离最远到最远距离最远5000米的文档：
		 */
		
		Point refPoint = new Point(new Position(-73.9667, 40.78));
		collection2.find(near("contact.location", refPoint, 5000.0, 1000.0)).forEach(printBlock);
	}
	
	

}
