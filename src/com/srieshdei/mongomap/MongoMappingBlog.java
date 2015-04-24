package com.srieshdei.mongomap;


import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.or;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.all;
import static com.mongodb.client.model.Filters.nin;
import static com.mongodb.client.model.Filters.elemMatch;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import com.mongodb.client.DistinctIterable;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.QueryBuilder;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;

public class MongoMappingBlog {
	MongoClient client=new MongoClient();
	MongoDatabase db=client.getDatabase("onlineAd");
	
	
	
	public  void simpleInsert()
	{

		MongoCollection<Document> collection=db.getCollection("customer");
		
		Document doc=new Document();
		doc.put("userid", "greg_66");
		doc.put("fname","Greg");
		
		// Variation
		HashMap<String,String> map=new HashMap<String,String>();
		map.put("lname", "Thomas");
		map.put("phone","777-888-9999");
		doc.putAll(map);
		
		//variation
		doc.append("email", "greg@gmail.com");
		doc.append("city","New York");
		
		collection.insertOne(doc);
		System.out.println("Inserted");
	}
	
	public  void selectWhere()
	{
		MongoCollection<Document> collection=db.getCollection("customer");
		
		Document wheredoc=new Document();
		wheredoc.put("userid","sri_76");
		
		Bson select=Projections.fields(Projections.include("userid","fname","lname"), Projections.exclude("_id"));
				
		Iterator<Document> cur=collection.find(wheredoc).projection(select).into(new ArrayList<Document>()).iterator();
	
		while(cur.hasNext())
		{
		Document doc=cur.next();
		System.out.println("Customer: "+doc.get("userid")+" - "+doc.get("fname")+" "+doc.get("lname"));
		}
	}

	
	public  void selectWhereANDOrderby()
	{
		MongoCollection<Document> collection=db.getCollection("customer");

		Document wheredoc=new Document();
		wheredoc.put("city","New York");
		wheredoc.put("active","Y");
		Bson sort=Sorts.orderBy(Sorts.descending("fname"), Sorts.ascending("user_id"));		
		
		Iterator<Document> cur=collection.find(wheredoc).sort(sort).into(new ArrayList<Document>()).iterator();
		System.out.println("\nPrinting Select Order by results");
		print(cur);
				
	}

	
	public void selectWhereOR()
	{
		MongoCollection<Document> collection=db.getCollection("customer");
		//var where={$or:[{'fname':'Greg'},{'city':'New York'}]};
		
		Bson filter=or(eq("fname", "Greg"),eq("city","New York"));
		Iterator<Document> cur=collection.find(filter).into(new ArrayList<Document>()).iterator();
		print(cur);
	}
	
	
	public void selectSkipLimit()
	{
		MongoCollection<Document> collection=db.getCollection("customer");

			Iterator<Document> cur=collection.find()
					.sort(new BasicDBObject("userid", 1))
					.skip(1)
					.limit(5)
					.into(new ArrayList<Document>()).iterator();;
					print(cur);
	}
	
	public void selectConditionsGTLTEQ()
	{
		MongoCollection<Document> collection=db.getCollection("item");
		Bson filter=gte("price", 420);
		
		Iterator<Document> cur=collection.find(filter)
					.into(new ArrayList<Document>()).iterator();
		print(cur);
	}
	
	public void selectisNullisNotNull()
	{
		MongoCollection<Document> collection=db.getCollection("customer");
	 
		 Bson where=new Document("phone",new Document("$exists", true))
		 .append("email", new Document("$exists", false));
		 
		 Iterator<Document> cur=collection.find(where)
					.into(new ArrayList<Document>()).iterator();;
					print(cur);
	}
	
	public void selectLike()
	{
		MongoCollection<Document> collection=db.getCollection("customer");
	
		Document where=new Document();
		Pattern regex = Pattern.compile("^Es.*");
		where.put("lname",regex );
		Iterator<Document> cur=collection.find(where)
					.into(new ArrayList<Document>()).iterator();;
		print(cur);
	}
	
	public void selectWhereIN()
	{
		MongoCollection<Document> collection=db.getCollection("customer");
		Bson filter=in("city","New York","San Francisco");
		Iterator<Document> cur=collection.find(filter).into(new ArrayList<Document>()).iterator();
		print(cur);
	}
	
	public void selectWhereAll()
	{
		MongoCollection<Document> collection=db.getCollection("customer");
		Pattern regex1 = Pattern.compile(".*TV.*");
		Pattern regex2 = Pattern.compile(".*LED.*");
		Pattern regex3 = Pattern.compile(".*57.*");
		Bson filter=all("searchpattern",regex1,regex2,regex3);
		Iterator<Document> cur=collection.find(filter).into(new ArrayList<Document>()).iterator();
		print(cur);
	}
	
	public void selectElemMatch()
	{
		MongoCollection<Document> collection=db.getCollection("customer");
		Bson filter=elemMatch("clickedads",and(eq("adseller","Frys"),gte("purchasedquantity",1)));
		Pattern regex1 = Pattern.compile(".*Sr.*");
		Bson filter1=and(filter, eq("fname",regex1));
		Iterator<Document> cur=collection.find(filter1).into(new ArrayList<Document>()).iterator();
		print(cur);
	}
	
	
	public void selectDistinct()
	{
		MongoCollection<Document> collection=db.getCollection("customer");
		 
		// Bson where=new Document("$distinct","phone");
		DistinctIterable result=	collection.distinct("phone", TestResult.class);

		System.out.println(result.first());
		/* Iterator<Document> cur=collection.distinct("phone", new ArrayList<TResult>())
					.into(new ArrayList<Document>()).iterator();;
					print(cur);*/
	}
	
	
	public void selectCount()
	{}
	
	
	
	public void print(Iterator<Document> cur)
	{
		System.out.println("Going to print");
				  
		while(cur.hasNext())
		{
		Document doc=cur.next();
		System.out.println(doc);
		//System.out.println("Customer: "+doc.get("userid")+" - "+doc.get("city")+" - "+doc.get("fname")+" "+doc.get("lname"));
		}
	}
	
	
	public class TestResult implements Serializable
	{
		
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		MongoMappingBlog mongomap=new MongoMappingBlog();
		//	mongomap.simpleInsert();
		//mongomap.simpleSelect();
		//mongomap.selectOrderby();
		// mongomap.selectWhereANDOrderby();\
		
		// mongomap.selectWhereOR();
	//	mongomap.selectSkipLimit();
	//	mongomap.selectConditionsGTLTEQ();
	//	mongomap.selectisNullisNotNull();
		//mongomap.selectLike();
		
	//	mongomap.selectDistinct();
		
	//	mongomap.selectWhereIN();
		
	//	mongomap.selectWhereAll();
		
		mongomap.selectElemMatch();
		

	}

}


