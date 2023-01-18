package com.github.dataswitch.support;

import com.github.dataswitch.BaseObject;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

public class MongodbProvider extends BaseObject{
	private String url;
	private String database;
	private String collectionName;
	
	public MongoClient createMongoClient() {
		MongoClient r = MongoClients.create(url);
		return r;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getDatabase() {
		return database;
	}

	public void setDatabase(String database) {
		this.database = database;
	}

	public String getCollectionName() {
		return collectionName;
	}

	public void setCollectionName(String collectionName) {
		this.collectionName = collectionName;
	}
	
	
	
	
}
