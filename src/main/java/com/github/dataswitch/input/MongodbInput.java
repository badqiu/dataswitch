package com.github.dataswitch.input;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.commons.lang.StringUtils;
import org.bson.Document;

import com.github.dataswitch.support.MongodbProvider;
import com.github.dataswitch.util.InputOutputUtil;
import com.github.dataswitch.util.ScriptEngineUtil;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

public class MongodbInput extends MongodbProvider implements Input {
	private MongoClient _client;
	private MongoDatabase _database = null;

	private int limit;
	private int skip;
	
	private String findScript;
	private String language;
	private Function<MongoCollection<Document>,FindIterable<Document>> findFunction = null;
	
	private MongoCollection<Document> _mongoCollection;
	private MongoCursor<Document> _mongoCursor;
	private FindIterable<Document> _findIterable;
	
	@Override
	public List<Object> read(int size) {
		List<Map> results = new ArrayList<Map>(size);
		for(int i = 0; _mongoCursor.hasNext() && i < size; i++) {
			Document doc = _mongoCursor.next();
			results.add(doc);
		}
		return (List)results;
	}
	
	private FindIterable<Document> executeMongodbFind() {
		FindIterable<Document> result = null;
		if(findFunction != null) {
			result = findFunction.apply(_mongoCollection);
		}else if(StringUtils.isBlank(findScript)) {
			result = _mongoCollection.find();
		}else {
			Map<String,Object> context = new HashMap<String,Object>();
			context.put("mongoCollection", _mongoCollection);
			result = (FindIterable)ScriptEngineUtil.eval(language, findScript, context);
		}
		
		if(limit > 0) {
			result.limit(limit);
		}
		if(skip > 0) {
			result.skip(skip);
		}
		return result;
	}
	
	@Override
	public void open(Map<String, Object> params) throws Exception {
		_client = createMongoClient();
		_database = _client.getDatabase(getDatabase());
		
		_mongoCollection = _database.getCollection(getCollectionName());
		
		_findIterable = executeMongodbFind();
		_mongoCursor = _findIterable.iterator();
	}

	@Override
	public void close() throws Exception {
		InputOutputUtil.close(_mongoCursor);
		InputOutputUtil.close(_client);
	}
}
