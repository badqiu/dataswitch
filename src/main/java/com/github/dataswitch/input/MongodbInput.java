package com.github.dataswitch.input;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.commons.lang.StringUtils;
import org.bson.Document;

import com.github.dataswitch.enums.Constants;
import com.github.dataswitch.support.MongodbProvider;
import com.github.dataswitch.util.InputOutputUtil;
import com.github.dataswitch.util.ScriptEngineUtil;
import com.github.dataswitch.util.Util;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

public class MongodbInput extends MongodbProvider implements Input {
	
	private String whereJson; //查询过滤JSON条件
	private String whereScript; //查询过滤脚本
	private String language; //查询过滤语言: groovy
	
	private Function<MongoCollection<Document>,FindIterable<Document>> findFunction = null;
	private int limit;
	private int skip;
	private int batchSize = Constants.DEFAULT_BUFFER_SIZE;
	
	private String columns; //要读取的列
	private String[] _columnsArray;
	
	private MongoClient _client;
	private MongoDatabase _database = null;
	
	private MongoCollection<Document> _mongoCollection;
	private FindIterable<Document> _findIterable;
	private MongoCursor<Document> _mongoCursor;
	
	public String getWhereScript() {
		return whereScript;
	}

	public void setWhereScript(String whereScript) {
		this.whereScript = whereScript;
	}

	public String getLanguage() {
		return language;
	}

	public void setLanguage(String language) {
		this.language = language;
	}

	public Function<MongoCollection<Document>, FindIterable<Document>> getFindFunction() {
		return findFunction;
	}

	public void setFindFunction(Function<MongoCollection<Document>, FindIterable<Document>> findFunction) {
		this.findFunction = findFunction;
	}

	public int getLimit() {
		return limit;
	}

	public void setLimit(int limit) {
		this.limit = limit;
	}

	public int getSkip() {
		return skip;
	}

	public void setSkip(int skip) {
		this.skip = skip;
	}

	public int getBatchSize() {
		return batchSize;
	}

	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}
	
	public String getWhereJson() {
		return whereJson;
	}

	public void setWhereJson(String whereJson) {
		this.whereJson = whereJson;
	}

	public String getColumns() {
		return columns;
	}

	public void setColumns(String columns) {
		this.columns = columns;
	}

	@Override
	public List<Object> read(int size) {
		List<Map> results = new ArrayList<Map>(size);
		for(int i = 0; _mongoCursor.hasNext() && i < size; i++) {
			Document doc = _mongoCursor.next();
			Map result = getDocByColumns(doc);
			results.add(result);
		}
		return (List)results;
	}

	private Map getDocByColumns(Document doc) {
		if(_columnsArray == null) {
			return doc;
		}
		
		Map result = new HashMap(_columnsArray.length * 2);
		for(String key : _columnsArray) {
			result.put(key,doc.get(key));
			
		}
		return result;

	}
	
	private FindIterable<Document> executeFindAndChange() {
		FindIterable<Document> result = executeFind();
		
		if(batchSize > 0) {
			_findIterable.batchSize(batchSize);
		}
		if(limit > 0) {
			result.limit(limit);
		}
		if(skip > 0) {
			result.skip(skip);
		}
		return result;
	}

	private FindIterable<Document> executeFind() {
		if(findFunction != null) {
			return findFunction.apply(_mongoCollection);
		}
		
		FindIterable<Document> result = null;
		if(StringUtils.isNotBlank(whereScript)) {
			Map<String,Object> context = new HashMap<String,Object>();
			context.put("mongoCollection", _mongoCollection);
			result = (FindIterable)ScriptEngineUtil.eval(language, whereScript, context);
			return result;
		}
		
		Document filter = new Document();
		if(StringUtils.isNotBlank(whereJson)) {
			filter = Document.parse(whereJson);
		}
		result = _mongoCollection.find(filter);
		return result;
	}
	
	@Override
	public void open(Map<String, Object> params) throws Exception {
		_client = createMongoClient();
		_database = _client.getDatabase(getDatabase());
		
		_mongoCollection = _database.getCollection(getCollection());
		
		_findIterable = executeFindAndChange();
		_mongoCursor = _findIterable.iterator();
		
		_columnsArray = Util.splitColumns(columns);
	}

	@Override
	public void close() throws Exception {
		InputOutputUtil.close(_mongoCursor);
		InputOutputUtil.close(_client);
	}
}
