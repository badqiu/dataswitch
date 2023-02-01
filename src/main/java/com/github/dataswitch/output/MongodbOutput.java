package com.github.dataswitch.output;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.util.Assert;

import com.github.dataswitch.enums.OutputMode;
import com.github.dataswitch.input.MongodbInput;
import com.github.dataswitch.support.MongodbProvider;
import com.github.dataswitch.util.InputOutputUtil;
import com.github.dataswitch.util.ScriptEngineUtil;
import com.github.dataswitch.util.Util;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOptions;

public class MongodbOutput extends MongodbProvider implements Output {
	private OutputMode outputMode = OutputMode.insert;
	private String whereScript;
	private String language;
	private String primaryKeys; //主键，如果outputMode为replace,update,delete时，需要使用
	private String[] _primaryKeysArray; 
	
	private Function<Map<String,Object>,Bson> whereFunction = null;
	
	private String columns; //要写入的列
	private String[] _columnsArray; //要写入的列

	MongoClient _client;
	MongoDatabase _database = null;
	MongoCollection<Document> _mongoCollection;
	
	
	public OutputMode getOutputMode() {
		return outputMode;
	}

	public void setOutputMode(OutputMode outputMode) {
		this.outputMode = outputMode;
	}

	public String getWhereScript() {
		return whereScript;
	}

	public void setWhereScript(String filterScript) {
		this.whereScript = filterScript;
	}

	public String getLanguage() {
		return language;
	}

	public void setLanguage(String language) {
		this.language = language;
	}

	public Function<Map<String, Object>, Bson> getWhereFunction() {
		return whereFunction;
	}

	public void setWhereFunction(Function<Map<String, Object>, Bson> filterFunction) {
		this.whereFunction = filterFunction;
	}
	
	public String getPrimaryKeys() {
		return primaryKeys;
	}

	public void setPrimaryKeys(String primaryKeys) {
		this.primaryKeys = primaryKeys;
	}

	public String getColumns() {
		return columns;
	}

	public void setColumns(String columns) {
		this.columns = columns;
	}

	@Override
	public void write(List<Object> rows) {
		writeByOutputMode((List)rows,outputMode);
	}

	protected void writeByOutputMode(List<Map<String,Object>> rows,OutputMode outputMode) {
		if(CollectionUtils.isEmpty(rows)) return;
		
		if(outputMode == OutputMode.insert) {
			List<Document> documents = toDocuments(rows);
			_mongoCollection.insertMany(documents);
		}else if(outputMode == OutputMode.replace) {
			for(Map<String,Object> row : rows) {
				ReplaceOptions upsert = new ReplaceOptions().upsert(true);
				_mongoCollection.replaceOne(getFilter(row), getDocByColumns(row),upsert);
			}
		}else if(outputMode == OutputMode.update) {
			for(Map<String,Object> row : rows) {
				Document doc = getDocByColumns(row);
				UpdateOptions updateOptions = new UpdateOptions();
				_mongoCollection.updateOne(getFilter(row), new Document("$set",doc),updateOptions);
			}
		}else if(outputMode == OutputMode.delete) {
			for(Map<String,Object> row : rows) {
				_mongoCollection.deleteOne(getFilter(row));
			}
		}else {
			throw new UnsupportedOperationException("unsupport outputMode:"+outputMode);
		}
		
	}
	
	public Document getDocByColumns(Map row) {
		Map doc = MongodbInput.getDocByColumns(row, _columnsArray);
		return new Document(doc);
	}

	private Bson getFilter(Map<String,Object> row) {
		if(whereFunction != null) {
			return whereFunction.apply(row);
		}
		
		if(ArrayUtils.isNotEmpty(_primaryKeysArray)) {
			return getPrimaryKeysFilter(row,_primaryKeysArray);
		}
		
//		if(StringUtils.isNotBlank(whereJson)) {
//			return Document.parse(whereJson);
//		}
		
		Assert.hasText(whereScript,"primaryKeys or whereScript must be not blank");
		return (Bson)ScriptEngineUtil.eval(language, whereScript,row);
	}

	public static Bson getPrimaryKeysFilter(Map<String, Object> row,String[] primaryKeysArray) {
		Bson filter = null;
		for(String fieldName : primaryKeysArray) {
			Bson condition = Filters.eq(fieldName, row.get(fieldName));
			if(filter == null) {
				filter = condition;
			}else {
				filter = Filters.and(condition);
			}
		}
		
		return filter;
	}

	private List<Document> toDocuments(List<Map<String,Object>> rows) {
		List<Document> documents = new ArrayList<Document>(rows.size());
		for(Map row : rows) {
			Document doc = getDocByColumns(row);
			documents.add(doc);
		}
		return documents;
	}
	
	@Override
	public void open(Map<String, Object> params) throws Exception {
		_client = createMongoClient();
		_database = _client.getDatabase(getDatabase());
		
		_mongoCollection = _database.getCollection(getCollection());
		
		_primaryKeysArray = Util.splitColumns(primaryKeys);
		_columnsArray = Util.splitColumns(columns);
	}

	@Override
	public void close() throws Exception {
		InputOutputUtil.close(_client);
	}
	
	@Override
	public void flush() throws IOException {
	}

}
