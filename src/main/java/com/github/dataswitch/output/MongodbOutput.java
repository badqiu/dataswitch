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
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;

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
		}else if(outputMode == OutputMode.upsert) {
//			executeBySingleReplace(rows);
			executeByBatchReplace(rows);
		}else if(outputMode == OutputMode.update) {
//			executeBySingleUpdate(rows);
			executeByBatchUpdate(rows);
		}else if(outputMode == OutputMode.delete) {
//			executeBySingleDelete(rows);
			executeByBatchDelete(rows);
		}else {
			throw new UnsupportedOperationException("unsupport outputMode:"+outputMode);
		}
		
	}

	private void executeByBatchDelete(List<Map<String, Object>> rows) {
		List<WriteModel<Document>> writes = new ArrayList<>();
		for (Map<String, Object> row : rows) {
		    Bson filter = getFilter(row);
		    DeleteOneModel<Document> deleteOne = new DeleteOneModel<>(filter);
		    writes.add(deleteOne);
		}
		BulkWriteOptions bulkWriteOptions = new BulkWriteOptions().ordered(false);
		BulkWriteResult bulkResult = _mongoCollection.bulkWrite(writes, bulkWriteOptions);
	}

	private void executeByBatchUpdate(List<Map<String, Object>> rows) {
		List<UpdateOneModel<Document>> updates = new ArrayList<UpdateOneModel<Document>>();
		for (Map<String, Object> row : rows) {
		    Document doc = getDocByColumns(row);
		    Bson filter = getFilter(row);
		    UpdateOneModel<Document> update = new UpdateOneModel<Document>(filter, new Document("$set", doc));
		    updates.add(update);
		}
		BulkWriteResult result = _mongoCollection.bulkWrite(updates);
	}

	private void executeByBatchReplace(List<Map<String, Object>> rows) {
		List<WriteModel<Document>> writes = new ArrayList<>();
		for (Map<String, Object> row : rows) {
		    Bson filter = getFilter(row);
		    Document replacement = getDocByColumns(row);
		    ReplaceOneModel<Document> replaceOne = new ReplaceOneModel<>(filter, replacement, new ReplaceOptions().upsert(true));
		    writes.add(replaceOne);
		}
		BulkWriteOptions bulkWriteOptions = new BulkWriteOptions().ordered(false);
		BulkWriteResult bulkResult = _mongoCollection.bulkWrite(writes, bulkWriteOptions);
	}

	private void executeBySingleDelete(List<Map<String, Object>> rows) {
		DeleteOptions deleteOptions = new DeleteOptions();
		for(Map<String,Object> row : rows) {
			_mongoCollection.deleteOne(getFilter(row),deleteOptions);
		}
	}

	private void executeBySingleUpdate(List<Map<String, Object>> rows) {
		UpdateOptions updateOptions = new UpdateOptions();
		for(Map<String,Object> row : rows) {
			Document doc = getDocByColumns(row);
			_mongoCollection.updateOne(getFilter(row), new Document("$set",doc),updateOptions);
		}
	}

	private void executeBySingleReplace(List<Map<String, Object>> rows) {
		ReplaceOptions replaceOptions = new ReplaceOptions().upsert(true);
		for(Map<String,Object> row : rows) {
			_mongoCollection.replaceOne(getFilter(row), getDocByColumns(row),replaceOptions);
		}
	}
	
	public Document getDocByColumns(Map row) {
		Map doc = MongodbInput.getDocByColumns(row, _columnsArray);
		return new Document(doc);
	}

	static final String MONGODB_ID_KEY = "_id";
	static final String[] MONGODB_ID_KEY_ARRAY = new String[] {MONGODB_ID_KEY};
	private Bson getFilter(Map<String,Object> row) {
		
		if(whereFunction != null) {
			return whereFunction.apply(row);
		}
		
		if(ArrayUtils.isNotEmpty(_primaryKeysArray)) {
			return getPrimaryKeysFilter(row,_primaryKeysArray);
		}
		
		Object mongodbId = row.get(MONGODB_ID_KEY);
		if(mongodbId != null) {
			return getPrimaryKeysFilter(row,MONGODB_ID_KEY_ARRAY);
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
