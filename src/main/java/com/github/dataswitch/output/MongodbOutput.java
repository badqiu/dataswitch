package com.github.dataswitch.output;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.commons.collections.CollectionUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.util.Assert;

import com.github.dataswitch.enums.OutputMode;
import com.github.dataswitch.support.MongodbProvider;
import com.github.dataswitch.util.InputOutputUtil;
import com.github.dataswitch.util.ScriptEngineUtil;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class MongodbOutput extends MongodbProvider implements Output {
	private OutputMode outputMode = OutputMode.insert;
	private String filterScript;
	private String language;
	
	private Function<Map<String,Object>,Bson> filterFunction = null;

	private MongoClient _client;
	private MongoDatabase _database = null;
	private MongoCollection<Document> _mongoCollection;
	
	
	public OutputMode getOutputMode() {
		return outputMode;
	}

	public void setOutputMode(OutputMode outputMode) {
		this.outputMode = outputMode;
	}

	public String getFilterScript() {
		return filterScript;
	}

	public void setFilterScript(String filterScript) {
		this.filterScript = filterScript;
	}

	public String getLanguage() {
		return language;
	}

	public void setLanguage(String language) {
		this.language = language;
	}

	public Function<Map<String, Object>, Bson> getFilterFunction() {
		return filterFunction;
	}

	public void setFilterFunction(Function<Map<String, Object>, Bson> filterFunction) {
		this.filterFunction = filterFunction;
	}

	@Override
	public void write(List<Object> rows) {
		writeByOutputMode((List)rows);
	}

	private void writeByOutputMode(List<Map<String,Object>> rows) {
		if(CollectionUtils.isEmpty(rows)) return;
		
		if(outputMode == OutputMode.insert) {
			List<Document> documents = toDocuments(rows);
			_mongoCollection.insertMany(documents);
		}else if(outputMode == OutputMode.replace) {
			for(Map<String,Object> row : rows) {
				_mongoCollection.replaceOne(getFilter(row), new Document(row));
			}
		}else if(outputMode == OutputMode.update) {
			for(Map<String,Object> row : rows) {
				_mongoCollection.updateOne(getFilter(row), new Document(row));
			}
		}else if(outputMode == OutputMode.delete) {
			for(Map<String,Object> row : rows) {
				_mongoCollection.deleteOne(getFilter(row));
			}
		}else {
			throw new UnsupportedOperationException("unsupport outputMode:"+outputMode);
		}
		
	}

	private Bson getFilter(Map<String,Object> row) {
		if(filterFunction != null) {
			return filterFunction.apply(row);
		}
		
		Assert.hasText(filterScript,"filterScript must be not blank");
		return (Bson)ScriptEngineUtil.eval(language, filterScript,row);
	}

	private List<Document> toDocuments(List<Map<String,Object>> rows) {
		List<Document> documents = new ArrayList<Document>(rows.size());
		for(Map row : rows) {
			Document doc = new Document(row);
			documents.add(doc);
		}
		return documents;
	}
	
	@Override
	public void open(Map<String, Object> params) throws Exception {
		_client = createMongoClient();
		_database = _client.getDatabase(getDatabase());
		
		_mongoCollection = _database.getCollection(getCollectionName());
	}

	@Override
	public void close() throws Exception {
		InputOutputUtil.close(_client);
	}
	
	@Override
	public void flush() throws IOException {
	}

}
