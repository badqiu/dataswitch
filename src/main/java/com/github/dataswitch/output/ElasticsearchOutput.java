package com.github.dataswitch.output;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.springframework.util.Assert;

import com.github.dataswitch.enums.OutputMode;
import com.github.dataswitch.util.InputOutputUtil;
import com.github.dataswitch.util.TableName;
import com.github.dataswitch.util.Util;

public class ElasticsearchOutput implements Output,TableName{
	private String hosts; //One or more Elasticsearch hosts to connect to, e.g. 'http://host_name:9092;http://host_name:9093'.
	private String username;
	private String password;
	private String index; //Elasticsearch index for every record. Can be a static index (e.g. 'myIndex') or a dynamic index (e.g. 'index-{log_ts|yyyy-MM-dd}'). 
	
	private String documentIdKeyDelimiter = "_"; //Delimiter for composite keys ("_" by default), e.g., "$" would result in IDs "KEY1$KEY2$KEY3".
	private String connectionPathPrefix; //Prefix string to be added to every REST communication, e.g., '/v1'.
	
	private int batchSize;
	private int batchTimeout;
	
	private int flushMaxMemorySize;
	private int maxRetry;
	private int retryInterval;
	private String settings; // {"index" :{"number_of_shards": 1, "number_of_replicas": 0}};
	private String columns; //要写的列
	
	private String primaryKeys; //主键字段,多列用逗号分隔
	private String[] _primaryKeys; //主键
	
	private boolean dropIndex; //写入前，删除索引
	private boolean createIndex;
	private boolean ignoreWriteError;
	
	private OutputMode outputMode = OutputMode.upsert;
	
	
	private RestHighLevelClient _client;
	
    public String getHosts() {
		return hosts;
	}

	public void setHosts(String hosts) {
		this.hosts = hosts;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getIndex() {
		return index;
	}

	public void setIndex(String index) {
		this.index = index;
	}
	
	public void setTable(String table) {
		setIndex(table);
	}
	
	@Override
	public String getTable() {
		return getIndex();
	}
	
	public String getDocumentIdKeyDelimiter() {
		return documentIdKeyDelimiter;
	}

	public void setDocumentIdKeyDelimiter(String documentIdKeyDelimiter) {
		this.documentIdKeyDelimiter = documentIdKeyDelimiter;
	}

	public String getPrimaryKeys() {
		return primaryKeys;
	}

	public void setPrimaryKeys(String primaryKeys) {
		this.primaryKeys = primaryKeys;
		_primaryKeys = Util.splitColumns(primaryKeys);
	}

	private int timeout = 1000 * 30;
	private synchronized RestHighLevelClient makeConnection() {
        RestHighLevelClient client = null;
        HttpHost[] hostList = newHttpHostArray(hosts);
        RestClientBuilder restClient = RestClient.builder(hostList);
        if(StringUtils.isNotBlank(connectionPathPrefix)) {
        	restClient.setPathPrefix(connectionPathPrefix);
        }
        
        int connectTimeout = timeout;
        int socketTimeout = timeout;
        int connectionRequestTimeout = timeout;
        
        restClient.setRequestConfigCallback(requestConfigBuilder -> {
                    requestConfigBuilder.setConnectTimeout(connectTimeout);
                    requestConfigBuilder.setSocketTimeout(socketTimeout);
                    requestConfigBuilder.setConnectionRequestTimeout(connectionRequestTimeout);
                    return requestConfigBuilder;
                });
    
		if(StringUtils.isBlank(username)) {
        	client = new RestHighLevelClient(restClient);
        }else {
        	final BasicCredentialsProvider basicCredentialsProvider = new BasicCredentialsProvider();
            basicCredentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username,password));
            
			client = new RestHighLevelClient(
	                restClient
	                        .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
	                            @Override
	                            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
	                                httpClientBuilder.disableAuthCaching();
	                                return httpClientBuilder.setDefaultCredentialsProvider(basicCredentialsProvider);
	                            }
	                        })
	        );
        }

        return client;
    }

	private static HttpHost[] newHttpHostArray(String hosts) {
		Assert.hasText(hosts,"hosts must be not blank");
		String[] hostList = org.springframework.util.StringUtils.tokenizeToStringArray(hosts, ",; \t\n");
		List<HttpHost> result = new ArrayList<HttpHost>();
		for(String host : hostList) {
			HttpHost item = HttpHost.create(host);
			result.add(item);
		}
		return result.toArray(new HttpHost[result.size()]);
	}

    @Override
    public void write(List<Map<String, Object>> rows)  {
    	try {
			write0((List)rows);
		} catch (IOException e) {
			throw new RuntimeException("write error",e);
		}
    }

	private void write0(List<Map> rows) throws IOException {
		if(CollectionUtils.isEmpty(rows)) return;
    	
//		writeBySingleAction(rows);
		
		BulkRequest bulkRequest = new BulkRequest();
		for(Map row : rows) {
			DocWriteRequest request = getRequestByOutputMode(outputMode,row);
			bulkRequest.add(request);
		}
		
		BulkResponse bulkResponse = _client.bulk(bulkRequest, RequestOptions.DEFAULT);
		
	    if (bulkResponse.hasFailures()) {
	        String firstFailureMessage = bulkResponse.getItems()[0].getFailureMessage();
			throw new RuntimeException("Failed to write data to Elasticsearch!,rows.size:"+rows.size()+" firstRow:"+rows.get(0)+" firstFailMessage:"+firstFailureMessage);
	    }
	}

	private void writeBySingleAction(List<Map> rows) throws IOException {
		IndicesClient indicesClient = _client.indices();
		for(Map row : rows) {
	    	CreateIndexRequest request = new CreateIndexRequest(index);
	    	
			//      request.settings(Settings.builder()
			//      .put("index.number_of_shards", 1)
			//      .put("index.number_of_replicas", 0)
				//);
	    	
	    	request.mapping(row);
			CreateIndexResponse createIndexResponse = indicesClient.create(request, RequestOptions.DEFAULT);
    	}
	}
	
	private DocWriteRequest getRequestByOutputMode(OutputMode outputMode2, Map row) {
		if(outputMode == OutputMode.upsert || outputMode == OutputMode.insert) {
			IndexRequest request = new IndexRequest(index);
			
			if(org.apache.commons.lang3.StringUtils.isNotBlank(primaryKeys)) {
				String id = getIdFromData(row);
				request.id(id);
			}
			
			request.source(row);
			return request;
		}if(outputMode == OutputMode.update) {
			String id = getIdFromData(row);
			if(StringUtils.isBlank(id)) {
				throw new IllegalArgumentException("not found id value by primaryKeys:"+primaryKeys+" on row:"+row);
			}
			UpdateRequest request = new UpdateRequest(index,id);
			request.doc(row);
			return request;
		}else if(outputMode == OutputMode.delete) {
			String id = getIdFromData(row);
			if(StringUtils.isBlank(id)) {
				throw new IllegalArgumentException("not found id value by primaryKeys:"+primaryKeys+" on row:"+row);
			}
			DeleteRequest request = new DeleteRequest(index,id);
			return request;
		}else {
			throw new UnsupportedOperationException("Unsupported outputMode:"+outputMode);
		}
	}

	protected String getIdFromData(Map row) {
		if(_primaryKeys == null) return null;
		
		if(_primaryKeys.length == 1) {
			Object id = row.get(primaryKeys);
			if(id == null) return null;
			return String.valueOf(id); 
		}else {
			StringJoiner joiner = new StringJoiner(documentIdKeyDelimiter);
			for(String key : _primaryKeys) {
				Object id = row.get(key);
				joiner.add(String.valueOf(id));
			}
			return joiner.toString();
		}
	}

	@Override
	public void open(Map<String, Object> params) throws Exception {
		_primaryKeys = Util.splitColumns(primaryKeys);
		_client = makeConnection();
	}
	
	@Override
	public void close() throws Exception {
		flush();
		InputOutputUtil.close(_client);
		_client = null;
	}
	
	@Override
	public void flush() throws IOException {
		IndicesClient indicesClient = _client.indices();
		indicesClient.flush(new FlushRequest(index), RequestOptions.DEFAULT);
	}

}
