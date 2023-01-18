package com.github.dataswitch.output;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.springframework.util.StringUtils;

import com.github.dataswitch.util.InputOutputUtil;

public class ElasticsearchOutput implements Output{
	private String hosts; //One or more Elasticsearch hosts to connect to, e.g. 'http://host_name:9092;http://host_name:9093'.
	private String username;
	private String password;
	private String index; //Elasticsearch index for every record. Can be a static index (e.g. 'myIndex') or a dynamic index (e.g. 'index-{log_ts|yyyy-MM-dd}'). 
	
	private String documentIdKeyDelimiter = "_"; //Delimiter for composite keys ("_" by default), e.g., "$" would result in IDs "KEY1$KEY2$KEY3".
	private String connectionPathPrefix; //Prefix string to be added to every REST communication, e.g., '/v1'.
	
	private int flushBatchSize;
	private int flushInterval;
	private int flushMaxMemorySize;
	private int maxRetry;
	private int retryInterval;
	
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

	private synchronized RestHighLevelClient makeConnection() {
        RestHighLevelClient client = null;
        HttpHost[] hostList = newHttpHostArray(hosts);
        if(StringUtils.isEmpty(username)) {
        	client = new RestHighLevelClient(RestClient.builder(hostList));
        }else {
        	final BasicCredentialsProvider basicCredentialsProvider = new BasicCredentialsProvider();
            basicCredentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username,password));
            
			client = new RestHighLevelClient(
	                RestClient.builder(hostList)
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
		String[] hostList = StringUtils.tokenizeToStringArray(hosts, ",; \t\n");
		List<HttpHost> result = new ArrayList<HttpHost>();
		for(String host : hostList) {
			HttpHost item = HttpHost.create(host);
			result.add(item);
		}
		return result.toArray(new HttpHost[result.size()]);
	}

    @Override
    public void write(List<Object> rows)  {
    	try {
			write0((List)rows);
		} catch (IOException e) {
			throw new RuntimeException("write error",e);
		}
    }

	private void write0(List<Map> rows) throws IOException {
		if(CollectionUtils.isEmpty(rows)) return;
    	
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
	
	@Override
	public void open(Map<String, Object> params) throws Exception {
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
