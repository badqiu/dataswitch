package com.github.dataswitch.output;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.springframework.util.Assert;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dataswitch.enums.Constants;
import com.github.dataswitch.util.CsvUtils;

public class DorisStreamLoadOutput implements Output {

	private static String FORMAT_JSON = "json";
	private static String FORMAT_CSV = "csv";
	
	private static String CSV_COLUMN_SEPARATOR = Constants.COLUMN_SPLIT;
	
    // Doris 连接配置
    private String host;
    private int port = 8030;
    private String database;
    private String table;
    private String user;
    private String password;
    private String format = "json";  // 支持json/csv
    private String csvColumnSeparator = CSV_COLUMN_SEPARATOR;

    
    private int timeoutSeconds = 600; // 默认超时600秒
    
    // 性能优化参数
    private int batchSize = 5000;     // 批次大小
    private int retryCount = 3;       // 失败重试次数
    private CloseableHttpClient httpClient;
    
    private Map<String,String> httpHeaders = new HashMap<String,String>();
    
    private List<String> csvColumns = new ArrayList();

    @Override
    public void open(Map<String, Object> params) throws Exception {
        // 从参数初始化配置
        if (params.containsKey("host")) host = (String) params.get("host");
        if (params.containsKey("port")) port = (int) params.get("port");
        if (params.containsKey("database")) database = (String) params.get("database");
        if (params.containsKey("table")) table = (String) params.get("table");
        if (params.containsKey("user")) user = (String) params.get("user");
        if (params.containsKey("password")) password = (String) params.get("password");
        if (params.containsKey("format")) format = (String) params.get("format");
        
        // 创建可复用的HTTP客户端
        httpClient = HttpClients.createDefault();
    }

    @Override
    public void write(List<Map<String, Object>> rows) {
        if (rows == null || rows.isEmpty()) return;
        
        try {
            // 1. 将数据转换为Doris支持的格式
            String payload = convertToDorisFormat(rows);
            
            // 2. 构建Stream Load请求
            String loadUrl = String.format("http://%s:%d/api/%s/%s/_stream_load", 
                                          host, port, database, table);
            
            HttpPut httpPut = new HttpPut(loadUrl);
            addRequiredHeaders(httpPut);
            
            // 3. 设置请求体
            httpPut.setEntity(new StringEntity(payload));
            
            // 4. 发送请求并处理响应
            httpClientExecuteWithRetry(httpPut);
        } catch (Exception e) {
            throw new RuntimeException("Doris Stream Load failed", e);
        }
    }

	private void httpClientExecuteWithRetry(HttpPut httpPut)
			throws IOException, ClientProtocolException, InterruptedException {
		int tmpRetryCount = 0;
		while (tmpRetryCount <= retryCount) {
		    HttpResponse response = httpClient.execute(httpPut);
		    if (handleResponse(response)) {
		        break; // 成功则退出重试循环
		    }
		    tmpRetryCount++;
		    Thread.sleep(1000 * (1 << tmpRetryCount)); // 指数退避
		}
	}

	private CsvUtils csvUtils = new CsvUtils();
    private String convertToDorisFormat(List<Map<String, Object>> rows) {
    	if(FORMAT_CSV.equals(format)){
    		return toCsvLines(rows);
    	}if(FORMAT_JSON.equals(format)) {
	        return toJsonLines(rows);
    	}else {
    		throw new RuntimeException("unsupport format:"+format+", support:csv or json");
    	}
    }

	private String toJsonLines(List<Map<String, Object>> rows) {
		StringBuilder sb = new StringBuilder();
		for (Map<String, Object> row : rows) {
		    sb.append(toJSONString(row));
		    sb.append("\n"); // 行分隔符
		}
		return sb.toString();
	}

	private String toCsvLines(List<Map<String, Object>> rows) {
		Assert.notEmpty(csvColumns,"csvColumns must be not empty");
		
		StringBuilder sb = new StringBuilder();
		for (Map<String, Object> row : rows) {
			List<Object> values = new ArrayList();
			for(String c : csvColumns) {
				Object columnValue = row.get(c);
				values.add(csvUtils.toCsvStringValue(columnValue)); 
			}
			sb.append(toCsvString(values));
		    sb.append("\n"); // 行分隔符
		}
		return sb.toString();
	}

    

	private Object toCsvString(List<Object> values) {
		return StringUtils.join(values,csvColumnSeparator);
	}

	ObjectMapper objectMapper = new ObjectMapper();
    private Object toJSONString(Map<String, Object> row) {
		try {
			return objectMapper.writeValueAsString(row);
		} catch (JsonProcessingException e) {
			throw new RuntimeException("writeValueAsString() row:"+row,e);
		}
	}

	// 添加必要的HTTP头[1,6](@ref)
    private void addRequiredHeaders(HttpPut httpPut) {
        // 认证头
        String auth = user + ":" + password;
        String encodedAuth = java.util.Base64.getEncoder().encodeToString(auth.getBytes());
        httpPut.setHeader("Authorization", "Basic " + encodedAuth);
        
        // Stream Load参数
        httpPut.setHeader("Expect", "100-continue");
        httpPut.setHeader("label", "stream_load_" + UUID.randomUUID()); // 唯一标识
        httpPut.setHeader("format", format);
        httpPut.setHeader("strip_outer_array", "true"); // JSON专用
		httpPut.setHeader("column_separator", csvColumnSeparator);    // CSV专用
        httpPut.setHeader("timeout", String.valueOf(timeoutSeconds));
        
        httpHeaders.forEach((key,value) -> {
        	httpPut.setHeader(key,value);
        });
    }

    // 处理响应并返回是否成功[1,4](@ref)
    private boolean handleResponse(HttpResponse response) throws IOException {
        int statusCode = response.getStatusLine().getStatusCode();
        HttpEntity entity = response.getEntity();
        String responseBody = EntityUtils.toString(entity);
        
        // 检查HTTP状态码
        if (statusCode != 200) {
            System.err.println("HTTP Error: " + statusCode + " - " + responseBody);
            return false;
        }
        
        // 检查Doris返回状态
        if (responseBody.contains("\"Status\":\"Success\"")) {
            System.out.println("Stream Load succeeded: " + 
                              StringUtils.substring(responseBody, 0, 100) + "...");
            return true;
        } else {
            System.err.println("Doris Import Error: " + responseBody);
            return false;
        }
    }

    @Override
    public void close() throws Exception {
        if (httpClient != null) {
            httpClient.close();
        }
    }

    @Override
    public void flush() throws IOException {
        // Stream Load实时写入，无需额外flush
    }

    // 配置setters（可在外部通过Map配置）
    public void setHost(String host) { this.host = host; }
    public void setPort(int port) { this.port = port; }
    public void setDatabase(String database) { this.database = database; }
    public void setTable(String table) { this.table = table; }
    public void setUser(String user) { this.user = user; }
    public void setPassword(String password) { this.password = password; }
    public void setFormat(String format) { this.format = format; }
    public void setBatchSize(int batchSize) { this.batchSize = batchSize; }

	public void setTimeoutSeconds(int timeoutSeconds) {
		this.timeoutSeconds = timeoutSeconds;
	}

	public void setRetryCount(int retryCount) {
		this.retryCount = retryCount;
	}
    
}