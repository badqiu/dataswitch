package com.github.dataswitch.output;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.ProtocolException;
import org.apache.http.client.RedirectStrategy;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dataswitch.enums.Constants;
import com.github.dataswitch.util.CsvUtils;

public class DorisStreamLoadOutput implements Output {
	
	private static Logger log = LoggerFactory.getLogger(DorisStreamLoadOutput.class);
	
	public static String FORMAT_JSON = "json";
    public static String FORMAT_CSV = "csv";
    
    private static String CSV_COLUMN_SEPARATOR = Constants.COLUMN_SPLIT;
    
    // Doris 连接配置
    private String host;
    private int port = 8030;
    private String database;
    private String table;
    private String user;
    private String password;
    private String format = FORMAT_JSON;  // 支持json/csv
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
        
        // 创建支持重定向的HTTP客户端
        httpClient = createHttpClientWithRedirect();
    }

    /**
     * 创建支持重定向的HTTP客户端，并确保重定向时携带认证头
     */
    private CloseableHttpClient createHttpClientWithRedirect() {
        // 自定义重定向策略：保留所有请求头（尤其是Authorization）
        RedirectStrategy customRedirectStrategy = new DefaultRedirectStrategy() {
            @Override
            public boolean isRedirected(HttpRequest request, HttpResponse response, HttpContext context) {
                try {
					return super.isRedirected(request, response, context);
				} catch (ProtocolException e) {
					throw new RuntimeException(e);
				}
            }

            @Override
            public HttpUriRequest getRedirect(HttpRequest request, HttpResponse response, HttpContext context) throws ProtocolException {
                HttpUriRequest redirect = super.getRedirect(request, response, context);
                // 复制原始请求的所有头信息到重定向请求
                redirect.setHeaders(request.getAllHeaders());
                return redirect;
            }
        };

        // 配置请求超时
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(timeoutSeconds * 1000)
                .setConnectionRequestTimeout(timeoutSeconds * 1000)
                .setSocketTimeout(timeoutSeconds * 1000)
                .build();

        return HttpClientBuilder.create()
                .setRedirectStrategy(customRedirectStrategy)  // 启用自定义重定向策略
                .setDefaultRequestConfig(requestConfig)
                .build();
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
            httpPut.setEntity(new StringEntity(payload, "UTF-8"));  // 显式指定编码
            
            // 4. 发送请求并处理响应
            httpClientExecuteWithRetry(httpPut);
        } catch (Exception e) {
            throw new RuntimeException("Doris Stream Load failed", e);
        }
    }

    private void httpClientExecuteWithRetry(HttpPut httpPut)
            throws IOException, InterruptedException {
        int tmpRetryCount = 0;
        while (tmpRetryCount <= retryCount) {
            try (CloseableHttpResponse response = httpClient.execute(httpPut)) {  // 使用CloseableHttpResponse确保资源释放
                if (handleResponse(response)) {
                    break; // 成功则退出重试循环
                }
            } catch (Exception e) {
                log.warn("httpClientExecuteWithRetry error, retry:"+tmpRetryCount,e);
            }
            tmpRetryCount++;
            long sleepTime = 1000L * (1 << tmpRetryCount); // 指数退避（1s, 2s, 4s...）
            Thread.sleep(sleepTime);
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
            List<Object> values = new ArrayList<>();
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

    // 添加必要的HTTP头
    private void addRequiredHeaders(HttpPut httpPut) {
        // 认证头（重定向时需要保留）
        String auth = user + ":" + password;
        String encodedAuth = java.util.Base64.getEncoder().encodeToString(auth.getBytes());
        httpPut.setHeader("Authorization", "Basic " + encodedAuth);
        
        // Stream Load参数
        httpPut.setHeader("label", "stream_load_" + UUID.randomUUID()); // 唯一标识，避免重复导入
        httpPut.setHeader("format", format);
        httpPut.setHeader("timeout", String.valueOf(timeoutSeconds));
        
        // 格式专用参数
        if (FORMAT_JSON.equals(format)) {
            httpPut.setHeader("strip_outer_array", "true"); // JSON数组处理
        } else if (FORMAT_CSV.equals(format)) {
            httpPut.setHeader("column_separator", csvColumnSeparator);    // CSV分隔符
        }
        
        // 重定向场景下移除Expect头，避免冲突
        httpPut.removeHeaders("Expect");
        
        // 添加自定义头
        httpHeaders.forEach((key,value) -> {
            httpPut.setHeader(key,value);
        });
    }

    // 处理响应并返回是否成功
    private boolean handleResponse(HttpResponse response) throws IOException {
        int statusCode = response.getStatusLine().getStatusCode();
        HttpEntity entity = response.getEntity();
        String responseBody = entity != null ? EntityUtils.toString(entity, "UTF-8") : ""; // 显式指定编码
        
        // 检查HTTP状态码（重定向后可能是200或3xx，但最终应返回200）
        if (statusCode != 200) {
            log.warn("HTTP Error: " + statusCode + " - " + responseBody);
            return false;
        }
        
        // 检查Doris返回状态（JSON格式）
        if (responseBody.contains("\"Status\":\"Success\"")) {
        	log.info("Stream Load succeeded: " + 
                              StringUtils.substring(responseBody, 0, 100) + "...");
            return true;
        } else {
        	log.warn("Doris Import Error: " + responseBody);
            return false;
        }
    }

    @Override
    public void close() throws Exception {
        if (httpClient != null) {
            httpClient.close(); // 关闭HTTP客户端，释放资源
        }
    }

    @Override
    public void flush() throws IOException {
        // Stream Load实时写入，无需额外flush
    }

    // 配置setters
    public void setHost(String host) { this.host = host; }
    public void setPort(int port) { this.port = port; }
    public void setDatabase(String database) { this.database = database; }
    public void setTable(String table) { this.table = table; }
    public void setUser(String user) { this.user = user; }
    public void setPassword(String password) { this.password = password; }
    public void setFormat(String format) { this.format = format; }
    public void setBatchSize(int batchSize) { this.batchSize = batchSize; }
    public void setTimeoutSeconds(int timeoutSeconds) { this.timeoutSeconds = timeoutSeconds; }
    public void setRetryCount(int retryCount) { this.retryCount = retryCount; }
    public void setCsvColumns(List<String> csvColumns) { this.csvColumns = csvColumns; }
    public void setCsvColumnSeparator(String csvColumnSeparator) { this.csvColumnSeparator = csvColumnSeparator; }
    public void setHttpHeaders(Map<String, String> httpHeaders) { this.httpHeaders = httpHeaders; }
    
    public void setJdbcUrl(String jdbcUrl) {
    	if (StringUtils.isBlank(jdbcUrl)) {
            return;
        }
        
        try {
            // 解析 JDBC URL 格式: jdbc:mysql://host:port/database
            Pattern pattern = Pattern.compile("jdbc:mysql://([^:/]+)(?::(\\d+))?/([^?]+)");
            Matcher matcher = pattern.matcher(jdbcUrl);
            
            if (matcher.find()) {
                // 提取主机名
                String extractedHost = matcher.group(1);
                if (StringUtils.isNotBlank(extractedHost)) {
                    this.host = extractedHost;
                }
                
                // 提取端口（如果存在）
                String portStr = matcher.group(2);
                if (StringUtils.isNotBlank(portStr)) {
//                    try {
//                        int extractedPort = Integer.parseInt(portStr);
//                        if (extractedPort > 0 && extractedPort <= 65535) {
//                            this.port = extractedPort;
//                        }
//                    } catch (NumberFormatException e) {
//                    	throw new RuntimeException("Invalid port in JDBC URL:"+jdbcUrl+" port:"+portStr);
//                    }
                }
                
                // 提取数据库名
                String extractedDb = matcher.group(3);
                if (StringUtils.isNotBlank(extractedDb)) {
                    this.database = extractedDb;
                }
            } else {
                throw new RuntimeException("Error parsing JDBC URL: " + jdbcUrl);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error parsing JDBC URL: " + jdbcUrl, e);
        }
    }
}