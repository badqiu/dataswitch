package com.github.dataswitch.output;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.httpclient.HttpStatus;
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
import com.github.dataswitch.BaseObject;
import com.github.dataswitch.util.CsvUtils;
import com.github.dataswitch.util.UrlUtil;
/**
 * doris http streamload导入数据
 * 支持csv,json format
 * 
 */
public class DorisStreamLoadOutput extends BaseObject implements Output,Cloneable {
	
	private static Logger log = LoggerFactory.getLogger(DorisStreamLoadOutput.class);
	
	public static final int FE_HTTP_PORT = 8030;
	public static final int FE_MYSQL_PORT = 9030;
	public static final int BE_HTTP_PORT = 8040;
	public static final int BE_DATA_PORT = 9060;
	
	public static String FORMAT_JSON = "json";
    public static String FORMAT_CSV = "csv";
    
    private static String CSV_COLUMN_SEPARATOR = "\t";
    
    // Doris 连接配置
    private String host;
    
    // Frontend: http=8030 mysql=9030  
    // Backend: http=8040 thrift_data=9060
    private int port = FE_HTTP_PORT; 
//    private int port = BE_HTTP_PORT;  //FE会重定向至BE这个端口
    
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
    
    private List<String> csvColumns = new ArrayList<String>();
    
    private boolean csvFilterUnknowChars = false;

    private String jdbcUrl;
    @Override
    public void open(Map<String, Object> params) throws Exception {
        // 创建支持重定向的HTTP客户端
        httpClient = createHttpClientWithRedirect();
        setByJdbcUrl(jdbcUrl);
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
                .setExpectContinueEnabled(false)
                .setRedirectsEnabled(true)
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
//            addHttpHeaders(httpPut);
            
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
        String errorMsg = null;
        while (tmpRetryCount <= retryCount) {
        	addHttpHeaders(httpPut);
        	
            try (CloseableHttpResponse response = httpClient.execute(httpPut)) {  // 使用CloseableHttpResponse确保资源释放
            	int statusCode = response.getStatusLine().getStatusCode();
                
                // 处理重定向响应（307）
                if (statusCode == HttpStatus.SC_TEMPORARY_REDIRECT) {
                    String newLocation = response.getFirstHeader("Location").getValue();
                    if (StringUtils.isNotBlank(newLocation)) {
                        log.info("Received 307 redirect to new location: {}",  newLocation);
                        
                        // 更新请求URL为重定向目标
                        httpPut.setURI(new URI(newLocation));
                        
                        // 重置重试计数，重定向不应计入错误重试
                        tmpRetryCount = 0; 
                        continue; // 立即重新尝试
                    }
                }
                
            	errorMsg = getResponseErrorMsg(response);
            	if (errorMsg == null) {
                    return;
                }
            } catch (Exception e) {
                log.warn("httpClientExecuteWithRetry error, retryCount:"+tmpRetryCount,e);
            }
            log.warn("httpClientExecuteWithRetry error, retryCount:"+tmpRetryCount+" errorMsg:"+errorMsg);
            tmpRetryCount++;
            long sleepTime = 1000L * (1 << tmpRetryCount); // 指数退避（1s, 2s, 4s...）
            Thread.sleep(sleepTime);
        }
        
        throw new RuntimeException(" stream load data into doris error,errorMsg:"+errorMsg);
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
        
        StringBuilder lines = new StringBuilder();
        for (Map<String, Object> row : rows) {
            List<Object> values = new ArrayList<>();
            for(String c : csvColumns) {
                Object columnValue = row.get(c);
                String csvStringValue = csvUtils.toCsvStringValue(columnValue);
                if(csvFilterUnknowChars) {
                	csvStringValue = csvStringValue.replaceAll("[\u0000-\u001F]", "");
                }
				values.add(csvStringValue); 
            }
            
            lines.append(toCsvLine(values));
            lines.append("\n"); // 行分隔符
        }
        return lines.toString();
    }

    // 引入包围符（例如双引号）和转义符
    private String enclose = "\"";
    private String escape = "\\"; // 用于转义字段中的包围符本身

    private String toCsvLine(List<Object> values) {
        // 对List中的每个值进行处理：转义包围符、包裹包围符
        List<String> processedValues = new ArrayList<>();
        for (Object value : values) {
            String strValue = String.valueOf(value);
            // 1. 转义：如果字段值中含有包围符，需要先转义（例如 " 转义为 \"）
            // 注意：此处的转义逻辑需根据你选择的包围符和转义符来确定
            String escapedValue = strValue.replace(enclose, escape + enclose);
            // 2. 包裹：用包围符将整个字段值包起来，防止其中的分隔符或换行符被误解析
            String enclosedValue = enclose + escapedValue + enclose;
            processedValues.add(enclosedValue);
        }
        // 3. 用分隔符拼接所有处理后的字段
        return StringUtils.join(processedValues, csvColumnSeparator);
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
    private void addHttpHeaders(HttpPut httpPut) {
        // 认证头（重定向时需要保留）
        String auth = user + ":" + password;
        String encodedAuth = java.util.Base64.getEncoder().encodeToString(auth.getBytes());
        httpPut.setHeader("Authorization", "Basic " + encodedAuth);
        
        // Stream Load参数
        httpPut.setHeader("label", table + "_uuid_" + UUID.randomUUID()); // 唯一标识，避免重复导入
        httpPut.setHeader("timeout", String.valueOf(timeoutSeconds));
        
        // 格式专用参数
        httpPut.setHeader("format", format);
        if (FORMAT_JSON.equals(format)) {
            httpPut.setHeader("strip_outer_array", "true"); // JSON数组处理
        } else if (FORMAT_CSV.equals(format)) {
            httpPut.setHeader("column_separator", csvColumnSeparator);    // CSV分隔符
        }
        
        // 重定向场景下移除Expect头，避免冲突
//        httpPut.removeHeaders("Expect");
        httpPut.setHeader("Expect","100-continue");
        
        // 添加自定义头
        httpHeaders.forEach((key,value) -> {
            httpPut.setHeader(key,value);
        });
    }

    // 处理响应并返回是否成功
    private String getResponseErrorMsg(HttpResponse response) throws IOException {
        int statusCode = response.getStatusLine().getStatusCode();
        HttpEntity entity = response.getEntity();
        String responseBody = entity != null ? EntityUtils.toString(entity, "UTF-8") : ""; // 显式指定编码
        
        // 检查HTTP状态码（重定向后可能是200或3xx，但最终应返回200）
        if (statusCode != 200) {
            String errorMsg = "HTTP Error: " + statusCode + " - " + responseBody;
            return errorMsg;
        }
        
        Map responseMap = objectMapper.readValue(responseBody, Map.class);
        
        // 检查Doris返回状态（JSON格式）
        boolean isResponseSuccess = "Success".equals(responseMap.get("Status"));
		if (isResponseSuccess) {
//        	log.info("Stream Load succeeded: " + StringUtils.substring(responseBody, 0, 100) + "...");
            return null;
        } else {
        	String errorMsg = "Doris Import Error: " + responseBody;
        	String errorUrl = (String)responseMap.get("ErrorURL");
        	if(StringUtils.isNotBlank(errorUrl)) {
        		try {
        			String errorContent = UrlUtil.httpGet(errorUrl);
        			errorMsg += " \n errorUrlContent:"+StringUtils.substring(errorContent, 0, 1024 * 100);
        		}catch(Exception e) {
        			log.warn("ignore get url content error,errorUrl:"+errorUrl,e);
        		}
        	}
            return errorMsg;
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
    
    public boolean isCsvFilterUnknowChars() {
		return csvFilterUnknowChars;
	}

	public void setCsvFilterUnknowChars(boolean csvFilterUnknowChars) {
		this.csvFilterUnknowChars = csvFilterUnknowChars;
	}

	public void setJdbcUrl(String jdbcUrl) {
    	setByJdbcUrl(jdbcUrl);
    }

	private void setByJdbcUrl(String jdbcUrl) {
		if (StringUtils.isBlank(jdbcUrl)) {
            return;
        }
    	
        jdbcUrl = jdbcUrl.trim();
        
        try {
            // 解析 JDBC URL 格式: jdbc:mysql://host:port/database
            Pattern pattern = Pattern.compile("jdbc:mysql://([^:/]+)(?::(\\d+))?/([^?]+)");
            Matcher matcher = pattern.matcher(jdbcUrl);
            
            if (matcher.find()) {
                // 提取主机名
                String extractedHost = matcher.group(1);
                if (StringUtils.isNotBlank(extractedHost)) {
                	if(StringUtils.isBlank(host)) {
                		this.host = extractedHost;
                	}
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
                	if(StringUtils.isBlank(database)) {
                		this.database = extractedDb;
                	}
                }
            } else {
                throw new RuntimeException("Error parsing JDBC URL: " + jdbcUrl);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error parsing JDBC URL: " + jdbcUrl, e);
        }
	}
	
	@Override
	public DorisStreamLoadOutput clone()  {
		try {
			return (DorisStreamLoadOutput)super.clone();
		} catch (CloneNotSupportedException e) {
			throw new RuntimeException(e);
		}
	}
}