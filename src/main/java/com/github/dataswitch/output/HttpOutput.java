package com.github.dataswitch.output;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.ProtocolException;
import java.net.URL;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import com.github.dataswitch.serializer.JsonSerializer;
import com.github.dataswitch.serializer.Serializer;

public class HttpOutput extends BaseOutput implements Output {

	private static Logger log = LoggerFactory.getLogger(HttpOutput.class);
	
	private String url;
	
	private Serializer serializer  = new JsonSerializer();

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}
	
	public Serializer getSerializer() {
		return serializer;
	}

	public void setSerializer(Serializer serializer) {
		this.serializer = serializer;
	}
	
	@Override
	public void open(Map<String, Object> params) throws Exception {
		super.open(params);
		init();
	}

	public void init() throws Exception {
		log.info("write to url:"+url);
		
		Assert.hasText(url,"url must be not empty");
		Assert.notNull(serializer,"serializer must be not null");
	}

	private HttpURLConnection openConnectionByConfig(URL url) throws IOException, ProtocolException {
		// 打开和URL之间的连接
		HttpURLConnection conn = (HttpURLConnection)url.openConnection();
        // 设置通用的请求属性
        
        // Post cannot use caches  
        // Post 请求不能使用缓存  
        conn.setUseCaches(false);
        // 发送POST请求必须设置如下两行
        conn.setDoOutput(true);
        conn.setDoInput(true);
        
        conn.setRequestProperty("accept", "*/*");
        conn.setRequestProperty("connection", "Keep-Alive");
        conn.setRequestProperty("user-agent", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)");
        conn.setRequestMethod("POST");  
        conn.setRequestProperty( "Content-Type", "application/json");
		return conn;
	}

	@Override
	public void close() throws IOException {
	}

	@Override
	public void writeObject(Object object) {
		try {
			URL url = new URL(this.url);
	        HttpURLConnection conn = openConnectionByConfig(url); 
	        
	        OutputStream outputStream = new BufferedOutputStream(conn.getOutputStream());
			serializer.serialize(object, outputStream);
			serializer.flush();
			outputStream.flush();
			
			int responseCode = conn.getResponseCode();
			validateResponseCode(responseCode,conn);
			
//			log.info("responseCode:"+responseCode+" response::"+response);
			
			IOUtils.closeQuietly(outputStream);
			conn.disconnect();
		}catch(Exception e) {
			throw new RuntimeException("write error,id:"+getId()+" url:"+url+" serializer:"+serializer,e);
		}
	}

	private void validateResponseCode(int responseCode,HttpURLConnection conn) throws IOException {
		if(responseCode >= 200 && responseCode <300) {
			return;
		}
		
		String response = IOUtils.toString(conn.getInputStream());
		throw new RuntimeException("error response code:" + responseCode+", response:"+response);
	}
	
}
