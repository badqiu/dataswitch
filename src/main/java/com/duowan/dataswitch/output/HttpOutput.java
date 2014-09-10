package com.duowan.dataswitch.output;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLConnection;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import com.duowan.dataswitch.serializer.Serializer;

public class HttpOutput extends BaseOutput implements Output {

	private static Logger log = LoggerFactory.getLogger(HttpOutput.class);
	
	private String url;
	
	private transient  Serializer serializer  = null;
	private transient  boolean isInit = false;

	private OutputStream outputStream;
	
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

	private void init() throws Exception {
		log.info("write to url:"+url);
		
		Assert.hasText(url,"url must be not empty");
		Assert.notNull(serializer,"serializer must be not null");
		URL url = new URL(this.url);
		
        // 打开和URL之间的连接
        URLConnection conn = url.openConnection();
        // 设置通用的请求属性
        conn.setRequestProperty("accept", "*/*");
        conn.setRequestProperty("connection", "Keep-Alive");
        conn.setRequestProperty("user-agent", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)");

        // 发送POST请求必须设置如下两行
        conn.setDoOutput(true);
        conn.setDoInput(true);
        outputStream = new BufferedOutputStream(conn.getOutputStream());
	}

	@Override
	public void close() {
		try {
			serializer.flush();
		} catch (IOException e) {
			throw new RuntimeException("flush error",e);
		}
		IOUtils.closeQuietly(outputStream);
	}

	@Override
	public void writeObject(Object object) {
		try {
			if(!isInit) {
				isInit = true;
				init();
			}
			serializer.serialize(object, outputStream);
		}catch(Exception e) {
			throw new RuntimeException("write error,id:"+getId(),e);
		}
	}
	
}
