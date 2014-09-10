package com.duowan.dataswitch.input;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.serializer.Deserializer;
import org.springframework.util.Assert;

public class HttpInput extends BaseInput implements Input{

	private static Logger log = LoggerFactory.getLogger(HttpInput.class);
	
	private List<String> urls;

	private Deserializer deserializer;
	
	private transient InputStream inputStream;
	private transient List<String> readUrls;
	private transient boolean isInit = false;
	
	public List<String> getUrls() {
		return urls;
	}

	public void setUrls(List<String> urls) {
		this.urls = urls;
	}
	
	public Deserializer getDeserializer() {
		return deserializer;
	}

	public void setDeserializer(Deserializer deserializer) {
		this.deserializer = deserializer;
	}

	public void setUrl(String... url) {
		setUrls(Arrays.asList(url));
	}
	
	public void init() {
		Assert.notNull(deserializer,"deserializer must be not null");
		readUrls = new ArrayList<String>(urls);
	}
	
	
	@Override
	public Object readObject() {
		if(!isInit) {
			isInit = true;
			init();
		}
		try {
			return read0(readUrls);
		} catch (IOException e) {
			throw new RuntimeException("read error,id:"+getId());
		}
	}
	
	private Object read0(List<String> readUrls) throws IOException {
		if(inputStream == null) {
			if(CollectionUtils.isEmpty(readUrls)) {
				return Collections.EMPTY_LIST;
			}
			
			String stringUrl = readUrls.remove(0);
			URL url = new URL(stringUrl);
			log.info("read from url:"+url);
			inputStream = new BufferedInputStream(url.openStream());
		}
		
		Object object = deserializer.deserialize(inputStream);
		if(object == null) {
			IOUtils.closeQuietly(inputStream);
			inputStream = null;
			return read0(readUrls);
		}
		return object;
	}
	
	@Override
	public void close() {
		IOUtils.closeQuietly(inputStream);
	}
}
