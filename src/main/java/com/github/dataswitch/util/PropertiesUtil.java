package com.github.dataswitch.util;

import java.io.IOException;
import java.io.StringReader;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;

public class PropertiesUtil {

	public static Properties createProperties(String content)  {
		Properties custom = new Properties();
		if(StringUtils.isNotBlank(content)) {
			try {
				custom.load(new StringReader(content));
				custom.putAll(trim(custom));
			}catch(Exception e) {
				throw new RuntimeException("error on content:"+content,e);
			}
		}
		
		return custom;
	}

	private static Properties trim(Properties custom) {
		Properties r = new Properties();
		custom.forEach((k,v) -> {
			r.put(trim(k), trim(v));
		});
		return r;
	}

	private static String trim(Object k) {
		if(k == null) return null;
		
		return StringUtils.trim(k.toString());
	}
	
}
