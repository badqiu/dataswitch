package com.github.dataswitch.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.util.InvalidPropertiesFormatException;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;

public class PropertiesUtil {

	public static Properties createProperties(String content)  {
		Properties custom = new Properties();
		if(StringUtils.isNotBlank(content)) {
			try {
				smartLoadFromString(content, custom);
				return trim(custom);
			}catch(Exception e) {
				throw new RuntimeException("error on content:"+content,e);
			}
		}
		
		return custom;
	}

	private static void smartLoadFromString(String content, Properties custom) throws IOException, InvalidPropertiesFormatException {
		if(content.contains("<properties>") && content.contains("</properties>")) {
			custom.loadFromXML(new ByteArrayInputStream(content.getBytes()));
		}else {
			custom.load(new StringReader(content));
		}
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
