package com.github.dataswitch.serializer;

import org.apache.commons.lang.StringUtils;
import org.springframework.core.serializer.Deserializer;

public class SerDesUtil {

	public static Serializer getSerializerByFormat(String format) {
		if(StringUtils.isBlank(format)) {
			return null;
		}
		
		if("txt".equalsIgnoreCase(format)) {
			return new TxtSerializer();
		}else if("xml".equalsIgnoreCase(format)) {
			return new XmlSerializer();
		}else if("json".equalsIgnoreCase(format)) {
			return new JsonSerializer();
		}else if("byte".equalsIgnoreCase(format)) {
			return new ByteSerializer();
		}
		
		throw new RuntimeException("error format:"+format);
	}
	
	public static Deserializer getDeserializerByFormat(String format) {
		if(StringUtils.isBlank(format)) {
			return null;
		}
		
		if("txt".equalsIgnoreCase(format)) {
			return new TxtDeserializer();
		}else if("xml".equalsIgnoreCase(format)) {
			return new XmlDeserializer();
		}else if("json".equalsIgnoreCase(format)) {
			return new JsonDeserializer();
		}else if("byte".equalsIgnoreCase(format)) {
			return new ByteDeserializer();
		}
		
		throw new RuntimeException("error format:"+format);
	}
}
