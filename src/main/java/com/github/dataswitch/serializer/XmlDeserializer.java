package com.github.dataswitch.serializer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.WeakHashMap;

import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.springframework.core.serializer.Deserializer;

import com.github.dataswitch.BaseObject;
import com.thoughtworks.xstream.XStream;

public class XmlDeserializer extends BaseObject implements Deserializer{
	static XStream xstream = new XStream();
	

	private Map<InputStream,BufferedReader> readers = new WeakHashMap<InputStream,BufferedReader>();
	
//	@Override
//	public Object deserialize(InputStream inputStream) throws IOException {
//		Object result = xstream.fromXML(inputStream);
//		return result;
//	}
	
	@Override
	public Map deserialize(InputStream inputStream) throws IOException {
		BufferedReader reader = toBufferedReader(inputStream);
		String line = null;
		while((line = reader.readLine()) != null) {
			if(StringUtils.isBlank(line)) {
				continue;
			}
			return (Map)deserializeLine(line);
		}
		return null;
	}

	protected Object deserializeLine(String xml) throws IOException, JsonParseException, JsonMappingException {
		return xstream.fromXML(xml);
	}

	private BufferedReader toBufferedReader(InputStream inputStream) {
		BufferedReader reader = readers.get(inputStream);
		if(reader == null) {
			reader = new BufferedReader(new InputStreamReader(inputStream));
			readers.put(inputStream, reader);
		}
		return reader;
	}
	
}
