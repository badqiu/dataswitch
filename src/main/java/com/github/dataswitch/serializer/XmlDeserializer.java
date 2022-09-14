package com.github.dataswitch.serializer;

import java.io.IOException;
import java.io.InputStream;

import org.springframework.core.serializer.Deserializer;

import com.github.dataswitch.BaseObject;
import com.thoughtworks.xstream.XStream;

public class XmlDeserializer extends BaseObject implements Deserializer{
	static XStream xstream = new XStream();
	
	@Override
	public Object deserialize(InputStream inputStream) throws IOException {
		Object result = xstream.fromXML(inputStream);
		return result;
	}

}
