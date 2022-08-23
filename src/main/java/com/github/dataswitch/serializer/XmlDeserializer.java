package com.github.dataswitch.serializer;

import java.io.IOException;
import java.io.InputStream;

import org.springframework.core.serializer.Deserializer;

import com.github.dataswitch.BaseObject;

public class XmlDeserializer extends BaseObject implements Deserializer{

	@Override
	public Object deserialize(InputStream inputStream) throws IOException {
		return null;
	}

}
