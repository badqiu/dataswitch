package com.github.dataswitch.serializer;

import java.io.IOException;
import java.io.OutputStream;

import com.github.dataswitch.BaseObject;

public class XmlSerializer extends BaseObject implements Serializer{

	@Override
	public void serialize(Object object, OutputStream outputStream) throws IOException {
		
	}

	@Override
	public void flush() throws IOException {
	}

}
