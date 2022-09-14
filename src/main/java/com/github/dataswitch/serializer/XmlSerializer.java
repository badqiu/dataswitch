package com.github.dataswitch.serializer;

import java.io.IOException;
import java.io.OutputStream;

import com.github.dataswitch.BaseObject;
import com.thoughtworks.xstream.XStream;

public class XmlSerializer extends BaseObject implements Serializer{

	static XStream xstream = new XStream();
	
	@Override
	public void serialize(Object object, OutputStream outputStream) throws IOException {
		if(object == null) return;
		
		xstream.toXML(object, outputStream);
		outputStream.write("\n".getBytes());
		
	}

	@Override
	public void flush() throws IOException {
	}

}
