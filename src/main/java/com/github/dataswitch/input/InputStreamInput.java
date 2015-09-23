package com.github.dataswitch.input;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.springframework.core.serializer.Deserializer;

public class InputStreamInput extends BaseInput implements Input{

	private InputStream inputStream;
	
	private Deserializer deserializer = null;

	public InputStreamInput() {
	}
	
	public InputStreamInput(InputStream inputStream, Deserializer deserializer) {
		super();
		this.inputStream = inputStream;
		this.deserializer = deserializer;
	}
	
	public void setInputStream(InputStream inputStream) {
		this.inputStream = inputStream;
	}

	public void setDeserializer(Deserializer deserializer) {
		this.deserializer = deserializer;
	}

	@Override
	public void close() throws IOException {
		IOUtils.closeQuietly(inputStream);
	}

	@Override
	public Object readObject() {
		try {
			return deserializer.deserialize(inputStream);
		} catch (IOException e) {
			throw new RuntimeException("deserialize inputStream error,inputStream:"+inputStream,e);
		}
	}
	
}
