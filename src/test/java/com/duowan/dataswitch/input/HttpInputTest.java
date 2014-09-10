package com.duowan.dataswitch.input;

import java.io.IOException;

import org.junit.Test;

import com.duowan.dataswitch.TestUtil;
import com.duowan.dataswitch.serializer.TxtDeserializer;

public class HttpInputTest {

	HttpInput input = new HttpInput();
	@Test
	public void test() throws IOException {
		input.setUrl("http://www.163.com","http://www.baidu.com");
		TxtDeserializer deserializer = new TxtDeserializer();
		deserializer.setColumns("username, age");
		deserializer.setColumnSplit(",");
		input.setDeserializer(deserializer);
		TestUtil.printInputReadRows(input);
	}

}
