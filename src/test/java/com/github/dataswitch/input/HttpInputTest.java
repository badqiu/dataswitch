package com.github.dataswitch.input;

import java.io.IOException;

import org.junit.Test;

import com.github.dataswitch.TestUtil;
import com.github.dataswitch.input.HttpInput;
import com.github.dataswitch.serializer.TxtDeserializer;

public class HttpInputTest {

	HttpInput input = new HttpInput();
	@Test
	public void test() throws Exception {
		input.setUrl("http://www.163.com","http://www.baidu.com");
		TxtDeserializer deserializer = new TxtDeserializer();
		deserializer.setColumns("username, age");
		deserializer.setColumnSeparator(",");
		input.setDeserializer(deserializer);
		TestUtil.printInputReadRows(input);
	}

}
