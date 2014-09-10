package com.duowan.dataswitch.input;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.junit.Test;
import org.springframework.util.ResourceUtils;

import com.duowan.dataswitch.TestUtil;
import com.duowan.dataswitch.serializer.TxtDeserializer;

public class FileInputTest {

	FileInput input = new FileInput();
	@Test
	public void test() throws Exception {
		TxtDeserializer deserializer = new TxtDeserializer();
		deserializer.setColumns("name,age , sex");
		deserializer.setColumnSplit(",");
		input.setDeserializer(deserializer);
		input.setDirs(Arrays.asList(ResourceUtils.getFile("classpath:test/fileinput").getAbsolutePath()));
		int count = TestUtil.printInputReadRows(input);
		assertEquals(count,106);
	}



}
