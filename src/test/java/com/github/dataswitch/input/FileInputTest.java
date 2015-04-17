package com.github.dataswitch.input;

import static org.junit.Assert.assertEquals;

import java.io.FileNotFoundException;
import java.util.Arrays;

import org.junit.Test;
import org.springframework.util.ResourceUtils;

import com.github.dataswitch.TestUtil;
import com.github.dataswitch.input.FileInput;
import com.github.dataswitch.serializer.TxtDeserializer;

public class FileInputTest {

	
	@Test
	public void test() throws Exception {
		FileInput input = newFileInput("name,age , sex","classpath:test/fileinput");
		
		int count = TestUtil.printInputReadRows(input);
		assertEquals(count,106);
	}

	public static FileInput newFileInput(String columns,String file) throws FileNotFoundException {
		FileInput input = new FileInput();
		TxtDeserializer deserializer = new TxtDeserializer();
		deserializer.setColumns(columns);
		deserializer.setColumnSplit(",");
		input.setDeserializer(deserializer);
		input.setDirs(Arrays.asList(ResourceUtils.getFile(file).getAbsolutePath()));
		return input;
	}



}
