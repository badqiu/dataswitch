package com.github.dataswitch.serializer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.junit.Test;
import org.springframework.util.ResourceUtils;

public class ExcelDeserializerTest {

	@Test
	public void test() throws FileNotFoundException, IOException {
		File file = ResourceUtils.getFile("classpath:test/excel_input/demo_dser.xls");
		ExcelDeserializer d = new ExcelDeserializer();
		d.setSkipLines(1);
		d.setColumns("name,sex,age,classis");
		Object result = d.deserialize(new FileInputStream(file));
		System.out.println("result:"+result);
	}

}
