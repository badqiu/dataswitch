package com.github.dataswitch;

import java.io.FileNotFoundException;
import java.util.Arrays;

import javax.sql.DataSource;

import org.junit.Test;
import org.springframework.util.ResourceUtils;

import com.github.dataswitch.input.FileInput;
import com.github.dataswitch.input.JdbcInputTest;
import com.github.dataswitch.output.JdbcOutput;
import com.github.dataswitch.serializer.TxtDeserializer;
import com.github.dataswitch.util.InputOutputUtil;

public class InputOutputTest {

	@Test
	public void file2jdbc() throws FileNotFoundException {
		//输入
		FileInput input = new FileInput();
		TxtDeserializer deserializer = new TxtDeserializer();
		deserializer.setColumns("id,username,age,sex");
		deserializer.setColumnSeparator(",");
		input.setDeserializer(deserializer);
		input.setDirs(Arrays.asList(ResourceUtils.getFile("classpath:test/fileinput").getAbsolutePath()));
		
		//输出
		JdbcOutput output = new JdbcOutput();
		DataSource ds = JdbcInputTest.createDataSourceAndInsertData();
		output.setDataSource(ds);
		output.setBeforeSql("delete from user");
		output.setSql("insert into user (id,username) values(:id,:username)");
		
		//拷贝数据 input => output
		InputOutputUtil.copy(input, output);
	}
	
}
