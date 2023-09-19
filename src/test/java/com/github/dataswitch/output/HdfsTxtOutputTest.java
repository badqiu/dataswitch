package com.github.dataswitch.output;

import java.util.Map;

import org.junit.Test;

import com.github.dataswitch.util.MapUtil;


public class HdfsTxtOutputTest {

	@Test
	public void test() throws Exception {
		Map row = MapUtil.newMap("username","badqiu","age",20,"height",185);
		HdfsTxtOutput output = new HdfsTxtOutput();
		output.setColumns("username,age");
		output.setColumnSeparator(",");
		output.setDir("/tmp/hdfs_txt_output_test/test");
		output.setHdfsUri("hdfs://localhost:54310");
		output.open(null);
		
		output.writeObject(row);
		output.close();
	}

}
