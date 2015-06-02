package com.github.dataswitch.output;

import java.util.Map;

import org.junit.Test;

import com.github.rapid.common.util.MapUtil;


public class HdfsTxtOutputTest {

	@Test
	public void test() {
		Map row = MapUtil.newMap("username","badqiu","age",20,"height",185);
		HdfsTxtOutput output = new HdfsTxtOutput();
		output.setColumns("username,age");
		output.setColumnSplit(",");
		output.setDir("/tmp/hdfs_txt_output_test/test");
		output.setHdfsUri("hdfs://localhost:54310");
		output.writeObject(row);
		output.close();
	}

}
