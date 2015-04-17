package com.github.dataswitch.output;

import org.junit.Test;

import com.github.dataswitch.TestUtil;
import com.github.dataswitch.output.FileOutput;
import com.github.dataswitch.serializer.TxtSerializer;

public class FileOutputTest {

	@Test
	public void test() {
		TxtSerializer serializer = new TxtSerializer();
		FileOutput fo = new FileOutput();
		fo.setDir("/tmp/fileout");
		serializer.setColumns("username, age");
		fo.setSerializer(serializer);
		fo.setCompressType("gzip");
		fo.write(TestUtil.newTestDatas(100));
		fo.close();
	}

}
