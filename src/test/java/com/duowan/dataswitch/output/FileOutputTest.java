package com.duowan.dataswitch.output;

import org.junit.Test;

import com.duowan.dataswitch.TestUtil;
import com.duowan.dataswitch.serializer.TxtSerializer;

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
