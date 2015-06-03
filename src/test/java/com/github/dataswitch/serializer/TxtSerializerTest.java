package com.github.dataswitch.serializer;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Date;
import java.util.Map;

import org.junit.Test;

import com.github.rapid.common.util.DateConvertUtil;
import com.github.rapid.common.util.MapUtil;

public class TxtSerializerTest {
	TxtSerializer ser = new TxtSerializer();

	@Test
	public void test() throws IOException {
		ser.setColumns("name,birth_date");
		ser.setColumnSplit(",");
		
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		Map map = MapUtil.newMap("name","badqiu","sex","f","birth_date",DateConvertUtil.parse("1998-8-1 10:12:13", "yyyy-MM-dd HH:mm:ss"));
		ser.serialize(map, out);
		ser.flush();
		
		System.out.println(out.toString());
		assertEquals("badqiu,1998-08-01 10:12:13",out.toString());
	}

}
