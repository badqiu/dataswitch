package com.github.dataswitch.output;

import static org.junit.Assert.assertEquals;

import java.util.Date;
import java.util.Map;

import org.junit.Test;

import com.github.dataswitch.util.MapUtil;

public class DorisStreamLoadOutputTest {

	@Test
	public void test() {
		String seperator = "\001";
		System.out.println(seperator);
		System.out.println(DorisStreamLoadOutput.escapeInvisibleChars(seperator));
		assertEquals("\\x01",seperator);
	}
	
	@Test
	public void json() {
		Map map = MapUtil.newMap("now",new Date());
		
		System.out.println(DorisStreamLoadOutput.toJSONString(map)); 
	}

}
