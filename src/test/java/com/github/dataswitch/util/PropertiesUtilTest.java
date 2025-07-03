package com.github.dataswitch.util;

import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;

public class PropertiesUtilTest {

	@Test
	public void test() {
		Properties props = PropertiesUtil.parsePropertiesFromCmdLine(null);
		Assert.assertTrue(props.isEmpty());
		
		props = PropertiesUtil.parsePropertiesFromCmdLine("-DtaskId=some -DstartTime=\"2025-10-10 10:10:10\" -Dage=10");
		System.out.println(props);
		Assert.assertEquals("{startTime=2025-10-10 10:10:10, taskId=some, age=10}", props.toString());
	}

}
