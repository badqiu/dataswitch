package com.github.dataswitch.output;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

import com.github.dataswitch.util.xstream.JavaBeanReflectionProvider;
import com.github.dataswitch.util.xstream.SmartDurationConverter;
import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.DomDriver;

public class TimeoutOutputTest {
	long hour = 3600 * 1000;
	long minute = 60 * 1000;
	long second =  1000;
	
	TimeoutOutput output = new TimeoutOutput();
	
	@Before
	public void before() {
		output.setAuthor("author_helloworld");
	}
	
	@Test
	public void setTimeout() {

		
		output.setTimeout("3s");
		assertEquals(3 * second,output.getTimeout());
		
		output.setTimeout("2m");
		assertEquals(120 * second,output.getTimeout());
		
		output.setTimeout("2h");
		assertEquals(2 * hour,output.getTimeout());
		
		output.setTimeout("90m");
		assertEquals(90 * minute,output.getTimeout());
		
		output.setTimeout("90m30s");
		assertEquals(90 * minute + 30 * second,output.getTimeout());
		
		output.setTimeout("5h30m30s");
		assertEquals(5 * hour + 30 * minute + 30 * second,output.getTimeout());
		
		output.setTimeout("2d5h30m30s");
		assertEquals(5 * hour + 30 * minute + 30 * second,output.getTimeout());
	}
	
	@Test
	public void setTimeoutFromXml() {
		XStream xstream = new XStream(new JavaBeanReflectionProvider(),new DomDriver());
		xstream.registerConverter(new SmartDurationConverter(),XStream.PRIORITY_VERY_HIGH);
		
		output = (TimeoutOutput)xstream.fromXML("<com.github.dataswitch.output.TimeoutOutput><timeout>2100</timeout></com.github.dataswitch.output.TimeoutOutput>");
		assertEquals(2100,output.getTimeout());
		
		output = (TimeoutOutput)xstream.fromXML("<com.github.dataswitch.output.TimeoutOutput><timeout>888</timeout></com.github.dataswitch.output.TimeoutOutput>");
		assertEquals(888,output.getTimeout());
		
		
		output = (TimeoutOutput)xstream.fromXML("<com.github.dataswitch.output.TimeoutOutput><timeout>30m20s</timeout></com.github.dataswitch.output.TimeoutOutput>");
		assertEquals(30 * minute + 20 * second,output.getTimeout());
		
		output = (TimeoutOutput)xstream.fromXML("<com.github.dataswitch.output.TimeoutOutput><timeout><null/></timeout></com.github.dataswitch.output.TimeoutOutput>");
		assertEquals(0,output.getTimeout());
		
		output = (TimeoutOutput)xstream.fromXML("<com.github.dataswitch.output.TimeoutOutput><timeoutSecond>10</timeoutSecond></com.github.dataswitch.output.TimeoutOutput>");
		assertEquals(10 * second,output.getTimeout());
		
		System.out.println(xstream.toXML(output));
	}

}
