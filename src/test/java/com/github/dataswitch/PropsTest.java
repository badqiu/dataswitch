package com.github.dataswitch;

import static org.junit.Assert.assertEquals;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.junit.Test;

public class PropsTest {

	@Test
	public void test() throws FileNotFoundException, IOException {
		Properties props = new Properties();
		props.load(new FileReader("src/test/resources/test/test.properties"));
		System.out.println(props);
		assertEquals("{hello=hello_value, diy=123 , space_key=space_value   }",props.toString());
	}
}
