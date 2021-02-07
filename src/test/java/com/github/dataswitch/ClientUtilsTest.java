package com.github.dataswitch;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.ClientUtils;
import org.junit.Test;

public class ClientUtilsTest {

	@Test
	public void test() {
		List<String> urls = new ArrayList<String>();
		urls.add("kafka4_in.lzfm.com:9092");
		
		ClientUtils.parseAndValidateAddresses(urls);
	}
}
