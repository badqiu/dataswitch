package com.github.dataswitch.input;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.eclipse.jetty.util.ajax.JSON;
import org.junit.Test;

public class KafkaInputTest {

	@Test
	public void test() throws Exception {
		KafkaInput input = new KafkaInput() {
			@Override
			protected List<Map<String, Object>> processOne(ConsumerRecord<Object, Object> c) {
				String str = (String)c.value();
				return (List)JSON.parse(str);
			}
		};
		input.setTopic("data_collect.http_request_info_log");
		input.setProperties(new Properties());
		input.setPropertiesString("group.id = modo-cloud-dw-etl\n"
				+ "bootstrap.servers = 120.78.88.6:9092");
		input.open(null);
		
		while(true) {
			List<Map> rows = (List)input.read(100);
			for(Map row : rows) {
				System.out.println("read_row:" + JSON.toString(row));
			}
		}
	}

}
