package com.github.dataswitch.output;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dataswitch.util.KafkaConfigUtil;
import com.github.dataswitch.util.PropertiesUtil;
import com.github.dataswitch.util.TableName;


public class KafkaOutput implements Output,TableName{

	protected static Logger logger = LoggerFactory.getLogger(KafkaOutput.class);
	
	private static ObjectMapper objectMapper = new ObjectMapper();
	
	private Properties properties;
	private String topic;

	private KafkaProducer<Object,Object> kafkaProducer;

	private String propertiesString;
	
	public Properties getProperties() {
		return properties;
	}

	public void setProperties(Properties outputKafka) {
		this.properties = outputKafka;
	}
	
	public String getPropertiesString() {
		return propertiesString;
	}

	public void setPropertiesString(String propertiesString) {
		this.propertiesString = propertiesString;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String outputTopic) {
		this.topic = outputTopic;
	}
	
	public void setTable(String table) {
		setTopic(table);
	}
	
	@Override
	public String getTable() {
		return getTopic();
	}

	public KafkaProducer<Object, Object> buildKafkaProducer(Properties properties) {
		KafkaConfigUtil.initJavaSecurityAuthLoginConfig(properties);
		
		Properties props = new Properties();
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.putAll(properties);
		props.putAll(PropertiesUtil.createProperties(propertiesString));
		
		logger.info("buildKafkaProducer() outputTopic:" + topic + " properties:"+props);
		KafkaProducer<Object,Object> kafkaProducer = new KafkaProducer<Object,Object>(props);
		return kafkaProducer;
	}
	
	@Override
	public void close() throws IOException {
		IOUtils.closeQuietly(kafkaProducer);
	}
	
	@Override
	public void open(Map<String, Object> params) throws Exception {
		Output.super.open(params);
		init();
	}

	private void init() {
		if(kafkaProducer == null) {
			synchronized (this) {
				if(kafkaProducer == null) {
					kafkaProducer = buildKafkaProducer(properties);
				}
			}
		}
	}
	
	@Override
	public void write(List<Object> rows) {
		if(CollectionUtils.isEmpty(rows)) {
			return;
		}
		
		for(Object row : rows) {
			Callback callback = new Callback() {
				@Override
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					if(exception != null) {
						logger.warn("send kafka msg error:"+exception+" data:" + row,exception);
					}
				} 
			};
			kafkaProducer.send(new ProducerRecord<Object, Object>(topic, row), callback );
		}
	}
	
	@Override
	public void flush() throws IOException {
		kafkaProducer.flush();
	}

}
