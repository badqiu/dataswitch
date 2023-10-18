package com.github.dataswitch.output;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

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

	private int retryTimes = 0;
	
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
	
	public int getRetryTimes() {
		return retryTimes;
	}

	public void setRetryTimes(int retryTimes) {
		this.retryTimes = retryTimes;
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
	public void write(List<Map<String, Object>> rows) {
		if(CollectionUtils.isEmpty(rows)) {
			return;
		}
		
		for(Object row : rows) {
			producerSendOne(row);
		}
	}

	protected void producerSendOne(Object row) {
		Callback callback = new Callback() {
			@Override
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				if(exception != null) {
					logger.warn("send kafka msg error:"+exception+" data:" + row,exception);
				}
			} 
		};
		
		Future<RecordMetadata> future = kafkaProducer.send(new ProducerRecord<Object, Object>(topic, row), callback );
//		sendMessageWithRetry(kafkaProducer,topic,row,retryTimes);
	}
	
	public void sendMessageWithRetry(KafkaProducer<Object, Object> kafkaProducer, String topic, Object row, int retryTimes) {
	    int retryCount = 0;
	    Exception lastException = null;

	    while(true) {
	        try {
	            Callback callback = new Callback() {
	                @Override
	                public void onCompletion(RecordMetadata metadata, Exception exception) {
	                    if (exception != null) {
	                        logger.warn("send kafka msg error:" + exception + " data:" + row, exception);
	                    }
	                }
	            };
	            kafkaProducer.send(new ProducerRecord<Object, Object>(topic, row), callback).get();
	            return; // 发送成功，退出重试逻辑
	        } catch (Exception e) {
	            retryCount++;
	            lastException = e;
	            logger.warn("Kafka message send failed, retry count: " + retryCount, e);
	            
	            if(retryCount > retryTimes) {
					break;
				}
	        }
	    }

	    // 重试次数达到上限，抛出最后一次异常
	    throw new RuntimeException("Kafka message send failed after " + retryTimes + " retries", lastException);
	}
	
	
	@Override
	public void flush() throws IOException {
		kafkaProducer.flush();
	}

}
