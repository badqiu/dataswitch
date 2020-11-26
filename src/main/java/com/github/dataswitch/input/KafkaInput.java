package com.github.dataswitch.input;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaInput implements Input{

	protected static Logger logger = LoggerFactory.getLogger(KafkaInput.class);
	
	
	private volatile boolean running = true;
	
	private Properties properties;
	private String topic;
	private boolean sync = false;
	
	private List<ConsumerWorker> kafkaConsumerThreads = new ArrayList<ConsumerWorker>();
	private LinkedBlockingQueue<Object> queue = new LinkedBlockingQueue(100);
	
	public Properties getProperties() {
		return properties;
	}

	public void setProperties(Properties inputKafka) {
		this.properties = inputKafka;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String inputTopic) {
		this.topic = inputTopic;
	}
	
	public KafkaConsumer buildKafkaConsumer(Properties kafkaProperties) {
		Properties properties = new Properties();
		properties.put("client.id", KafkaInput.class.getSimpleName());
		properties.put("acks", "1");
		properties.put("retries", "1");
		properties.put("compression.type", "snappy");
		properties.put("enable.auto.commit", false);
		
		properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		
		properties.putAll(kafkaProperties);
	        
		KafkaConsumer<Object, Object> kafkaConsumer = null;
		try {
			kafkaConsumer = new KafkaConsumer<Object, Object>(properties);
			logger.info("buildKafkaConsumer() properties:"+properties);
			
			kafkaConsumer.listTopics();
//			kafkaConsumer.subscribe(Arrays.asList(topic));
			
			return kafkaConsumer;
		}catch(Exception e) {
			throw new RuntimeException("buildKafkaConsumer error,msg:"+e,e);
		}
	}
	
	public void startConsumerKafkaData() {
		
		KafkaConsumer<String,String> kafkaConsumer = buildKafkaConsumer(properties);
		List<PartitionInfo> partitions = kafkaConsumer.partitionsFor(topic);
		for(PartitionInfo p : partitions) {
			kafkaConsumer = buildKafkaConsumer(properties);
			TopicPartition topicPartition= new TopicPartition(p.topic(),p.partition());
			kafkaConsumer.assign(Arrays.asList(topicPartition));
			ConsumerWorker worker = startConsumerThread(kafkaConsumer);
			kafkaConsumerThreads.add(worker);
		}
		
	}

	private ConsumerWorker startConsumerThread(KafkaConsumer<String, String> kafkaConsumer) {
		ConsumerWorker task = new ConsumerWorker();
		task.kafkaConsumer = kafkaConsumer;
		
		Thread thread = new Thread(task,"kafka_consumer_"+topic);
		thread.start();
		return task;
	}


//	private AtomicLong count = new AtomicLong();
	public class ConsumerWorker implements Runnable{
		private KafkaConsumer<String, String> kafkaConsumer;
		
		public void run() {
			try {
				logger.info("consumer thread start, work on partitions:" + kafkaConsumer.assignment());
				
				while(running) {
					try {
						ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(3));
						if(records == null || records.isEmpty()) {
							continue;
						}
						
						for(ConsumerRecord<String,String> c : records) {
							String line = c.value();
							queue.offer(line);
//							count.incrementAndGet();
						}
						
						kafkaConsumer.commitSync();
					}catch(Exception e) {
						logger.error("consumer error",e);
					}
				}
			}finally {
				logger.info("consumer thread exit");
			}
		}
		
		public void close() {
			kafkaConsumer.close();
		}

	}

	@Override
	public void close() throws IOException {
		running = false;
		
		for(ConsumerWorker w : kafkaConsumerThreads) {
			w.close();
		}
	}

	@Override
	public List<Object> read(int size) {
		initIfNeed();
		
		try {
			Object object = queue.take();
			if(object == null) return Collections.EMPTY_LIST;
			
			
			return Arrays.asList(object);
		}catch(InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
	
	private boolean init = false;
	private void initIfNeed() {
		if(init) return;
		
		synchronized (this) {
			if(init) return;
			init = true;
			
			startConsumerKafkaData();
		}
	}



}
