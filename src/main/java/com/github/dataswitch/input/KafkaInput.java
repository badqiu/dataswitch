package com.github.dataswitch.input;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dataswitch.util.BlockingQueueUtil;
import com.github.dataswitch.util.KafkaConfigUtil;
import com.github.dataswitch.util.PropertiesUtil;


public class KafkaInput implements Input{

	protected static Logger logger = LoggerFactory.getLogger(KafkaInput.class);
	
	private volatile boolean running = true;
	
	private Properties properties;
	private String propertiesString;
	private String topic;
	
	/** 是否同步消费数据，同步单线程，异步多线程 */
	private boolean sync = false;
	
	private transient List<ConsumerWorker> kafkaConsumerThreads = new ArrayList<ConsumerWorker>();
	private transient LinkedBlockingQueue<Object> queue = new LinkedBlockingQueue<Object>(10000);
	private transient KafkaConsumer<Object,Object> kafkaConsumer = null;
	
	
	public Properties getProperties() {
		return properties;
	}

	public void setProperties(Properties inputKafka) {
		this.properties = inputKafka;
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

	public void setTopic(String inputTopic) {
		this.topic = inputTopic;
	}
	
	public KafkaConsumer buildKafkaConsumer(Properties kafkaProperties) {
		KafkaConfigUtil.initJavaSecurityAuthLoginConfig(kafkaProperties);
		
		Properties properties = new Properties();
		properties.put("client.id", KafkaInput.class.getSimpleName());
		properties.put("acks", "1");
		properties.put("retries", "1");
		properties.put("compression.type", "snappy");
		properties.put("enable.auto.commit", false);
		
		properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		
		properties.putAll(PropertiesUtil.createProperties(propertiesString));
		properties.putAll(kafkaProperties);
	        
		KafkaConsumer<Object, Object> kafkaConsumer = null;
		try {
			kafkaConsumer = new KafkaConsumer<Object, Object>(properties);
			logger.info("buildKafkaConsumer() properties:"+properties);
			
			kafkaConsumer.listTopics();
			
			return kafkaConsumer;
		}catch(Exception e) {
			throw new RuntimeException("buildKafkaConsumer error,msg:"+e+" properties:"+properties,e);
		}
	}
	
	public void startConsumerKafkaData() {
		
		KafkaConsumer<Object,Object> kafkaConsumer = buildKafkaConsumer(properties);
		if(sync) {
			kafkaConsumer.subscribe(Arrays.asList(topic));
			this.kafkaConsumer = kafkaConsumer;
		}else {
			List<PartitionInfo> partitions = kafkaConsumer.partitionsFor(topic);
			for(PartitionInfo p : partitions) {
				kafkaConsumer = buildKafkaConsumer(properties);
				TopicPartition topicPartition= new TopicPartition(p.topic(),p.partition());
				kafkaConsumer.assign(Arrays.asList(topicPartition));
				ConsumerWorker worker = startConsumerThread(kafkaConsumer);
				kafkaConsumerThreads.add(worker);
			}
		}
		
	}

	private ConsumerWorker startConsumerThread(KafkaConsumer<Object, Object> kafkaConsumer) {
		ConsumerWorker task = new ConsumerWorker();
		task.kafkaConsumer = kafkaConsumer;
		
		Thread thread = new Thread(task,"kafka_consumer_"+topic);
		thread.start();
		return task;
	}

	public class ConsumerWorker implements Runnable{
		private KafkaConsumer<Object, Object> kafkaConsumer;
		
		public void run() {
			try {
				logger.info("consumer thread start, work on partitions:" + kafkaConsumer.assignment());
				
				while(running) {
					try {
						ConsumerRecords<Object, Object> records = kafkaConsumer.poll(1000 * 3);
						if(records == null || records.isEmpty()) {
							continue;
						}
						
						for(ConsumerRecord<Object,Object> c : records) {
							Object value = processOne(c);
							if(value != null) {
								queue.put(value);
							}
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
			IOUtils.closeQuietly(kafkaConsumer);
		}

	}

	@Override
	public void close() throws IOException {
		running = false;
		
		IOUtils.closeQuietly(kafkaConsumer);
		
		for(ConsumerWorker w : kafkaConsumerThreads) {
			w.close();
		}
	}

	@Override
	public List<Object> read(int size) {
		initIfNeed();
		
		if(sync) {
			return syncRead();
		}else {
			return asyncRead(size);
		}
	}

	private List<Object> syncRead() {
		while(true) {
			ConsumerRecords<Object, Object> records = kafkaConsumer.poll(3000);
			if(records == null || records.isEmpty()) {
				continue;
			}
			
			List<Object> result = new ArrayList(records.count());
			for(ConsumerRecord<Object,Object> c : records) {
				Object value = processOne(c);
				
				if(value != null) {
					result.add(value);
				}
			}
			return result;
		}
	}

	protected Object processOne(ConsumerRecord<Object, Object> c) {
		return c.value();
	}

	private List<Object> asyncRead(int size) {
		try {
			int timeout = 3000;
			return BlockingQueueUtil.batchTake(queue,size, timeout);
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
