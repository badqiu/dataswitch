package com.github.dataswitch.input;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientConfigurationBuilder;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.apache.rocketmq.client.apis.consumer.PushConsumerBuilder;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dataswitch.util.QueueUtil;
import com.github.dataswitch.util.TableName;




public class RocketMqInput implements Input,TableName{

	protected static Logger logger = LoggerFactory.getLogger(RocketMqInput.class);
	
	private static ObjectMapper objectMapper = new ObjectMapper();
	
	private String topic;

	private String endpoints;

	private String tag = "*"; // 订阅消息的过滤规则，表示订阅所有Tag的消息。
	
    private String consumerGroup; // 为消费者指定所属的消费者分组，Group需要提前创建。
    
    private int asyncReadTimeout = 500; //异步读的超时时间
    private Class valueType = Map.class;
    
	private BlockingQueue<Object> _queue = new ArrayBlockingQueue<Object>(1000);

	private int consumptionThreadCount = 20;

	private boolean ignoreReadError = false;
	
	
	private ClientServiceProvider _clientServiceProvider;
	private ClientConfiguration _clientConfiguration;
	private PushConsumer _pushConsumer;
	
	
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
	
	public String getEndpoints() {
		return endpoints;
	}

	public void setEndpoints(String endpoints) {
		this.endpoints = endpoints;
	}

	public String getTag() {
		return tag;
	}

	public void setTag(String tag) {
		this.tag = tag;
	}

	public String getConsumerGroup() {
		return consumerGroup;
	}

	public void setConsumerGroup(String consumerGroup) {
		this.consumerGroup = consumerGroup;
	}

	public int getAsyncReadTimeout() {
		return asyncReadTimeout;
	}

	public void setAsyncReadTimeout(int asyncReadTimeout) {
		this.asyncReadTimeout = asyncReadTimeout;
	}

	public Class getValueType() {
		return valueType;
	}

	public void setValueType(Class valueType) {
		this.valueType = valueType;
	}

	public int getConsumptionThreadCount() {
		return consumptionThreadCount;
	}

	public void setConsumptionThreadCount(int consumptionThreadCount) {
		this.consumptionThreadCount = consumptionThreadCount;
	}

	public boolean isIgnoreReadError() {
		return ignoreReadError;
	}

	public void setIgnoreReadError(boolean ignoreReadError) {
		this.ignoreReadError = ignoreReadError;
	}

	public PushConsumer buildPushConsumer()  {
		Assert.hasText(endpoints,"endpoints must be not blank");
		Assert.hasText(consumerGroup,"consumerGroup must be not blank");
		Assert.hasText(topic,"topic must be not blank");
		
		_clientServiceProvider = ClientServiceProvider.loadService();
        ClientConfigurationBuilder clientConfigurationBuilder = ClientConfiguration.newBuilder().setEndpoints(endpoints);
        _clientConfiguration = clientConfigurationBuilder.build();
        
        
		try {
			
	        FilterExpression filterExpression = new FilterExpression(tag, FilterExpressionType.TAG);
	        
	        // 初始化PushConsumer，需要绑定消费者分组ConsumerGroup、通信参数以及订阅关系。
	        PushConsumerBuilder pushConsumerBuilder = _clientServiceProvider.newPushConsumerBuilder();
			pushConsumerBuilder.setConsumptionThreadCount(consumptionThreadCount);
	        
			PushConsumer pushConsumer = pushConsumerBuilder
	            .setClientConfiguration(_clientConfiguration)
	            // 设置消费者分组。
	            .setConsumerGroup(consumerGroup)
	            // 设置预绑定的订阅关系。
	            .setSubscriptionExpressions(Collections.singletonMap(topic, filterExpression))
	            // 设置消费监听器。
	            .setMessageListener(messageView -> {
	            	
	                try {
	                	Object value = processMsg(messageView);
	                	
	                	if(value != null) {
	                		_queue.put(value);
	                	}
	                	
					} catch (Exception e) {
						if(ignoreReadError) {
							logger.warn("ignore read error,messageView:"+messageView,e);
						}else {
							throw new RuntimeException("error",e);
						}
					}
	                
	                return ConsumeResult.SUCCESS;
	            })
	            .build();
			return pushConsumer;
		} catch (ClientException e) {
			throw new RuntimeException("build producer error,topic:"+topic,e);
		}
        
	}
	
	@Override
	public void close() throws IOException {
		IOUtils.closeQuietly(_pushConsumer);
	}
	
	@Override
	public void open(Map<String, Object> params) throws Exception {
		Input.super.open(params);
		init();
	}

	private synchronized void init() {
		if(_pushConsumer == null) {
			_pushConsumer = buildPushConsumer();
		}
	}
	
	@Override
	public List<Object> read(int size) {
		try {
			return read0(size);
		}catch(Exception e) {
			throw new RuntimeException("read error",e);
		}
	}

	private List<Object> read0(int size) throws InterruptedException  {
		List<Object> messages = QueueUtil.batchTake(_queue,size, asyncReadTimeout);
		return messages;
	}

	protected Object processMsg(MessageView msg) throws JsonParseException, JsonMappingException, IOException {
		byte[] array = msg.getBody().array();
		if(ArrayUtils.isEmpty(array)) return null;
		
		return objectMapper.readValue(array, valueType);
	}


}
