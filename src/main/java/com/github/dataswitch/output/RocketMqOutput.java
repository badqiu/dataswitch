package com.github.dataswitch.output;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientConfigurationBuilder;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.message.MessageBuilder;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.ProducerBuilder;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dataswitch.util.TableName;



public class RocketMqOutput implements Output,TableName{

	protected static Logger logger = LoggerFactory.getLogger(RocketMqOutput.class);
	
	private static ObjectMapper objectMapper = new ObjectMapper();
	
	private String topic;

	private String endpoints;

	private String messageGroup;
	private String messageKey;
	private String messageTag;
	
	private Producer producer;
	private ClientServiceProvider clientServiceProvider;

	
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

	public String getMessageGroup() {
		return messageGroup;
	}

	public void setMessageGroup(String messageGroup) {
		this.messageGroup = messageGroup;
	}

	public String getMessageKey() {
		return messageKey;
	}

	public void setMessageKey(String messageKey) {
		this.messageKey = messageKey;
	}

	public String getMessageTag() {
		return messageTag;
	}

	public void setMessageTag(String messageTag) {
		this.messageTag = messageTag;
	}

	public Producer buildProducer()  {
		clientServiceProvider = ClientServiceProvider.loadService();
        ClientConfigurationBuilder clientConfigurationBuilder = ClientConfiguration.newBuilder().setEndpoints(endpoints);
        ClientConfiguration configuration = clientConfigurationBuilder.build();
        
        ProducerBuilder producerBuilder = clientServiceProvider.newProducerBuilder()
            .setTopics(topic)
            .setClientConfiguration(configuration);

		try {
			Producer producer = producerBuilder.build();
			return producer;
		} catch (ClientException e) {
			throw new RuntimeException("build producer error,topic:"+topic,e);
		}
        
	}
	
	@Override
	public void close() throws IOException {
		IOUtils.closeQuietly(producer);
	}
	
	@Override
	public void open(Map<String, Object> params) throws Exception {
		Output.super.open(params);
		init();
	}

	private void init() {
		if(producer == null) {
			synchronized (this) {
				if(producer == null) {
					producer = buildProducer();
				}
			}
		}
	}
	
	@Override
	public void write(List<Object> rows) {
		try {
			write0((List)rows);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private void write0(List<Map> rows) throws ClientException, JsonProcessingException {
		if(CollectionUtils.isEmpty(rows)) {
			return;
		}
		
		for(Map row : rows) {
			Message message = buildMessage(row);
			SendReceipt sendReceipt = producer.send(message);
		}
	}

	private Message buildMessage(Map row) throws JsonProcessingException {
		MessageBuilder messageBuilder = clientServiceProvider.newMessageBuilder();
		
		if(StringUtils.isNotBlank(messageTag)) {
			String msgTag = (String)row.get(messageTag);
			messageBuilder.setTag(msgTag);
		}
		
		if(StringUtils.isNotBlank(messageKey)) {
			String msgKeys = (String)row.get(messageKey);
			messageBuilder.setKeys(msgKeys);
		}
		
		if(StringUtils.isNotBlank(messageGroup)) {
			messageBuilder.setMessageGroup(messageGroup);
		}
		
		Message message = messageBuilder
		            .setTopic(topic)
		            .setBody(objectMapper.writeValueAsBytes(row))
		            .build();
		return message;
	}
	
	@Override
	public void flush() throws IOException {
	}

}
