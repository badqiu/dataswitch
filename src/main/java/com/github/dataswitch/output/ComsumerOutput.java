package com.github.dataswitch.output;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import com.github.dataswitch.BaseObject;

public class ComsumerOutput extends BaseObject implements Output{

	private Consumer<List<Map<String, Object>>> consumer;
	
	public ComsumerOutput() {
	}

	public ComsumerOutput(Consumer<List<Map<String, Object>>> consumer) {
		this.consumer = consumer;
	}
	
	public Consumer<List<Map<String, Object>>> getConsumer() {
		return consumer;
	}

	public void setConsumer(Consumer<List<Map<String, Object>>> consumer) {
		Objects.requireNonNull(consumer, "consumer must be not null");
		this.consumer = consumer;
	}

	@Override
	public void write(List<Map<String, Object>> rows) {
		consumer.accept(rows);
	}
	
	
}
