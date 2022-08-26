package com.github.dataswitch.output;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import com.github.dataswitch.BaseObject;

public class ComsumerOutput extends BaseObject implements Output{

	private Consumer<List<Object>> consumer;
	
	public ComsumerOutput() {
	}

	public ComsumerOutput(Consumer<List<Object>> consumer) {
		this.consumer = consumer;
	}
	
	public Consumer<List<Object>> getConsumer() {
		return consumer;
	}

	public void setConsumer(Consumer<List<Object>> consumer) {
		Objects.requireNonNull(consumer, "consumer must be not null");
		this.consumer = consumer;
	}

	@Override
	public void write(List<Object> rows) {
		consumer.accept(rows);
	}
	
	
}
