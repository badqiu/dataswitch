package com.github.dataswitch.input;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.function.Supplier;

import com.github.dataswitch.BaseObject;
import com.github.dataswitch.util.Util;

public class FunctionInput <RESULT> extends BaseObject implements Input{

	private Function<Integer,RESULT> function;
	
	public FunctionInput() {
		super();
	}

	public FunctionInput(Function<Integer,RESULT> function) {
		setFunction(function);
	}
	
	public FunctionInput(Supplier<RESULT> supplier) {
		setSupplier(supplier);
	}
	
	public Function<Integer,RESULT> getFunction() {
		return function;
	}

	public void setFunction(Function<Integer,RESULT> function) {
		Objects.requireNonNull(function, "function must be not null");
		this.function = function;
	}
	
	public void setSupplier(Supplier<RESULT> supplier) {
		setFunction(Util.toFunction(supplier));
	}
	
	private void setCallable(Callable<RESULT> callable) {
		setFunction(Util.toFunction(callable));
	}

	@Override
	public List<Object> read(int size) {
		Object result = function.apply(size);
		Collection collection = Util.oneToList(result);
		return new ArrayList(collection);
	}
	
	
}
