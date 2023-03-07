package com.github.dataswitch.input;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.springframework.util.Assert;

import com.github.dataswitch.BaseObject;

public class ObjectMethodInput extends BaseObject  implements Input{

	private Object object;
	private String method;
	
	private Object[] args = null;
	private boolean invoked = false;
	private Method _targetMethod = null;
	
	public Object getObject() {
		return object;
	}

	public void setObject(Object object) {
		this.object = object;
	}

	public String getMethod() {
		return method;
	}

	public void setMethod(String method) {
		this.method = method;
	}

	public Object[] getArgs() {
		return args;
	}

	public void setArgs(Object... args) {
		this.args = args;
	}

	@Override
	public List<Object> read(int size) {
		if(invoked) {
			return null;
		}
		invoked = true;
		
		Assert.notNull(_targetMethod,"_targetMethod must be not null");
		
		try {
			Object result = _targetMethod.invoke(object,args);
			return convert2List(result);
		} catch (Exception e) {
			throw new RuntimeException("error invoke method:"+_targetMethod+" on object:"+object,e);
		} 
	}
	
	private List<Object> convert2List(Object result) {
		if(result == null)
			return null;
		if(result.getClass().isArray()) {
			return new ArrayList(Arrays.asList((Object[])result));
		}
		if(result instanceof Collection) {
			return new ArrayList((Collection) result);
		}
		return new ArrayList(Arrays.asList(result));
	}
	
	private Method getRequiredMethod(Object object,String methodName) {
		Method targetMethod = null;
		for(Method method : object.getClass().getMethods()) {
			if(method.getName().equals(methodName)) {
				targetMethod = method;
			}
		}
		
		if(targetMethod == null) {
			throw new IllegalStateException("not found method:"+method+" on object:"+object);
		}
		return targetMethod;
	}
	
	@Override
	public void open(Map<String, Object> params) throws Exception {
		Input.super.open(params);
		
		if(_targetMethod == null) {
			_targetMethod = getRequiredMethod(object,method);
		}
	}
	
	@Override
	public void close() throws IOException {
	}
	
	
}
