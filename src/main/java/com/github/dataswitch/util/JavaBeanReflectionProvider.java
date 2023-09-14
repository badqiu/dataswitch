package com.github.dataswitch.util;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.hadoop.util.StringUtils;

import com.thoughtworks.xstream.converters.reflection.PureJavaReflectionProvider;

/**
 * 提供优先使用java bean的property反射进行数据写入
 * 
 * @author badqiu
 *
 */
public class JavaBeanReflectionProvider extends PureJavaReflectionProvider {

	@Override
	public Field getField(Class definedIn, String fieldName) {
		Field field = super.getField(definedIn, fieldName);
		return toStringFieldIfSimple(field);
	}
	
	@Override
	public Class getFieldType(Object object, String fieldName, Class definedIn) {
		Class type = super.getFieldType(object, fieldName, definedIn);
		if(isSimpleType(type)) {
			return String.class;
		}
		return type;
	}
	
	@Override
	public Field getFieldOrNull(Class definedIn, String fieldName) {
		Field fieldOrNull = super.getFieldOrNull(definedIn, fieldName);
		return toStringFieldIfSimple(fieldOrNull);
	}
	
	private Field toStringFieldIfSimple(Field field) {
		if(field == null) return null;
		
		try {
			Class<?> type = field.getType();
			if(isSimpleType(type)) {
				FieldUtils.writeDeclaredField(field, "type", String.class,true);
				return field;
			}
			
			return field;
		}catch(Exception e) {
			throw new RuntimeException("error on field:"+field.getName(),e);
		}
	}

	private boolean isSimpleType(Class<?> type) {
		if(type.isPrimitive()) {
			return true;
		}
		if(type == Long.class || type == Integer.class 
				|| type == Double.class || type == Float.class
				|| type == Boolean.class || type == String.class) {
			return true;
		}

		return false;
	}

	@Override
	public void writeField(Object object, String fieldName, Object value, Class definedIn) {
		try {
			invokePerfetMethod(object,fieldName,value,definedIn);
		}catch(Exception e) {
	        super.writeField(object, fieldName, value, definedIn);
		}
	}

	private void invokePerfetMethod(Object object, String fieldName, Object value, Class definedIn) throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		try {
			Method method = object.getClass().getDeclaredMethod("set"+StringUtils.camelize(fieldName),String.class);
			if(method != null) {
				invokeMethod(object, method,value);
				return;
			}
		}catch(Exception e) {
			BeanUtils.setProperty(object, fieldName, value);
		}
	}

	private void invokeMethod(Object object, Method method,Object...args)
			throws IllegalAccessException, InvocationTargetException {
		if(method == null) {
			return;
		}
		
		method.setAccessible(true);
		method.invoke(object, args);
		return;
	}
	

	
}
