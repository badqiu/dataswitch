package com.github.dataswitch.util.xstream;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.reflect.FieldUtils;

import com.github.dataswitch.util.BeanUtils;
import com.thoughtworks.xstream.converters.reflection.PureJavaReflectionProvider;

/**
 * 提供优先使用java bean的property或method反射进行数据写入
 * 
 * 原有问题:  直接通过field进行数据读写，绕过了bean属性方法，绕过了方法检查
 * 
 * @author badqiu
 *
 */
public class JavaBeanReflectionProvider extends PureJavaReflectionProvider {

	@Override
	public Field getField(Class definedIn, String fieldName) {
		Field field = super.getField(definedIn, fieldName);
		return toStringFieldIfSimple(definedIn,field,fieldName);
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
		return toStringFieldIfSimple(definedIn,fieldOrNull,fieldName);
	}
	
	private Field toStringFieldIfSimple(Class definedIn, Field field,String fieldName) {
		
		try {
			if(field == null) {
				return getBeanPropertyAsField(definedIn, fieldName);
			}
			
			
			Class<?> type = field.getType();
			if(isSimpleType(type)) {
				return newField(definedIn,fieldName,String.class);
			}
			
			return field;
		}catch(Exception e) {
			throw new RuntimeException("error on field:"+fieldName,e);
		}
	}

	private Field getBeanPropertyAsField(Class definedIn, String fieldName) throws IllegalAccessException {
		PropertyDescriptor pd = org.springframework.beans.BeanUtils.getPropertyDescriptor(definedIn, fieldName);
		if(pd != null) {
			if(isSimpleType(pd.getPropertyType())) {
				return newField(definedIn,fieldName,String.class);
			}else {
				return newField(definedIn,fieldName,pd.getPropertyType());
			}
		}
		
		String methodName = toSetMethodName(fieldName);
		if(findMethod(definedIn,methodName) != null) {
			return newField(definedIn,fieldName,String.class);
		}
		
		return null;
	}

	private boolean isSimpleType(Class<?> type) {
		return ClassUtils.isPrimitiveOrWrapper(type);
	}

	@Override
	public void writeField(Object object, String fieldName, Object value, Class definedIn) {
		try {
			invokePerfetMethod(object,fieldName,value);
		}catch(Exception e) {
	        super.writeField(object, fieldName, value, definedIn);
		}
	}

	protected void invokePerfetMethod(Object object, String fieldName, Object value) throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		if(object == null) return;
		try {
			invokeBySetMethod(object, fieldName, value);
		}catch(Exception e) {
			BeanUtils.setProperty(object, fieldName, value);
		}
	}

	private void invokeBySetMethod(Object object, String fieldName, Object value)
			throws IllegalAccessException, InvocationTargetException {
		
		String methodName = toSetMethodName(fieldName);
		
		try {
			Class valueClass = value != null ? value.getClass() : String.class;
			Method method = object.getClass().getDeclaredMethod(methodName,valueClass);
			if(method != null) {
				invokeMethod(object, method,value);
				return;
			}
		}catch(java.lang.NoSuchMethodException e) {
			//ignore
		}
		
		Method method = findMethod(object.getClass(),methodName);
		if(method != null) {
			Class targetType = method.getParameterTypes()[0];
			Object finalValue = ConvertUtils.convert(value,targetType);
			invokeMethod(object, method,finalValue);
		}
	}

	private String toSetMethodName(String fieldName) {
		if(fieldName == null) return null;
		
		String methodName = "set"+StringUtils.capitalize(fieldName);
		return methodName;
	}
	

	public static Field newField(Class definedIn,String fieldName, Class fieldType) throws IllegalAccessException {
		Field copy = definedIn.getDeclaredFields()[0];
		FieldUtils.writeDeclaredField(copy, "type", fieldType,true);
		FieldUtils.writeDeclaredField(copy, "name", fieldName,true);
		copy.setAccessible(true);
		return copy;
	}

	public static Method findMethod(Class definedIn, String method) {
		if(definedIn == null) return null;
		return org.springframework.beans.BeanUtils.findDeclaredMethodWithMinimalParameters(definedIn, method);
//		try {
//			Method[] methods = definedIn.getDeclaredMethods();
//			return findMethod(methods,method);
//		} catch (SecurityException e) {
//			throw new RuntimeException(e);
//		}
	}

	public static Method findMethod(Method[] methods,String method) {
		for(Method m : methods) {
			if(m.getName().equals(method)) {
				return m;
			}
		}
		return null;
	}
	
	public static void invokeMethod(Object object, Method method,Object...args)
			throws IllegalAccessException, InvocationTargetException {
		if(object == null) return;
		if(method == null) return;
		
		method.setAccessible(true);
		method.invoke(object, args);
		return;
	}
	
}
