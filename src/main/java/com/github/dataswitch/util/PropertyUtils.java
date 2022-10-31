package com.github.dataswitch.util;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.util.Map;

/**
 * apache PropertyUtils的等价类，修改如下:
 * 1. 将check exception改为uncheck exception
 * 2. getSimpleProperty(bean,name) getProperty(bean,name) 如果发现bean是Map则使用 Map.get(name)返回值
 * @author badqiu
 *
 */
public class PropertyUtils {

	private static void handleException(Exception e) {
		BeanUtils.handleReflectionException(e);
	}
	
	public static void clearDescriptors() {
		com.github.dataswitch.util.PropertyUtils.clearDescriptors();
	}

	public static void copyProperties(Object dest, Object orig){
		try {
			com.github.dataswitch.util.PropertyUtils.copyProperties(dest, orig);
		}catch(Exception e) {
			handleException(e);
		}
	}

	public static Map describe(Object bean) {
		try {
			return com.github.dataswitch.util.PropertyUtils.describe(bean);
		}catch(Exception e) {
			handleException(e);
			return null;
		}
	}

	public static Object getIndexedProperty(Object bean, String name, int index){
		try {
			return com.github.dataswitch.util.PropertyUtils.getIndexedProperty(bean, name, index);
		}catch(Exception e) {
			handleException(e);
			return null;
		}
	}

	public static Object getIndexedProperty(Object bean, String name) {
		try {
			return com.github.dataswitch.util.PropertyUtils.getIndexedProperty(bean, name);
		}catch(Exception e) {
			handleException(e);
			return null;
		}
	}

	public static Object getMappedProperty(Object bean, String name, String key) {
		try {
			return com.github.dataswitch.util.PropertyUtils.getMappedProperty(bean, name, key);
		}catch(Exception e) {
			handleException(e);
			return null;
		}
	}

	public static Object getMappedProperty(Object bean, String name) {
		try {
			return com.github.dataswitch.util.PropertyUtils.getMappedProperty(bean, name);
		}catch(Exception e) {
			handleException(e);
			return null;
		}
	}

	public static Object getNestedProperty(Object bean, String name) {
		try {
			return com.github.dataswitch.util.PropertyUtils.getNestedProperty(bean, name);
		}catch(Exception e) {
			handleException(e);
			return null;
		}
	}

	public static Object getProperty(Object bean, String name) {
		if(bean == null) return null;
		try {
			if(bean instanceof Map) {
				return ((Map)bean).get(name);
			} else {
				return com.github.dataswitch.util.PropertyUtils.getProperty(bean, name);
			}
		}catch(Exception e) {
			handleException(e);
			return null;
		}
	}

	public static PropertyDescriptor getPropertyDescriptor(Object bean, String name) {
		try {
			return com.github.dataswitch.util.PropertyUtils.getPropertyDescriptor(bean, name);
		}catch(Exception e) {
			handleException(e);
			return null;
		}
	}

	public static PropertyDescriptor[] getPropertyDescriptors(Class beanClass) {
		return com.github.dataswitch.util.PropertyUtils.getPropertyDescriptors(beanClass);
	}

	public static PropertyDescriptor[] getPropertyDescriptors(Object bean) {
		return com.github.dataswitch.util.PropertyUtils.getPropertyDescriptors(bean);
	}

	public static Class getPropertyEditorClass(Object bean, String name) {
		try {
			return com.github.dataswitch.util.PropertyUtils.getPropertyEditorClass(bean, name);
		}catch(Exception e) {
			handleException(e);
			return null;
		}
	}

	public static Class getPropertyType(Object bean, String name) {
		try {
			return com.github.dataswitch.util.PropertyUtils.getPropertyType(bean, name);
		}catch(Exception e) {
			handleException(e);
			return null;
		}
	}

	public static Method getReadMethod(PropertyDescriptor descriptor) {
		return com.github.dataswitch.util.PropertyUtils.getReadMethod(descriptor);
	}

	public static Object getSimpleProperty(Object bean, String name){
		if(bean == null) return null;
		try {
			if(bean instanceof Map) {
				return ((Map)bean).get(name);
			} else {
				return com.github.dataswitch.util.PropertyUtils.getSimpleProperty(bean, name);
			}
		}catch(Exception e) {
			handleException(e);
			return null;
		}
	}

	public static Method getWriteMethod(PropertyDescriptor descriptor) {
		return com.github.dataswitch.util.PropertyUtils.getWriteMethod(descriptor);
	}


	public static boolean isReadable(Object bean, String name) {
		return com.github.dataswitch.util.PropertyUtils.isReadable(bean, name);
	}

	public static boolean isWriteable(Object bean, String name) {
		return com.github.dataswitch.util.PropertyUtils.isWriteable(bean, name);
	}

	public static void setIndexedProperty(Object bean, String name, int index,Object value) {
		try {
		com.github.dataswitch.util.PropertyUtils.setIndexedProperty(bean, name, index, value);
		}catch(Exception e) {
			handleException(e);
		}
	}

	public static void setIndexedProperty(Object bean, String name, Object value){
		try {
		com.github.dataswitch.util.PropertyUtils.setIndexedProperty(bean, name, value);
		}catch(Exception e) {
			handleException(e);
		}
	}

	public static void setMappedProperty(Object bean, String name, Object value){
		try {
		com.github.dataswitch.util.PropertyUtils.setMappedProperty(bean, name, value);
		}catch(Exception e) {
			handleException(e);
		}
	}

	public static void setMappedProperty(Object bean, String name, String key,Object value) {
		try{
		com.github.dataswitch.util.PropertyUtils.setMappedProperty(bean, name, key, value);
		}catch(Exception e) {
			handleException(e);
		}
	}

	public static void setNestedProperty(Object bean, String name, Object value){
		try {
		com.github.dataswitch.util.PropertyUtils.setNestedProperty(bean, name, value);
		}catch(Exception e) {
			handleException(e);
		}
	}

	public static void setProperty(Object bean, String name, Object value){
		try {
		com.github.dataswitch.util.PropertyUtils.setProperty(bean, name, value);
		}catch(Exception e) {
			handleException(e);
		}
	}

	public static void setSimpleProperty(Object bean, String name, Object value){
		try {
		com.github.dataswitch.util.PropertyUtils.setSimpleProperty(bean, name, value);
		}catch(Exception e) {
			handleException(e);
		}
	}

	
}
