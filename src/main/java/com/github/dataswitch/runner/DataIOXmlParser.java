package com.github.dataswitch.runner;

import java.io.InputStream;
import java.io.Reader;
import java.util.Map;

import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import com.github.dataswitch.InputsOutputs;
import com.github.dataswitch.input.JdbcInput;
import com.github.dataswitch.output.JdbcOutput;
import com.github.dataswitch.util.freemarker.FreemarkerInputStream;
import com.github.dataswitch.util.freemarker.FreemarkerReader;
import com.github.dataswitch.util.xstream.CustomMarshallingStrategy;
import com.github.dataswitch.util.xstream.JavaBeanReflectionProvider;
import com.github.dataswitch.util.xstream.SmartDurationConverter;
import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.DomDriver;

import freemarker.template.Configuration;

public class DataIOXmlParser implements ApplicationContextAware{

	private ApplicationContext applicationContext;
	private Map<Object, Object> beans;

	public DataIOXmlParser(){
	}
	
	public DataIOXmlParser(Map<Object, Object> beans,
			ApplicationContext applicationContext) {
		this.beans = beans;
		this.applicationContext = applicationContext;
	}
	
	public DataIOXmlParser(ApplicationContext applicationContext) {
		this.applicationContext = applicationContext;
	}
	
	public ApplicationContext getApplicationContext() {
		return applicationContext;
	}

	public void setApplicationContext(ApplicationContext applicationContext) {
		this.applicationContext = applicationContext;
	}

	public Map<Object, Object> getBeans() {
		return beans;
	}

	public void setBeans(Map<Object, Object> beans) {
		this.beans = beans;
	}

	public Beans fromXml(InputStream input,Map params,Configuration conf) {
		XStream xstream = newCustomXStream();
		
		FreemarkerInputStream freemarkerInput = new FreemarkerInputStream(input,conf,params);
		Beans r = (Beans)xstream.fromXML(freemarkerInput);
		
		return r;
	}

	public Beans fromXml(Reader input,Map params,Configuration conf) {
		XStream xstream = newCustomXStream();
		
		FreemarkerReader freemarkerInput = new FreemarkerReader(input,conf,params);
		Beans r = (Beans)xstream.fromXML(freemarkerInput);
		return r;
	}
	
	private XStream newCustomXStream() {
		XStream xstream = newBaseXStream(beans,applicationContext);
		
		xstream.alias("beans", Beans.class);
		xstream.addImplicitCollection(Beans.class, "beans");

		xstream.alias("JdbcInput", JdbcInput.class);
		xstream.alias("JdbcOutput", JdbcOutput.class);
		xstream.alias("InputsOutputs", InputsOutputs.class);
		
		
//		xstream.registerConverter(new JavaBeanConverter(xstream.getMapper(),JdbcInput.class),XStream.PRIORITY_VERY_LOW);
//		xstream.registerConverter(new JavaBeanConverter(xstream.getMapper(),JdbcOutput.class),XStream.PRIORITY_VERY_LOW);
//		xstream.registerConverter(new JavaBeanConverter(xstream.getMapper(),InputsOutputs.class),XStream.PRIORITY_VERY_LOW);
//		xstream.registerConverter(new JavaBeanConverter(xstream.getMapper()));
		return xstream;
	}
	
	
	public static XStream newBaseXStream(Map beans,ApplicationContext applicationContext) {
//		Assert.notNull(applicationContext,"applicationContext must be not null");
		
		XStream xstream = new XStream(new JavaBeanReflectionProvider(),new DomDriver());
		xstream.registerConverter(new SmartDurationConverter(),XStream.PRIORITY_VERY_HIGH);
		
		xstream.useAttributeFor(int.class);
		xstream.useAttributeFor(long.class);
		xstream.useAttributeFor(char.class);
		xstream.useAttributeFor(float.class);
		xstream.useAttributeFor(boolean.class);
		xstream.useAttributeFor(double.class);
		xstream.useAttributeFor(Integer.class);
		xstream.useAttributeFor(Long.class);
		xstream.useAttributeFor(Character.class);
		xstream.useAttributeFor(Float.class);
		xstream.useAttributeFor(Double.class);
		xstream.useAttributeFor(Boolean.class);
		xstream.useAttributeFor(String.class);
		
		CustomMarshallingStrategy marshallingStrategy = new CustomMarshallingStrategy();
		marshallingStrategy.setBeans(beans);
		marshallingStrategy.setApplicationContext(applicationContext);
		xstream.setMarshallingStrategy(marshallingStrategy);
		return xstream;
	}
	


}
