package com.github.dataswitch;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.github.dataswitch.util.BeanUtils;
import com.github.dataswitch.util.PropertiesUtil;
import com.github.dataswitch.util.URLQueryUtil;
import com.thoughtworks.xstream.XStream;

public class BaseObject implements Enabled {

	private String id; // 对象ID
	private String remarks; // 对象备注
	private Properties props; // 对象自定义属性
	
//	private String createDate; //创建日期
//	private String modifyDate; //修改日期
	private String author; //作者
//	private String version; //版本
//	private String changelog; //修改日志
//	private String see; //查看其它文档链接
//	private boolean deprecated = false; //己不推荐使用
	
	
	private boolean enabled = true;
	
	private String tags; //标签，一般用于过滤数据使用
	
	private boolean lock;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getRemarks() {
		return remarks;
	}

	public void setRemarks(String remarks) {
		this.remarks = remarks;
	}

	public Properties getProps() {
		return props;
	}

	public void setProps(Properties props) {
		this.props = props;
	}
	
	public boolean isEnabled() {
		return enabled;
	}

	public void setEnabled(boolean enabled) {
		this.enabled = enabled;
	}
	
	public String getAuthor() {
		return author;
	}

	public void setAuthor(String author) {
		this.author = author;
	}
	
	public String getTags() {
		return tags;
	}

	public void setTags(String tags) {
		this.tags = tags;
	}
	
	public boolean isLock() {
		return lock;
	}

	public void setLock(boolean lock) {
		this.lock = lock;
	}

	@Override
	public boolean enabled() {
		return isEnabled();
	}
	
	public void setConfigByJson(String json) {
		if(StringUtils.isBlank(json)) return;
		
		ObjectMapper objectMapper = new ObjectMapper();
		try {
			Map<String,String> params = objectMapper.readValue(json, Map.class);
			configByMap(params);
		} catch (Exception e) {
			throw new RuntimeException(id + " error configByJson:"+json,e);
		} 
	}
	
	public void setConfigByQuery(String query) {
		if(StringUtils.isBlank(query)) return;
		
		Map<String,String> params = URLQueryUtil.splitQueryForSingleValue(query);
		try {
			configByMap(params);
		} catch (Exception e) {
			throw new RuntimeException(id + " error configByQuery:"+query,e);
		} 
	}
	
	public void setConfigByProperties(String properties) {
		if(StringUtils.isBlank(properties)) return;
		
		Map<String,String> params = (Map)PropertiesUtil.createProperties(properties);
		try {
			configByMap(params);
		} catch (Exception e) {
			throw new RuntimeException(id + " error setConfigByProperties:"+properties,e);
		}
	}

	public void setConfigByXml(String xml) {
		if(StringUtils.isBlank(xml)) return;
		
		XStream xstream = new XStream();
		try {
			Map root = new HashMap();
			Map<String,String> params = (Map)xstream.fromXML(xml,root);
			
			configByMap(params);
		} catch (Exception e) {
			throw new RuntimeException(id + " error configByXml:"+xml,e);
		} 
	}

	public void configByMap(Map<String, String> params) {
		BeanUtils.copyProperties(this, params);
	}
	
	public void setConfigByYaml(String yaml) {
		if(StringUtils.isBlank(yaml)) return;
		
		YAMLMapper mapper = new YAMLMapper();
        try {
            Map<String, String> params = mapper.readValue(yaml, Map.class);
            configByMap(params);
        } catch (IOException e) {
        	throw new RuntimeException(id + " error configByYaml:"+yaml,e);
        }
	}
	

}
