package com.github.dataswitch;

import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dataswitch.util.BeanUtils;
import com.github.dataswitch.util.PropertiesUtil;
import com.github.dataswitch.util.URLQueryUtil;

public class BaseObject implements Enabled {

	private String id; // 对象ID
	private String remarks; // 对象备注
	private Properties props; // 对象自定义属性
	
//	private String createDate; //创建日期
//	private String modifyDate; //修改日期
//	private String author; //作者
//	private String version; //版本
//	private String changelog; //修改日志
//	private String see; //查看其它文档链接
//	private boolean deprecated = false; //己不推荐使用
	
	
	private boolean enabled = true;

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
	
	@Override
	public boolean enabled() {
		return isEnabled();
	}
	
	public void setConfigByJson(String json) {
		if(StringUtils.isBlank(json)) return;
		
		ObjectMapper objectMapper = new ObjectMapper();
		try {
			Map<String,String> params = objectMapper.readValue(json, Map.class);
			BeanUtils.copyProperties(this, params);
		} catch (Exception e) {
			throw new RuntimeException("error configByJson:"+json,e);
		} 
	}
	
	public void setConfigByQuery(String query) {
		Map<String,String> params = URLQueryUtil.splitQueryForSingleValue(query);
		try {
			BeanUtils.copyProperties(this, params);
		} catch (Exception e) {
			throw new RuntimeException("error configByQuery:"+query,e);
		} 
	}
	
	public void setConfigByProperties(String properties) {
		Map<String,String> params = (Map)PropertiesUtil.createProperties(properties);
		try {
			BeanUtils.copyProperties(this, params);
		} catch (Exception e) {
			throw new RuntimeException("error setConfigByProperties:"+properties,e);
		}
	}

}
