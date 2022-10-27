package com.github.dataswitch;

import java.util.Map;
import java.util.Properties;

import com.github.dataswitch.util.URLQueryUtil;
import com.github.rapid.common.beanutils.BeanUtils;

public class BaseObject implements Enabled {

	private String id; // 对象ID
	private String remarks; // 对象备注
	private Properties props; // 对象自定义属性
	
//	private String createDate; //创建日期
//	private String author; //作者
//	private String version; //版本
//	private String changelog; //修改日志
	
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
	
	public void setConfigByQuery(String query) {
		Map<String,String> params = URLQueryUtil.splitQueryForSingleValue(query);
		BeanUtils.copyProperties(this, params);
	}

}
