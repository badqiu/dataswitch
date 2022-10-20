package com.github.dataswitch;

import java.util.Properties;

public class BaseObject {

	private String id; // 对象ID
	private String remarks; // 对象备注
	private Properties props; // 对象自定义属性
	
//	private String createDate; //创建日期
//	private String author; //作者
//	private String version; //版本

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

}
