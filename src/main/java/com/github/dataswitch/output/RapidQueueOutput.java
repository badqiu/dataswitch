package com.github.dataswitch.output;

import java.util.List;

import org.apache.commons.collections.CollectionUtils;

import com.github.dataswitch.BaseObject;

public class RapidQueueOutput extends BaseObject implements Output{
	private String host;
	private int port;
	private String username;
	private String password;
	private String exchange;
	private String vhost;

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getExchange() {
		return exchange;
	}

	public void setExchange(String exchange) {
		this.exchange = exchange;
	}
	
	public String getVhost() {
		return vhost;
	}

	public void setVhost(String vhost) {
		this.vhost = vhost;
	}

	@Override
	public void close() {
	}

	@Override
	public void write(List<Object> rows) {
		if(CollectionUtils.isEmpty(rows)) return;
	}

}
