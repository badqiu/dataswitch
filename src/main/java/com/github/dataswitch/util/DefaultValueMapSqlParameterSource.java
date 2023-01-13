package com.github.dataswitch.util;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.springframework.jdbc.core.SqlParameterValue;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.util.Assert;
/**
 * 与MapSqlParameterSource区别： 不抛异常，返回默认值
 * 
 * @author badqiu
 *
 */
public class DefaultValueMapSqlParameterSource extends MapSqlParameterSource{


	private final Map<String, Object> values = new HashMap<String, Object>();

	private Object defaultValue;
	
	/**
	 * Create an empty MapSqlParameterSource,
	 * with values to be added via <code>addValue</code>.
	 * @see #addValue(String, Object)
	 */
	public DefaultValueMapSqlParameterSource() {
	}

	/**
	 * Create a new MapSqlParameterSource, with one value
	 * comprised of the supplied arguments.
	 * @param paramName the name of the parameter
	 * @param value the value of the parameter
	 * @see #addValue(String, Object)
	 */
	public DefaultValueMapSqlParameterSource(String paramName, Object value) {
		addValue(paramName, value);
	}

	/**
	 * Create a new MapSqlParameterSource based on a Map.
	 * @param values a Map holding existing parameter values (can be <code>null</code>)
	 */
	public DefaultValueMapSqlParameterSource(Map<String, ?> values) {
		addValues(values);
	}


	/**
	 * Add a parameter to this parameter source.
	 * @param paramName the name of the parameter
	 * @param value the value of the parameter
	 * @return a reference to this parameter source,
	 * so it's possible to chain several calls together
	 */
	public MapSqlParameterSource addValue(String paramName, Object value) {
		Assert.notNull(paramName, "Parameter name must not be null");
		this.values.put(paramName, value);
		if (value instanceof SqlParameterValue) {
			registerSqlType(paramName, ((SqlParameterValue) value).getSqlType());
		}
		return this;
	}

	/**
	 * Add a parameter to this parameter source.
	 * @param paramName the name of the parameter
	 * @param value the value of the parameter
	 * @param sqlType the SQL type of the parameter
	 * @return a reference to this parameter source,
	 * so it's possible to chain several calls together
	 */
	public MapSqlParameterSource addValue(String paramName, Object value, int sqlType) {
		Assert.notNull(paramName, "Parameter name must not be null");
		this.values.put(paramName, value);
		registerSqlType(paramName, sqlType);
		return this;
	}

	/**
	 * Add a parameter to this parameter source.
	 * @param paramName the name of the parameter
	 * @param value the value of the parameter
	 * @param sqlType the SQL type of the parameter
	 * @param typeName the type name of the parameter
	 * @return a reference to this parameter source,
	 * so it's possible to chain several calls together
	 */
	public MapSqlParameterSource addValue(String paramName, Object value, int sqlType, String typeName) {
		Assert.notNull(paramName, "Parameter name must not be null");
		this.values.put(paramName, value);
		registerSqlType(paramName, sqlType);
		registerTypeName(paramName, typeName);
		return this;
	}

	/**
	 * Add a Map of parameters to this parameter source.
	 * @param values a Map holding existing parameter values (can be <code>null</code>)
	 * @return a reference to this parameter source,
	 * so it's possible to chain several calls together
	 */
	public MapSqlParameterSource addValues(Map<String, ?> values) {
		if (values != null) {
			for (Map.Entry<String, ?> entry : values.entrySet()) {
				this.values.put(entry.getKey(), entry.getValue());
				if (entry.getValue() instanceof SqlParameterValue) {
					SqlParameterValue value = (SqlParameterValue) entry.getValue();
					registerSqlType(entry.getKey(), value.getSqlType());
				}
			}
		}
		return this;
	}

	/**
	 * Expose the current parameter values as read-only Map.
	 */
	public Map<String, Object> getValues() {
		return Collections.unmodifiableMap(this.values);
	}


	public boolean hasValue(String paramName) {
		return this.values.containsKey(paramName);
	}
	
	public Object getDefaultValue() {
		return defaultValue;
	}

	public void setDefaultValue(Object defaultValue) {
		this.defaultValue = defaultValue;
	}

	public Object getValue(String paramName) {
		Object result = this.values.get(paramName);
		if(result == null) {
			return defaultValue;
		}
		return result;
	}

	
}
