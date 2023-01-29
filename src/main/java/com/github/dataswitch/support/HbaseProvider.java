package com.github.dataswitch.support;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import com.github.dataswitch.BaseObject;
import com.github.dataswitch.util.InputOutputUtil;
import com.github.dataswitch.util.PropertiesUtil;

public class HbaseProvider extends BaseObject {
	private String hbaseConfig;
	private String zookeeperZnodeParent = "/hbase";
	private String table;

	Connection _connection = null;

	private static Logger logger = LoggerFactory.getLogger(HbaseProvider.class);
	
	public String getHbaseConfig() {
		return hbaseConfig;
	}

	public void setHbaseConfig(String hbaseConfig) {
		this.hbaseConfig = hbaseConfig;
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public static Object convertBytesByDataType(byte[] bytes, String columnType,Charset charset) {
		if(ArrayUtils.isEmpty(bytes)) return null;
		
		if(StringUtils.isBlank(columnType)) {
			return new String(bytes,charset);
		}
		
		if("string".equals(columnType)) {
			return new String(bytes,charset);
		}else if("int".equals(columnType)) {
			return Bytes.toInt(bytes);
		}else if("long".equals(columnType)) {
			return Bytes.toLong(bytes);
		}else if("date".equals(columnType)) {
			long value = Bytes.toLong(bytes);
			return new Date(value);
		}else if("double".equals(columnType)) {
			return Bytes.toDouble(bytes);
		}else if("float".equals(columnType)) {
			return Bytes.toFloat(bytes);
		}else if("boolean".equals(columnType)) {
			return Bytes.toBoolean(bytes);
		}else {
			return new String(bytes,charset);
		}
	}
	
    public static byte[] objectToHbaseBytes(Object value,Charset encoding) {
    	if(value == null) return null;
    	
    	if(value instanceof String) {
    		return ((String)value).getBytes(encoding);
    	}else if (value instanceof Double) {
    		return Bytes.toBytes((Double)value);
    	}else if (value instanceof Float) {
    		return Bytes.toBytes((Float)value);
    	}else if (value instanceof Long) {
    		return Bytes.toBytes((Long)value);
    	}else if (value instanceof Integer) {
    		return Bytes.toBytes((Integer)value);
    	}else if (value instanceof Short) {
    		return Bytes.toBytes((Short)value);
    	}else if (value instanceof BigDecimal) {
    		return Bytes.toBytes((BigDecimal)value);
    	}else if (value instanceof Boolean) {
    		return Bytes.toBytes((Boolean)value);
    	}else if (value instanceof ByteBuffer) {
    		return Bytes.toBytes((ByteBuffer)value);
    	}else if (value instanceof Date) {
    		return Bytes.toBytes(((Date)value).getTime());
    	}
    	
		return value.toString().getBytes(encoding);
	}

	public static long objectToTimestamp(Object versionValue) {
		if(versionValue == null) {
    		return 0;
    	}
    	
    	if(versionValue instanceof Long) {
    		return (Long)versionValue;
    	}
    	if(versionValue instanceof Number) {
    		return ((Number)versionValue).longValue();
    	}
    	if(versionValue instanceof String) {
    		String versionDate = (String)versionValue;
    		if(StringUtils.isBlank(versionDate)) {
    			return 0;
    		}
    		
    		Date date;
			try {
				date = DateUtils.parseDate(versionDate, "yyyy-MM-dd HH:mm:ss.SSS","yyyy-MM-dd HH:mm:ss");
				return date.getTime();
			} catch (java.text.ParseException e) {
				throw new RuntimeException("cannot parse date:"+versionDate,e);
			}
    	}
    	
    	throw new RuntimeException("error value,cannot parse value to date:"+versionValue);
	}
	
	public static org.apache.hadoop.conf.Configuration getHbaseConfiguration(String hbaseConfig) {
		Assert.hasText(hbaseConfig, "hbaseConfig must be not blank,cannot connect to hbase");
		org.apache.hadoop.conf.Configuration hConfiguration = HBaseConfiguration.create();
		try {
			Map<String, String> hbaseConfigMap = (Map) PropertiesUtil.createProperties(hbaseConfig);
			// 用户配置的 key-value 对 来表示 hbaseConfig
			Assert.isTrue(hbaseConfigMap != null, "hbaseConfig不能为空Map结构!");
			for (Map.Entry<String, String> entry : hbaseConfigMap.entrySet()) {
				hConfiguration.set(entry.getKey(), entry.getValue());
			}
		} catch (Exception e) {
			throw new RuntimeException("getHbaseConfiguration error", e);
		}
		return hConfiguration;
	}

	public static org.apache.hadoop.hbase.client.Connection getHbaseConnection(String hbaseConfig) {
		org.apache.hadoop.conf.Configuration hConfiguration = getHbaseConfiguration(hbaseConfig);

		org.apache.hadoop.hbase.client.Connection hConnection = null;
		try {
			hConnection = ConnectionFactory.createConnection(hConfiguration);
		} catch (Exception e) {
			InputOutputUtil.close(hConnection);
			throw new RuntimeException("getHbaseConnection error", e);
		}
		return hConnection;
	}

	public static BufferedMutator getBufferedMutator(String hbaseConfig, String userTable, long writeBufferSize) {
		org.apache.hadoop.conf.Configuration hConfiguration = getHbaseConfiguration(hbaseConfig);
		org.apache.hadoop.hbase.client.Connection hConnection = getHbaseConnection(hbaseConfig);
		TableName hTableName = TableName.valueOf(userTable);
		org.apache.hadoop.hbase.client.Admin admin = null;
		BufferedMutator bufferedMutator = null;
		try {
			admin = hConnection.getAdmin();
			checkHbaseTable(admin, hTableName);
			// 参考HTable getBufferedMutator()
			bufferedMutator = hConnection.getBufferedMutator(new BufferedMutatorParams(hTableName)
					.pool(HTable.getDefaultExecutor(hConfiguration)).writeBufferSize(writeBufferSize));
		} catch (Exception e) {
			InputOutputUtil.close(bufferedMutator);
			InputOutputUtil.close(admin);
			InputOutputUtil.close(hConnection);
			throw new IllegalStateException("getBufferedMutator error,userTable:" + userTable, e);
		}
		return bufferedMutator;
	}

	public static Table getTable(String hbaseConfig, String userTable, long writeBufferSize) {
		org.apache.hadoop.hbase.client.Connection hConnection = getHbaseConnection(hbaseConfig);
		TableName hTableName = TableName.valueOf(userTable);
		org.apache.hadoop.hbase.client.Admin admin = null;
		org.apache.hadoop.hbase.client.Table hTable = null;
		try {
			admin = hConnection.getAdmin();
			checkHbaseTable(admin, hTableName);
			hTable = hConnection.getTable(hTableName);
			BufferedMutatorParams bufferedMutatorParams = new BufferedMutatorParams(hTableName);
			bufferedMutatorParams.writeBufferSize(writeBufferSize);
		} catch (Exception e) {
			InputOutputUtil.close(hTable);
			InputOutputUtil.close(admin);
			InputOutputUtil.close(hConnection);
			throw new IllegalStateException("getTable error,userTable:" + userTable, e);
		}
		return hTable;
	}

	public static void deleteTable(String hbaseConfig, String userTable) {
		logger.info(String.format("由于您配置了deleteType delete,HBasWriter begins to delete table %s .", userTable));
		Scan scan = new Scan();
		org.apache.hadoop.hbase.client.Table hTable = getTable(hbaseConfig, userTable,1);
		ResultScanner scanner = null;
		try {
			scanner = hTable.getScanner(scan);
			for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
				hTable.delete(new Delete(rr.getRow()));
			}
		} catch (Exception e) {
			throw new IllegalStateException("deleteTable error,userTable:" + userTable, e);
		} finally {
			InputOutputUtil.close(scanner);
			InputOutputUtil.close(hTable);
		}
	}

	public static void truncateTable(String hbaseConfig, String userTable) {
		logger.info(String.format("由于您配置了 truncate 为true,HBasWriter begins to truncate table %s .", userTable));
		TableName hTableName = TableName.valueOf(userTable);
		org.apache.hadoop.hbase.client.Connection hConnection = getHbaseConnection(hbaseConfig);
		org.apache.hadoop.hbase.client.Admin admin = null;
		try {
			admin = hConnection.getAdmin();
			checkHbaseTable(admin, hTableName);
			admin.disableTable(hTableName);
			admin.truncateTable(hTableName, true);
		} catch (Exception e) {
			throw new IllegalStateException("truncateTable error,userTable:" + userTable, e);
		} finally {
			InputOutputUtil.close(admin);
			InputOutputUtil.close(hConnection);
		}
	}

	private static void checkHbaseTable(Admin admin, TableName hTableName) throws IOException {
		if (!admin.tableExists(hTableName)) {
			throw new IllegalStateException("HBase源头表" + hTableName.toString() + "不存在, 请检查您的配置 或者 联系 Hbase 管理员.");
		}
		if (!admin.isTableAvailable(hTableName)) {
			throw new IllegalStateException("HBase源头表" + hTableName.toString() + " 不可用, 请检查您的配置 或者 联系 Hbase 管理员.");
		}
		if (admin.isTableDisabled(hTableName)) {
			throw new IllegalStateException(
					"HBase源头表" + hTableName.toString() + "is disabled, 请检查您的配置 或者 联系 Hbase 管理员.");
		}
	}

	public Table getHbaseTable() throws IOException {
		if (_connection == null) {
			_connection = getHbaseConnection(hbaseConfig);
		}
		return _connection.getTable(TableName.valueOf(table));
	}

}
