package com.github.dataswitch.output;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.MemoryCompactionPolicy;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.MobCompactPartitionPolicy;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.encoding.IndexBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.util.Assert;

import com.github.dataswitch.enums.Constants;
import com.github.dataswitch.enums.OutputMode;
import com.github.dataswitch.support.HbaseProvider;
import com.github.dataswitch.util.InputOutputUtil;
import com.github.dataswitch.util.Util;

public class HbaseOutput extends HbaseProvider implements Output{

	private String family; //要写入的hbase family,必填
	private String columns; //要写入的hbase列，可为空，为空将输入值全部写入
	
	private String rowkeyColumn = "rowkey";
	private String versionColumn; //版本列，timestamp列,可选
	private String encoding = StandardCharsets.UTF_8.name();
	
	private boolean skipWal = false; //关闭WAL日志写入，可以提升性能，但存在数据丢失风险
	private boolean skipNull = true; //是否忽略null值，不忽略将填写byte[0]作为null值
	private boolean skipEmpty = false; //是否忽略empty字符串
	private boolean createTable = false; //是否hbase 建表
	private int writeBufferSize = Constants.DEFAULT_BUFFER_SIZE; //批量写入的大小
	private OutputMode outputMode = OutputMode.upsert;
	
	private BufferedMutator _bufferedMutator;
	private String[] _columnsArray;
	private Charset _charset;
	
	public String getFamily() {
		return family;
	}

	public void setFamily(String family) {
		this.family = family;
	}

	public String getColumns() {
		return columns;
	}

	public void setColumns(String columns) {
		this.columns = columns;
	}

	public String getRowkeyColumn() {
		return rowkeyColumn;
	}

	public void setRowkeyColumn(String rowkeyColumn) {
		this.rowkeyColumn = rowkeyColumn;
	}

	public String getVersionColumn() {
		return versionColumn;
	}

	public void setVersionColumn(String versionColumn) {
		this.versionColumn = versionColumn;
	}

	public String getEncoding() {
		return encoding;
	}

	public void setEncoding(String encoding) {
		this.encoding = encoding;
	}

	public boolean isSkipWal() {
		return skipWal;
	}

	public void setSkipWal(boolean skipWal) {
		this.skipWal = skipWal;
	}

	public boolean isSkipNull() {
		return skipNull;
	}

	public void setSkipNull(boolean skipNull) {
		this.skipNull = skipNull;
	}
	
	public boolean isSkipEmpty() {
		return skipEmpty;
	}

	public void setSkipEmpty(boolean skipEmpty) {
		this.skipEmpty = skipEmpty;
	}

	public int getWriteBufferSize() {
		return writeBufferSize;
	}

	public void setWriteBufferSize(int writeBufferSize) {
		this.writeBufferSize = writeBufferSize;
	}
	
	public OutputMode getOutputMode() {
		return outputMode;
	}

	public void setOutputMode(OutputMode outputMode) {
		this.outputMode = outputMode;
	}
	
	public boolean isCreateTable() {
		return createTable;
	}

	public void setCreateTable(boolean createTable) {
		this.createTable = createTable;
	}

	@Override
	public void open(Map<String, Object> params) throws Exception {
		Assert.hasText(rowkeyColumn,"rowkeyColumn must be not blank");
		Assert.hasText(family,"family must be not blank");

		_columnsArray = Util.splitColumns(columns);
		_charset = Charset.forName(encoding);
		
		if(createTable) {
			executeCreateHbaseTable();
		}
		
		_bufferedMutator = getBufferedMutator(getHbaseConfig(),getTable(),writeBufferSize);
	}
	
	private void executeCreateHbaseTable() {
		executeCreateHbaseTableIfNotExists(getHbaseConfig(),getTable(),getFamily());
	}

	public static void executeCreateHbaseTableIfNotExists(String hbaseConfig,String table,String family) {
		org.apache.hadoop.conf.Configuration hConfiguration = getHbaseConfiguration(hbaseConfig);
		org.apache.hadoop.hbase.client.Connection hConnection = getHbaseConnection(hbaseConfig);
		TableName hTableName = TableName.valueOf(table);
		org.apache.hadoop.hbase.client.Admin admin = null;
		BufferedMutator bufferedMutator = null;
		try {
			admin = hConnection.getAdmin();
			if (admin.tableExists(hTableName)) {
				return;
			}
			
			TableDescriptor tableDesc = newTableDescriptor(hTableName, family);
			admin.createTable(tableDesc);
			checkHbaseTable(admin, hTableName);
		} catch (Exception e) {
			InputOutputUtil.close(bufferedMutator);
			InputOutputUtil.close(admin);
			InputOutputUtil.close(hConnection);
			throw new IllegalStateException("executeCreateHbaseTable error,userTable:" + table, e);
		}
	}

	private static TableDescriptor newTableDescriptor(TableName hTableName, String family) {
		TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(hTableName);
		
		ColumnFamilyDescriptorBuilder columnFamily = ColumnFamilyDescriptorBuilder.newBuilder(family.getBytes());
		builder.setColumnFamily(columnFamily.build());
		
		return builder.build();
	}

	@Override
	public void flush() throws IOException {
		if(_bufferedMutator != null) {
			_bufferedMutator.flush();
		}
	}
	
	@Override
	public void close() throws Exception {
		InputOutputUtil.close(_bufferedMutator);
	}
	
	@Override
	public void write(List<Object> rows) {
		if(CollectionUtils.isEmpty(rows)) return;
		
		for(Object row : rows) {
			Mutation m = convert2Mutation(row);
			
			if(m == null) {
				continue;
			}
			
			try {
				_bufferedMutator.mutate(m);
			} catch (IOException e) {
				throw new RuntimeException("write error, row:"+row,e);
			}
		}
	}

	private Mutation convert2Mutation(Object row) {
		return convert2Mutation((Map)row);
	}
	
	private Mutation convert2Mutation(Map row) {
		if(MapUtils.isEmpty(row)) return null;
		
		if(outputMode == outputMode.delete) {
			byte[] rowkey = getRowkey(row);
			return new Delete(rowkey);
		}else {
			return convertRecordToPut(row);
		}
	}
	
    public Put convertRecordToPut(Map record){
    	if(MapUtils.isEmpty(record)) return null;
    	
        Put put = newPut(record);
        
        if(_columnsArray == null) {
        	record.forEach((columnName,columnValueObject) -> {
        		addColumnForPut(put, (String)columnName,columnValueObject);
        	});
        }else {
	        for (String columnName : _columnsArray) {
	        	Object columnValueObject = record.get(columnName);
	            addColumnForPut(put, columnName,columnValueObject);
	        }
        }
        
        return put;
    }

	private void addColumnForPut(Put put, String columnName,Object columnValueObject) {
		byte[] columnValueBytes = getColumnBytes(columnValueObject);
		
		if(skipEmpty && ArrayUtils.isEmpty(columnValueBytes)) {
			return;
		}
		
		if(columnValueBytes == null) {
			if(skipNull) {
				return;
			}else {
				columnValueBytes = HConstants.EMPTY_BYTE_ARRAY;
			}
		}
		
		byte[] columnNameBytes = Bytes.toBytes(columnName);
		put.addColumn(Bytes.toBytes(family),columnNameBytes,columnValueBytes);
	}

	protected Put newPut(Map record) {
		byte[] rowkey = getRowkey(record);
        Put put = null;
        if(StringUtils.isBlank(versionColumn)){
            put = new Put(rowkey);
            if(skipWal){
                //等价与0.94 put.setWriteToWAL(super.walFlag);
                put.setDurability(Durability.SKIP_WAL);
            }
        }else {
            long timestamp = getVersion(record);
            put = new Put(rowkey,timestamp);
        }
		return put;
	}

	protected byte[] getColumnBytes(Object object) {
		return objectToHbaseBytes(object,_charset);
	}

	public byte[] getRowkey(Map record){
    	Object value = record.get(rowkeyColumn);
    	if(value == null) {
    		throw new IllegalStateException("rowkey must be not null by rowkeyColumn:"+rowkeyColumn+" on row:"+record);
    	}
    	
    	return objectToHbaseBytes(value,_charset);
    }

	public long getVersion(Map record) {
    	Object versionValue = record.get(versionColumn);
    	return objectToTimestamp(versionValue);
    }
	
}
