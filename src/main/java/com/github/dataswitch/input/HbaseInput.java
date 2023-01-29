package com.github.dataswitch.input;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Scan.ReadType;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dataswitch.enums.Constants;
import com.github.dataswitch.support.HbaseProvider;
import com.github.dataswitch.util.InputOutputUtil;
import com.github.dataswitch.util.PropertiesUtil;

public class HbaseInput  extends HbaseProvider implements Input{
	private static Logger logger = LoggerFactory.getLogger(HbaseInput.class);
	
	private String columnFamily = null;
    private String startKey = null;
    private String endKey = null;
    private String columnsType = null;
    

    protected String encoding = StandardCharsets.UTF_8.name();
    protected int scanCacheSize = Constants.DEFAULT_BUFFER_SIZE;
    protected int  scanBatchSize = Constants.DEFAULT_BUFFER_SIZE;

    protected Result _lastResult = null;
    protected Scan _scan;
    protected ResultScanner _resultScanner;

    protected Table _hbaseTable;
    private byte[] _startKey = null;
    private byte[] _endKey = null;
    private Charset _charset = null;
    private Properties _columnsType = null;
    

    protected void initScan(Scan scan) {
    };

    public void prepare() throws Exception {
        this._scan = new Scan();
        
        //this._scan.setSmall(false);
        this._scan.setReadType(ReadType.PREAD);
        
        this._scan.withStartRow(_startKey);
        this._scan.withStopRow(_endKey);
        this._scan.addFamily(Bytes.toBytes(columnFamily));
        
        logger.info("The task set startRowkey=[{}], endRowkey=[{}].", this.startKey, this.endKey);
        //scan的Caching Batch全部留在hconfig中每次从服务器端读取的行数，设置默认值未256
        this._scan.setCaching(this.scanCacheSize);
        //设置获取记录的列个数，hbase默认无限制，也就是返回所有的列,这里默认是100
        this._scan.setBatch(this.scanBatchSize);
        //为是否缓存块，hbase默认缓存,同步全部数据时非热点数据，因此不需要缓存
        this._scan.setCacheBlocks(false);
        initScan(this._scan);

        this._resultScanner = this._hbaseTable.getScanner(this._scan);
    }

    @Override
	public void close() throws Exception {
        InputOutputUtil.close(this._resultScanner);
        InputOutputUtil.close(this._hbaseTable);
    }

    protected Result getNextHbaseRow()  {
        try {
			return getNextHbaseRow0();
		} catch (IOException e) {
			throw new RuntimeException("getNextHbaseRow error",e);
		}
    }

	private Result getNextHbaseRow0() throws IOException {
		Result result;
        try {
            result = _resultScanner.next();
        } catch (IOException e) {
            if (_lastResult != null) {
                this._scan.setStartRow(_lastResult.getRow());
            }
            _resultScanner = this._hbaseTable.getScanner(_scan);
            result = _resultScanner.next();
            if (_lastResult != null && Bytes.equals(_lastResult.getRow(), result.getRow())) {
                result = _resultScanner.next();
            }
        }
        _lastResult = result;
        // may be null
        return result;
	}

    
	@Override
	public List<Object> read(int size) {
		List<Object> list = new ArrayList<Object>(size);
		for(int i = 0; i < size; i++) {
			Result result = getNextHbaseRow();
			if(result == null) {
				break;
			}
			
			Map row = result2Map(result);
			if(row == null) {
				continue;
			}
			
			list.add(row);
			
			if(list.size() > size) {
				break;
			}
		}
		return list;
	}
	
	protected Map result2Map(Result result) {
		if(result == null) return null;
		
		NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(Bytes.toBytes(columnFamily));
		
		return convertByColumnType(familyMap);
	}

	private Map convertByColumnType(NavigableMap<byte[], byte[]> familyMap) {
		Map map = new HashMap(familyMap.size() * 2);
		familyMap.forEach((k,v) -> {
			String key = new String(k,_charset);
			Object value = getValueByColumnType(key,v);
			if(value == null) {
				return;
			}
			
			map.put(key, value);
		});
		return familyMap;
	}

	private Object getValueByColumnType(String key, byte[] bytes) {
		if(bytes == null) return null;
		if(bytes.length == 0) return null;
		
		String columnType = getColumnType(key);
		if(StringUtils.isBlank(columnType)) {
			return Bytes.toString(bytes);
		}
		
		if("int".equals(columnType)) {
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
		}else if("string".equals(columnType)) {
			return Bytes.toString(bytes);
		}else if("boolean".equals(columnType)) {
			return Bytes.toBoolean(bytes);
		}else {
			return Bytes.toString(bytes);
		}
	}

	private String getColumnType(String key) {
		return _columnsType.getProperty(key);
	}

	@Override
	public void open(Map<String, Object> params) throws Exception {
		this._hbaseTable = getTable(getHbaseConfig(),getTable(),1);

        this._charset = Charset.forName(encoding);
        this._startKey = objectToHbaseBytes(startKey, _charset);
        this._endKey = objectToHbaseBytes(endKey, _charset);
        
        this._columnsType = PropertiesUtil.createProperties(columnsType);
        prepare();
	}
	

}
