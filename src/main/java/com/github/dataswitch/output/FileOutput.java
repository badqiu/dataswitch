package com.github.dataswitch.output;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import com.github.dataswitch.serializer.ByteSerializer;
import com.github.dataswitch.serializer.JsonSerializer;
import com.github.dataswitch.serializer.SerDesUtil;
import com.github.dataswitch.serializer.Serializer;
import com.github.dataswitch.serializer.TxtSerializer;
import com.github.dataswitch.serializer.XmlSerializer;
import com.github.dataswitch.util.BeanUtils;
import com.github.dataswitch.util.CompressUtil;
import com.github.dataswitch.util.TableName;


public class FileOutput extends BaseOutput implements Output,TableName{

	private static Logger log = LoggerFactory.getLogger(FileOutput.class);
	
	private String dir; //文件存储目录
	private long maxFileSize; //最大文件大小
	private String compressType; // 压缩类型: gzip, snappy,bzip2
	private boolean deleteDir = false; //是否写入之前，删除目录
	private String filename = "fileoutput_0000_";
	
	private String format = null;
	private transient  Serializer serializer;
	private transient OutputStream outputStream;
	
	public String getDir() {
		return dir;
	}

	public void setDir(String dir) {
		this.dir = dir;
	}
	
	public boolean isDeleteDir() {
		return deleteDir;
	}

	public void setDeleteDir(boolean deleteDir) {
		this.deleteDir = deleteDir;
	}

	public String getCompressType() {
		return compressType;
	}

	public void setCompressType(String compressType) {
		this.compressType = compressType;
	}
	
	public String getFilename() {
		return filename;
	}

	public void setFilename(String filename) {
		this.filename = filename;
	}
	
	public void setTable(String table) {
		setFilename(table);
	}
	
	@Override
	public String getTable() {
		return getFilename();
	}
	

	public Serializer getSerializer() {
		return serializer;
	}

	public void setSerializer(Serializer serializer) {
		this.serializer = serializer;
	}
	
	public long getMaxFileSize() {
		return maxFileSize;
	}

	public void setMaxFileSize(long maxFileSize) {
		this.maxFileSize = maxFileSize;
	}

	public String getFormat() {
		return format;
	}

	public void setFormat(String format) {
		this.format = format;
	}

	@Override
	public void open(Map<String, Object> params) throws Exception {
		super.open(params);
		init();
	}
	
	public void init() throws IOException {
		if(serializer == null) {
			serializer = getSerializerByFormat();
		}
		
		Assert.notNull(serializer,"serializer must be not null");
		File dirFile = newFile(dir);
		
		if(isDeleteDir()) {
			log.info("delete dir:"+dirFile);
			FileUtils.deleteDirectory(dirFile);
		}
		
		File file = newFile(newFilenameByCompressType());
		log.info("mkdirs dir:"+dirFile);
		file.getParentFile().mkdirs();
		
		log.info("fileoutput:"+file.getPath());
		outputStream = new BufferedOutputStream(CompressUtil.newCompressOutput(compressType,openOutputStream(file)));
		
		
	}

	protected Serializer getSerializerByFormat() {
		Serializer r = SerDesUtil.getSerializerByFormat(format);
		Properties props = getProps();
		if(props != null) {
			BeanUtils.copyProperties(r, props);
		}
		return r;
	}

	private String newFilenameByCompressType() {
		String ext = CompressUtil.getExtionsionByCompressType(compressType);
		if(StringUtils.isNotBlank(compressType) && StringUtils.isBlank(ext)) {
			throw new RuntimeException("not found ext name for compressType:"+compressType);
		}
		return dir + "/" + filename + (ext == null ? "" : "."+ext);
	}

	protected OutputStream openOutputStream(File file) throws FileNotFoundException {
		return new FileOutputStream(file);
	}

	protected File newFile(String path) {
		return new File(path);
	}

	@Override
	public void close() {
		try {
			serializer.flush();
		} catch (IOException e) {
			throw new RuntimeException("flush error",e);
		}
		IOUtils.closeQuietly(outputStream);
	}
	
	@Override
	public void flush() throws IOException {
		serializer.flush();
	}

	@Override
	public void writeObject(Object object) {
		try {
			serializer.serialize(object, outputStream);
		}catch(IOException e) {
			throw new RuntimeException("write error,id:"+getId(),e);
		}
	}
}
