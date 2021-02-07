package com.github.dataswitch.output;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import com.github.dataswitch.serializer.Serializer;
import com.github.dataswitch.util.CompressUtil;


public class FileOutput extends BaseOutput implements Output {

	private static Logger log = LoggerFactory.getLogger(FileOutput.class);
	
	private String dir; //文件存储目录
	private int maxFileSize; //最大文件大小
	private String compressType; // 压缩类型: gzip, snappy,bzip2
	private boolean deleteDir = false; //是否写入之前，删除目录
	private String filename = "fileoutput_0000_";
	
	private transient  Serializer serializer;
	private transient  boolean isInit = false;
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

	public Serializer getSerializer() {
		return serializer;
	}

	public void setSerializer(Serializer serializer) {
		this.serializer = serializer;
	}
	
	public void init() throws IOException {
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
			if(!isInit) {
				isInit = true;
				init();
			}
			
			serializer.serialize(object, outputStream);
		}catch(IOException e) {
			throw new RuntimeException("write error,id:"+getId(),e);
		}
	}
}
