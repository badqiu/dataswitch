package com.github.dataswitch.input;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;

import com.github.dataswitch.util.HadoopConfUtil;
import com.github.dataswitch.util.hadoop.HdfsFile;

public class HdfsInput extends FileInput{

	private String hdfsUri; // hdfs uri: hdfs://ip:port
	private String hdfsUser; // hdfs ugi: hadoop:hadoop
	
	@Override
	public void open(Map<String, Object> params) throws Exception {
		super.open(params);
	}
	
	private FileSystem fs = null;
	@Override
	protected File newFile(String path) {
		try {
			if(fs == null) {
				fs = HadoopConfUtil.getFileSystem(hdfsUri,hdfsUser);
			}
			return new HdfsFile(fs,path);
		} catch (IOException e) {
			throw new RuntimeException("newFile error,path:"+path,e);
		}
	}
	
	public String getHdfsUri() {
		return hdfsUri;
	}

	public void setHdfsUri(String hdfsUri) {
		this.hdfsUri = hdfsUri;
	}

	public String getHdfsUser() {
		return hdfsUser;
	}

	public void setHdfsUser(String hdfsUser) {
		this.hdfsUser = hdfsUser;
	}

	@Override
	protected InputStream openFileInputStream(File currentFile)
			throws FileNotFoundException {
		HdfsFile f = (HdfsFile)currentFile;
		return f.open();
	}
	
	@Override
	public void close() {
		super.close();
	}
}
