package com.github.dataswitch.output;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.OutputStream;

import org.apache.hadoop.fs.FileSystem;

import com.github.dataswitch.util.HadoopConfUtil;
import com.github.dataswitch.util.hadoop.HdfsFile;

public class HdfsOutput extends FileOutput {

	private String fileType;// seq,txt
	private boolean ignoreKey; // 是否忽略 KEY 值(SEQ 文件格式有效)
	private String hadoopJobUgi = ""; //HDFS 登录帐号，格式为：username,groupname,(groupname...) #password
	private String hadoopConf; // hadoop-site.xml conf

	private String hdfsUri;
	private String hdfsUser;
	private FileSystem fs = null;
	
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
	protected File newFile(String path) {
		try {
			if(fs == null) {
				fs = HadoopConfUtil.getFileSystem(hdfsUri,hdfsUser);
			}
			return new HdfsFile(fs,path);
		}catch(Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	protected OutputStream openOutputStream(File file) throws FileNotFoundException {
		HdfsFile f = (HdfsFile)file;
		return f.create(); //create new file and override
	}

}
