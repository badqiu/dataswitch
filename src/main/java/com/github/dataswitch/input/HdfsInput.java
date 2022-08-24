package com.github.dataswitch.input;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;

import com.github.dataswitch.util.HadoopConfUtil;
import com.github.rapid.common.hadoop.HdfsFile;

public class HdfsInput extends FileInput{

//	private String hadoopJobUgi = ""; //HDFS 登录帐号，格式为：username,groupname,(groupname...) #password
//	private String hadoopConf; // hadoop-site.xml conf
	private String hdfsUri; // hdfs uri: hdfs://ip:port
	private String hdfsUser; // hdfs ugi: hadoop:hadoop

//	private FileSystem fs;
//	private List<HdfsFile> files;
//	private ReaderInput currentInput;
	
//	@Override
//	public List<Map> read(int size) {
//		try {
//			
//			if(currentInput == null) {
//				if(files.isEmpty()) {
//					return Collections.EMPTY_LIST;
//				}
//				HdfsFile file = files.remove(0);
//				InputStream input = file.open();
//				currentInput = new ReaderInput(input);
//			}
//			
//			List<Map> rows = currentInput.read(size);
//			if(CollectionUtils.isEmpty(rows)) {
//				InputOutputUtil.close(currentInput);
//				currentInput = null;
//				return read(size);
//			}
//			return rows;
//		}catch(Exception e) {
//			throw new RuntimeException(e);
//		}
//	}
//	
//	private void init() throws IOException {
//		fs = getFileSystem();
//		List<File> files = new ArrayList<File>();
//		for(String dir : getDirs()) {
//			HdfsFile file = new HdfsFile(fs, new Path(dir));
//			files.addAll(listAllFiles(file));
//		}
//		
//	}
	
//	@Override
//	protected List<File> listAllFiles() {
//		List<File> files = new ArrayList<File>();
//		for(String dir : getDirs()) {
//			HdfsFile file = new HdfsFile(fs, new Path(dir));
//			files.addAll(listAllFiles(file));
//		}
//		return files;
//	}
	
	@Override
	public void open(Map<String, Object> params) throws Exception {
		super.open(params);
//		init();
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
