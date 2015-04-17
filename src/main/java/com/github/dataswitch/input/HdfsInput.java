package com.github.dataswitch.input;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.duowan.common.hadoop.HdfsFile;

public class HdfsInput extends FileInput{

	private String hadoopJobUgi = ""; //HDFS 登录帐号，格式为：username,groupname,(groupname...) #password
	private String hadoopConf; // hadoop-site.xml conf

//	private FileSystem fs;
//	private boolean isInit = false;
//	private List<HdfsFile> files;
//	private ReaderInput currentInput;
	
//	@Override
//	public List<Map> read(int size) {
//		try {
//			if(!isInit) {
//				isInit = true;
//				init();
//			}
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
	protected File newFile(String dir) {
		return new HdfsFile(fs,dir);
	}
	
	@Override
	protected InputStream openFileInputStream(File currentFile)
			throws FileNotFoundException {
		HdfsFile f = (HdfsFile)currentFile;
		return f.open();
	}
	
	static FileSystem fs = null;
	public static FileSystem getFileSystem() throws IOException {
		if(fs == null) {
			Configuration conf = new Configuration();
	        fs = FileSystem.get(conf);
		}
		return fs;
	}
	
	@Override
	public void close() {
		super.close();
	}
}
