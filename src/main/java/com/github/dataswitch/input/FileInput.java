package com.github.dataswitch.input;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.serializer.Deserializer;
import org.springframework.util.AntPathMatcher;
import org.springframework.util.Assert;
import org.springframework.util.ResourceUtils;
import org.springframework.util.StringUtils;

import com.github.dataswitch.serializer.SerDesUtil;
import com.github.dataswitch.util.BeanUtils;
import com.github.dataswitch.util.CompressUtil;

public class FileInput extends BaseInput implements Input{

	private static Logger log = LoggerFactory.getLogger(FileInput.class);
	
	/**
	 * 数据输入的目录
	 */
	private List<String> dirs;

	private Deserializer deserializer = null;
	
	
	/**
	 * 检查是否有文件 ，不存在则报错
	 */
	private boolean errorOnEmptyFile = true;
	
	private String include;
	private String exclude;
	
	private String format;
	
	private transient AntPathMatcher antPathMatcher = new AntPathMatcher();
	
	private transient InputStream inputStream;
	
	private List<File> _files = null;
	public List<String> getDirs() {
		return dirs;
	}

	public void setDirs(List<String> dirs) {
		this.dirs = dirs;
	}
	
	public void setDir(String dir) {
		String[] dirArray = StringUtils.tokenizeToStringArray(dir, "\t\n,;");
		setDirs(Arrays.asList(dirArray));
	}
	
	public Deserializer getDeserializer() {
		return deserializer;
	}

	public void setDeserializer(Deserializer deserializer) {
		this.deserializer = deserializer;
	}
	
	public String getInclude() {
		return include;
	}

	public void setInclude(String include) {
		this.include = include;
	}

	public String getExclude() {
		return exclude;
	}

	public void setExclude(String exclude) {
		this.exclude = exclude;
	}
	
	public boolean isErrorOnEmptyFile() {
		return errorOnEmptyFile;
	}

	public void setErrorOnEmptyFile(boolean errorOnEmptyFile) {
		this.errorOnEmptyFile = errorOnEmptyFile;
	}
	
	public String getFormat() {
		return format;
	}

	public void setFormat(String format) {
		this.format = format;
	}

	@Override
	public Object readObject() {
		try {
			
			if(inputStream == null) {
				if(CollectionUtils.isEmpty(_files)) {
					return null;
				}
				
				File currentFile = _files.remove(0);
				log.info("read from file:"+currentFile);
				inputStream = new BufferedInputStream(CompressUtil.newDecompressInputByExt(openFileInputStream(currentFile),currentFile.getName()));
			}
			
			Object object = deserializer.deserialize(inputStream);
			if(object == null) {
				IOUtils.closeQuietly(inputStream);
				inputStream = null;
				return readObject();
			}
			return object;
		}catch(IOException e) {
			throw new RuntimeException("read error,id:"+getId()+" inputDirs:"+dirs,e);
		}
	}
	
	@Override
	public void open(Map<String, Object> params) throws Exception {
		super.open(params);
		init();
	}

	private void init() {
		if(deserializer == null) {
			deserializer = getDeserializerByFormat();
		}
		Assert.notNull(deserializer,"deserializer must be not null");
		this._files = listAllFiles();
		if(CollectionUtils.isEmpty(this._files)) {
			log.warn("not found any file by dirs:"+getDirs());
		}
		
		if(errorOnEmptyFile) {
			Assert.notEmpty(this._files,"not found any file by dirs:"+getDirs());
		}
	}

	protected Deserializer getDeserializerByFormat() {
		Deserializer r = SerDesUtil.getDeserializerByFormat(format);
		Properties props = getProps();
		if(props != null) {
			BeanUtils.copyProperties(r, props);
		}
		return r;
	}

	protected InputStream openFileInputStream(File currentFile)throws FileNotFoundException {
		return new FileInputStream(currentFile);
	}
	
	private List<File> listAllFiles() {
		return listAllFiles(dirs);
	}

	@Override
	public void close() {
		IOUtils.closeQuietly(inputStream);
	}
	
	public List<File> listAllFiles(List<String> dirs) {
		Assert.notEmpty(dirs,"'dirs' must be not empty");
		List<File> files = new LinkedList<File>();
		for(String dir : dirs) {
			List tempFiles = listAllFiles(newFile(dir));
			files.addAll(tempFiles);
		}
		return filterByIncludeAndExclude(files);
	}
	
	List<File> filterByIncludeAndExclude(List<File> files) {
		if(org.apache.commons.lang.StringUtils.isBlank(include) && org.apache.commons.lang.StringUtils.isBlank(exclude) ) {
			return files;
		}
		
		List<File> result = new ArrayList<File>();
		for(File file : files) {
			if(org.apache.commons.lang.StringUtils.isNotBlank(exclude) &&  antPathMatcher.match(exclude, file.getName())) {
				continue;
			}
			
			if(org.apache.commons.lang.StringUtils.isBlank(include) || antPathMatcher.match(include,file.getName())) {
				result.add(file);
			}
		}
		return result;
	}

	protected File newFile(String dir) {
		try {
			return ResourceUtils.getFile(dir);
		} catch (FileNotFoundException e) {
			return new File(dir);
		}
	}

	@SuppressWarnings("unchecked")
	public static List<File> listAllFiles(File dir) {
		Collection<File> files = null;
		if(dir.isDirectory()) {
			files =  FileUtils.listFiles(dir,null,true);
		}else {
			files = Arrays.asList(dir);
		}
		
		List<File> result = new ArrayList<File>();
		for(File f : files) {
			if(f.isDirectory()) {
				continue;
			}
			if(f.isHidden()) {
				continue;
			}
			if(f.getName().startsWith("_")) {
				continue;
			}
			if(f.getName().startsWith(".")) {
				continue;
			}
			if(f.getPath().contains(".svn")) {
				continue;
			}
			if(f.length() <= 0) {
				continue;
			}
			if(f.isFile()) {
				result.add(f);
			}
		}
		return result;
	}
	
	
}
