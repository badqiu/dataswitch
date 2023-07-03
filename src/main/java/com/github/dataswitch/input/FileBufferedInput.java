package com.github.dataswitch.input;

import java.io.File;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dataswitch.output.FileOutput;
import com.github.dataswitch.serializer.ByteDeserializer;
import com.github.dataswitch.serializer.ByteSerializer;
import com.github.dataswitch.util.IOUtil;
import com.github.dataswitch.util.InputOutputUtil;
/**
 * 提供中间缓存数据的buffer
 * @author badqiu
 */
public class FileBufferedInput extends ProxyInput{

	private static Logger logger = LoggerFactory.getLogger(FileBufferedInput.class);
	
	private String dir;
	private String filename;
	
	private FileInput _bufferedInput;
	
	private boolean randomFilename = false;
	
	public FileBufferedInput() {
	}
	
	public FileBufferedInput(Input proxy) {
		super(proxy);
	}
	
	public String getDir() {
		return dir;
	}
	
	public void setDir(String baseDir) {
		this.dir = baseDir;
	}
	
	public String getFilename() {
		return filename;
	}
	
	public void setFilename(String filename) {
		this.filename = filename;
	}
	
	public boolean isRandomFilename() {
		return randomFilename;
	}

	public void setRandomFilename(boolean randomFilename) {
		this.randomFilename = randomFilename;
	}

	public String finalFilename() {
		if(StringUtils.isNotBlank(filename)) {
			return filename;
		}
		if(randomFilename) {
			this.filename = "buffer_"+UUID.randomUUID().toString();
		}
		
		return this.filename;
	}
	
	@Override
	public List<Object> read(int size) {
		File file = new File(dir,finalFilename());
		if(!file.exists() || file.length() <= 0) {
			saveIntoBufferdFile(file);
		}
		
		if(_bufferedInput == null) {
			_bufferedInput = new FileInput();
			_bufferedInput.setDir(file.getAbsolutePath());;
			_bufferedInput.setDeserializer(new ByteDeserializer());
		}
		
		return _bufferedInput.read(size);
	}

	private void saveIntoBufferdFile(File file) {
		logger.info("create buf file:"+file);
		FileOutput bufferedOutput = new FileOutput();
		bufferedOutput.setDir(dir);
		bufferedOutput.setFilename(finalFilename());
		bufferedOutput.setSerializer(new ByteSerializer());
		Input proxy = getProxy();
		try {
			InputOutputUtil.copy(proxy,bufferedOutput);
		}finally {
			InputOutputUtil.close(proxy);
			InputOutputUtil.close(bufferedOutput);
		}
	}

	@Override
	public void close() {
		IOUtil.closeQuietly(_bufferedInput);
		IOUtil.closeQuietly(getProxy());
	}
	
}
