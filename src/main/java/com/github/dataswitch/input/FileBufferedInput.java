package com.github.dataswitch.input;

import java.io.File;
import java.util.List;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
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
	private boolean deleteFileOnClose = true;
	private File _tempFile = null;
	
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
	
	public boolean isDeleteFileOnClose() {
		return deleteFileOnClose;
	}

	public void setDeleteFileOnClose(boolean deleteFileOnClose) {
		this.deleteFileOnClose = deleteFileOnClose;
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
		_tempFile = new File(dir,finalFilename());
		if(!_tempFile.exists() || _tempFile.length() <= 0) {
			saveIntoBufferdFile(_tempFile);
		}
		
		if(_bufferedInput == null) {
			_bufferedInput = new FileInput();
			_bufferedInput.setDir(_tempFile.getAbsolutePath());;
			_bufferedInput.setDeserializer(new ByteDeserializer());
		}
		
		return _bufferedInput.read(size);
	}

	private void saveIntoBufferdFile(File file) {
		logger.info("create buf file:"+file);
		
		FileOutput bufferedOutput = new FileOutput();
		bufferedOutput.setDir(file.getParentFile().getAbsolutePath());
		bufferedOutput.setFilename(file.getName());
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
		
		if(deleteFileOnClose) {
			logger.info("delete temp buffer file:"+_tempFile);
			FileUtils.deleteQuietly(_tempFile);
		}
	}
	
}
