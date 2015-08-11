package com.github.dataswitch.input;

import java.io.File;
import java.util.List;

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

	private String dir;
	private String filename;
	private FileOutput bufferedOutput;
	private FileInput bufferedInput;
	
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
	
	@Override
	public List<Object> read(int size) {
		File file = new File(dir,filename);
		if(!file.exists() || file.length() <= 0) {
			System.out.println("create buf,file:"+file);
			bufferedOutput = new FileOutput();
			bufferedOutput.setDir(dir);
			bufferedOutput.setFilename(filename);
			bufferedOutput.setSerializer(new ByteSerializer());
			try {
				InputOutputUtil.copy(getProxy(),bufferedOutput);
			}finally {
				InputOutputUtil.close(getProxy());
				InputOutputUtil.close(bufferedOutput);
			}
		}
		
		if(bufferedInput == null) {
			bufferedInput = new FileInput();
			bufferedInput.setDir(file.getAbsolutePath());;
			bufferedInput.setDeserializer(new ByteDeserializer());
		}
		
		return bufferedInput.read(size);
	}

	@Override
	public void close() {
		IOUtil.closeQuietly(bufferedInput);
		IOUtil.closeQuietly(bufferedOutput);
		IOUtil.closeQuietly(getProxy());
	}
	
}
