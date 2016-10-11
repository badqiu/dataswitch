package com.github.dataswitch.util;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;

public class IOUtil {

	public static List<String> readLines(BufferedReader reader ,int size) throws IOException {
		List<String> lines = new ArrayList<String>();
		String line = null;
		int count = 1;
		while((line = reader.readLine()) != null) {
			count++;
			lines.add(line);
			if(count > size) {
				break;
			}
		}
		return lines;
	}
	
	public static void writeWithLength(DataOutputStream dos,byte[] buf) throws IOException {
		dos.writeInt(buf.length);
		dos.write(buf);
	}
	
	public static void writeWithLength(OutputStream out,byte[] buf) throws IOException {
		int v = buf.length;
		out.write((v >>> 24) & 0xFF);
	    out.write((v >>> 16) & 0xFF);
	    out.write((v >>>  8) & 0xFF);
	    out.write((v >>>  0) & 0xFF);
	    out.write(buf);
	}
	
	
	public static byte[] javaObject2Bytes(Object obj) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		new ObjectOutputStream(baos).writeObject(obj);
		byte[] buf = baos.toByteArray();
		return buf;
	}
	
	public static byte[] readByLength(DataInputStream dis) throws IOException {
		int length = dis.readInt();
		if(length > 0) {
			byte[] buf = new byte[length];
			dis.read(buf);
			return buf;
		}
		return null;
	}
	
	public static void close(Closeable io) {
		if(io != null) {
			try {
				io.close();
			} catch (IOException e) {
				throw new RuntimeException("close error",e);
			}
		}
	}
	
	public static void closeQuietly(Closeable io) {
		if(io != null) {
			try {
				io.close();
			} catch (IOException e) {
				//ignore
			}
		}
	}
	
}
