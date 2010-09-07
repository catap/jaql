package com.ibm.jaql.benchmark.fs;

import java.io.IOException;
import java.io.OutputStream;


public class MemoryOutputStream extends OutputStream {
	final MemoryBuffer memoryBuffer;
	final byte[] buffer;
	long length;

	public MemoryOutputStream(MemoryBuffer memBuffer) {
		memoryBuffer = memBuffer;
		buffer = memoryBuffer.getBuffer();
		length = memoryBuffer.getLength();
	}

	@Override
	public void write(int b) throws IOException {
		buffer[(int) length] = (byte) b;
		memoryBuffer.length = ++length;
	}
	
	@Override
    public void write(byte[] b, int off, int len) throws IOException {
		try {
			System.arraycopy(b, off, buffer, (int) length, len);
		} catch (ArrayIndexOutOfBoundsException ex) {
			throw new RuntimeException("Buffer size too small", ex);
		}
		
		length = length + len;
		memoryBuffer.length = length;
    }

}
