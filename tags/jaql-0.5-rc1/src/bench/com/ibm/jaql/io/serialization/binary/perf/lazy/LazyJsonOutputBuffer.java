package com.ibm.jaql.io.serialization.binary.perf.lazy;

import java.io.DataOutput;
import java.io.IOException;

public class LazyJsonOutputBuffer implements DataOutput {
	public byte[] buffer = new byte[64 * 1024];
	public int pos = 0;
	
	public void reset() {
		pos = 0;
	}

	@Override
	public void write(int b) throws IOException {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public void write(byte[] b) throws IOException {
		System.arraycopy(b, 0, buffer, pos, b.length);
		pos += b.length;
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		System.arraycopy(b, off, buffer, pos, len);
		pos += len;
	}

	@Override
	public void writeBoolean(boolean v) throws IOException {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public void writeByte(int v) throws IOException {
		buffer[pos++] = (byte) (v & 0xFF);
	}

	@Override
	public void writeBytes(String s) throws IOException {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public void writeChar(int v) throws IOException {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public void writeChars(String s) throws IOException {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public void writeDouble(double v) throws IOException {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public void writeFloat(float v) throws IOException {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public void writeInt(int v) throws IOException {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public void writeLong(long v) throws IOException {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public void writeShort(int v) throws IOException {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public void writeUTF(String s) throws IOException {
		throw new RuntimeException("Not implemented");
	}
}
