package com.ibm.jaql.io.serialization.binary.perf.lazy;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public final class LazyJsonInputBuffer implements DataInput {
	protected int length = -1;
	protected int offset = -1; //Offset in the buffer
	private int pos = -1; //Current reading position changes during byte reads etc
	private byte[] buffer;
	
	public void setBuffer(byte[] buffer, int offset, int length) {
		this.buffer = buffer;
		this.offset = offset;
		this.length = length;
		this.pos = 0;
	}
	
	@Override
	public boolean readBoolean() throws IOException {
		throw new RuntimeException("not implemented");
	}

	@Override
	public byte readByte() throws IOException {
		return buffer[offset+pos++];
	}

	@Override
	public char readChar() throws IOException {
		throw new RuntimeException("not implemented");
	}

	@Override
	public double readDouble() throws IOException {
		throw new RuntimeException("not implemented");
	}

	@Override
	public float readFloat() throws IOException {
		throw new RuntimeException("not implemented");
	}

	@Override
	public void readFully(byte[] target) throws IOException {
		System.arraycopy(buffer, offset+pos, target, 0, target.length);
		pos += target.length;
	}

	@Override
	public void readFully(byte[] target, int offset, int length) throws IOException {
		System.arraycopy(buffer, offset+pos, target, 0, length);
		pos += length;
	}

	@Override
	public int readInt() throws IOException {
		throw new RuntimeException("not implemented");
	}

	@Override
	public String readLine() throws IOException {
		throw new RuntimeException("not implemented");
	}

	@Override
	public long readLong() throws IOException {
		throw new RuntimeException("not implemented");
	}

	@Override
	public short readShort() throws IOException {
		throw new RuntimeException("not implemented");
	}

	@Override
	public String readUTF() throws IOException {
		throw new RuntimeException("not implemented");
	}

	@Override
	public int readUnsignedByte() throws IOException {
		return buffer[offset+pos++] & 0xFF;
	}

	@Override
	public int readUnsignedShort() throws IOException {
		throw new RuntimeException("not implemented");
	}

	@Override
	public int skipBytes(int arg0) throws IOException {
		throw new RuntimeException("not implemented");
	}

	public byte[] copyBufferContent() {
		byte[] bytes = new byte[length];
		System.arraycopy(buffer, offset, bytes, 0, length);
		return bytes;
	}
	
	public LazyJsonInputBuffer readBuffer(LazyJsonInputBuffer subBuffer, int len) {
		subBuffer.setBuffer(buffer, offset+pos, len);
		pos+=len;
		return subBuffer;
	}

	public LazyJsonInputBuffer getCopy() {
		LazyJsonInputBuffer copy = new LazyJsonInputBuffer();
		copy.buffer = this.buffer;
		copy.length = this.length;
		copy.pos = this.pos;
		copy.offset = this.offset;

		return copy;
	}

	public void writeToOutput(DataOutput out) throws IOException {
		out.write(buffer, offset, length);
	}
	
	//TODO: asserts for boundary checks
}
