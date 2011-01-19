package com.ibm.jaql.benchmark.fs;

public class MemoryBuffer {
	//TODO: Configurable by property
	private final byte[] buffer = new byte[256*1024*1024];
	//private boolean locked;
	protected long length;
	
	public MemoryBuffer() {
		length = 0;
		//locked = false;
	}
	
	public byte[] getBuffer() {
		//locked = true;
		return buffer;
	}
	
	/*
	 * length is set directly because of performance reasons
	 */
	
	public long getLength() {
		return length;
	}
}
