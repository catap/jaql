package com.ibm.jaql.benchmark.fs;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

/*
 * Does not do any constraint checks
 */
public final class MemoryInputStream extends InputStream implements PositionedReadable, Seekable {
	final MemoryBuffer memoryBuffer;
	final byte[] buffer;
	long length;
	long bufferLength;
	int pos;
	final long sequenceHeaderSize;
	
	
	public MemoryInputStream(MemoryBuffer memBuffer, long headerSize) {
		sequenceHeaderSize = headerSize;
		memoryBuffer = memBuffer;
		buffer = memoryBuffer.getBuffer();
		bufferLength = memBuffer.getLength();
		length = bufferLength;
		pos = 0;
	}

	@Override
    public synchronized int available() throws IOException {
        return (int) (length - pos);
    }

	@Override
	public int read() {
		return ((int) (buffer[pos++] & 0xff));
	}
	
	@Override
	public int read(byte[] b, int off, int len) {
		System.arraycopy(buffer, pos, b, off, len);
		pos += len;
		return len;
	}
	
    @Override
    public boolean markSupported() {
        return false;
    }
    
    @Override
    public void mark(int readLimit) {
        //Not supported
    }
    
    @Override
    public void reset() throws IOException {
        throw new IOException("Not supported");
    }

	@Override
	public final int read(long position, byte[] b, int offset, int len) 
			throws IOException {
		System.arraycopy(buffer, (int) position, b, offset, len);
		return len;
	}

	@Override
	public void readFully(long position, byte[] b) throws IOException {
		System.arraycopy(buffer, (int) position, b, 0, b.length);
	}

	@Override
	public void readFully(long position, byte[] b, int offset, int len) 
			throws IOException {
		System.arraycopy(buffer, (int) position, b, offset, len);
	}

	@Override
	public long getPos() throws IOException {
		return pos;
	}

	@Override
	public void seek(long pos) throws IOException {
		if(pos >= length) {
			throw new IOException("File is to small");
		}
		this.pos = (int) pos;
	}

	@Override
	public boolean seekToNewSource(long arg0) throws IOException {
		return false;
	}

}
