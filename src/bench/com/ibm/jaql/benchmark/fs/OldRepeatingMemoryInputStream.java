package com.ibm.jaql.benchmark.fs;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

public final class OldRepeatingMemoryInputStream extends InputStream implements PositionedReadable, Seekable {
	final MemoryBuffer memoryBuffer;
	final byte[] buffer;
	long length;
	long bufferLength;
	long pos;
	final int repeat;
	final long sequenceHeaderSize;
	
	
	public OldRepeatingMemoryInputStream(MemoryBuffer memBuffer, int repeat, long headerSize) {
		sequenceHeaderSize = headerSize;
		memoryBuffer = memBuffer;
		this.repeat = repeat;
		buffer = memoryBuffer.getBuffer();
		bufferLength = memBuffer.getLength();
		length = sequenceHeaderSize + (bufferLength - sequenceHeaderSize) * repeat;
		pos = 0;
	}

	@Override
    public synchronized int available() throws IOException {
        return (int) (length - pos);
    }

	@Override
	public int read() {
		byte[] b = new byte[1];
		//int count = read(b, 0, 1);
		int count = 0;
		try {
			count = read(pos, b, 0, 1);
			pos += count;
		} catch (IOException e) {
			throw new RuntimeException("Internal Error", e);
		}
		if(count == 1) {
			return ((int) (b[0] & 0xff));
		}
		
		return -1;
	}
	
	@Override
	public int read(byte[] b, int off, int len) {
		int count = 0;
		try {
			count = read(pos, b, off, len);
			pos += count;
		} catch (IOException e) {
			throw new RuntimeException("Internal Error", e);
		}
		
		return count;
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
		/* Does no checks for bounding errors because thats done on an upper level */
		//len = (int) Math.min(len, (length-position));
		if(position >= bufferLength) {
			final int pos = (int) (sequenceHeaderSize + ((position - sequenceHeaderSize) % (bufferLength - sequenceHeaderSize)));
			//final int pos = (int) (position % bufferLength) + MemoryFsFile.HEADER_SIZE;
			System.arraycopy(buffer, pos, b, offset, len);
		} else {
			System.arraycopy(buffer, (int) (position % bufferLength), b, offset, len);
		}
		return len;
	}

	@Override
	public void readFully(long position, byte[] buffer) throws IOException {
		readFully(position, buffer, 0, buffer.length);
	}

	@Override
	public void readFully(long position, byte[] buffer, int offset, int len) 
			throws IOException {
		if(len > (length - position)) {
			throw new IOException("Not enough bytes available");
		}
		read(position, buffer, offset, len);
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
