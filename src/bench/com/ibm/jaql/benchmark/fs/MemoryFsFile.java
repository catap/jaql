package com.ibm.jaql.benchmark.fs;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.permission.FsPermission;



public class MemoryFsFile extends MemoryFsNode {
	MemoryBuffer buffer;
	int bufferSize;
	short replication;
	FsPermission permission;
	long blockSize;
	int length = 0;
	int repeat = 1;
	
	public MemoryFsFile(String name, MemoryFsDirectory parent, int bufferSize, short replication, FsPermission permission, long blockSize) {
		super(name, parent, permission);
		this.bufferSize = bufferSize;
		this.replication = replication;
		this.blockSize = blockSize;
	}

	@Override
	public FileStatus getFileStatus() {
		long len = this.length;
		if(buffer != null && repeat == 1) {
			len = buffer.getLength();
		} else if (buffer != null) {
			//len = FsUtil.getSequenceHeaderSize(FsUtil.getType(constructPath())) + (buffer.getLength() - FsUtil.getSequenceHeaderSize(FsUtil.getType(constructPath()))) * repeat;
			len = buffer.getLength();
		}
		return new FileStatus(len, false, replication, blockSize, 1337, constructPath());
	}

	public FSDataInputStream getInputStream(int bufferSize) {
		try {
			//return new FSDataInputStream(new MemoryInputStream(buffer, repeat, FsUtil.getSequenceHeaderSize(FsUtil.getType(constructPath()))));
			return new FSDataInputStream(new MemoryInputStream(buffer, FsUtil.getSequenceHeaderSize(FsUtil.getType(constructPath()))));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	public FSDataOutputStream getOutputStream() {
		if(buffer == null) {
			buffer = new MemoryBuffer();
		} else {
			throw new RuntimeException("Changing files is not supported");
		}
		try {
			return new FSDataOutputStream(new MemoryOutputStream(buffer));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	public void setRepeat(int count) {
		if(buffer == null || buffer.length == 0) {
			throw new RuntimeException("File needs to have contents before the repeatflag can be set");
		}
		
		repeat = count;
	}

	@Override
	public boolean delete() {
		//TODO: Delete Buffer ...
		parent.removeNode(getName());
		return true;
	}

	@Override
	public boolean isDirectory() {
		return false;
	}

	@Override
	public boolean isFile() {
		return true;
	}
}

