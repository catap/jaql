package com.ibm.jaql.benchmark.fs;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

public abstract class MemoryFsNode {
	protected String name;
	protected MemoryFsDirectory parent;
	protected FsPermission permission;
	protected MemoryFileSystem fileSystem;
	
	public abstract boolean delete();
	public abstract boolean isDirectory();
	public abstract boolean isFile();
	public abstract FileStatus getFileStatus();
	
	public MemoryFsNode(String name, MemoryFsDirectory parent, FsPermission permission) {
		this.name = name;
		this.parent = parent;
		this.permission = permission;
	}
	
	public boolean rename(String newName) {
		this.name = newName;
		return true;
	}
	
	protected String getName() {
		return name;
	}

	protected Path constructPath() {
		if(parent != null) {
			return new Path(parent.constructPath(), getName());
		} else {
			return new Path(fileSystem.scheme, fileSystem.authority, getName());
		}
	}
	
	protected void setFileSystem(MemoryFileSystem fs) {
		fileSystem = fs;
	}
	
	@Override
	public String toString() {
		return constructPath().toString();
	}
}
