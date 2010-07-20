package com.ibm.jaql.benchmark.fs;

import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

public class MemoryFsDirectory extends MemoryFsNode {
	HashMap<String, MemoryFsNode> nodes;
	
	public MemoryFsDirectory(String name, MemoryFsDirectory parent, FsPermission permission) {
		super(name, parent, permission);
		nodes = new HashMap<String, MemoryFsNode>();
	}
	
	public void addNode(MemoryFsNode node) {
		if(nodes.containsKey(node.getName())) {
			throw new RuntimeException("File already exists");
		}
		
		//Make sure that parent is set right
		node.parent = this;
		nodes.put(node.getName(), node);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public boolean delete() {
		if(parent == null) return false;
		
		Object[] arr =nodes.entrySet().toArray();
		for (int i = 0; i < arr.length; i++) {
			((Entry<String, MemoryFsNode>)arr[i]).getValue().delete();
		}
		
		//All Entries should be removed and directory empty
		if(nodes.size() > 1) {
			throw new RuntimeException("Deletion not successfull");
		}
		
		parent.removeNode(getName());
		return true;
	}
	
	protected void removeNode(String name) {
		nodes.remove(name);
	}
	
	@Override
	public FileStatus getFileStatus() {
		return new FileStatus(0, true, (short)1, 1, 1337, constructPath());
	}

	public MemoryFsNode getNode(Path node) {
		//Go to root of node tree
		if(parent != null) {
			return parent.getNode(node);
		} else {
			return getNodeInternal(node);
		}
	}
	
	private MemoryFsNode getNodeInternal(Path node) {
		if(node.equals(constructPath())) {
			return this;
		}
		if(node.getParent().equals(constructPath())) {
			return nodes.get(node.getName());
		} else {
			Path newWorkingPath = node.getParent();
			String name = node.getName();
			Path currentPath = constructPath();
			while(!newWorkingPath.equals(currentPath)) {
				name = newWorkingPath.getName();
				newWorkingPath = newWorkingPath.getParent();
			}
			
			MemoryFsDirectory n = (MemoryFsDirectory) nodes.get(name);
			return n==null?null:n.getNodeInternal(node);
		}
	}

	@Override
	public boolean isDirectory() {
		return true;
	}

	@Override
	public boolean isFile() {
		return false;
	}
}
