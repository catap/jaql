package com.ibm.jaql.benchmark.fs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

public class MemoryFileSystem extends FileSystem {
	protected static MemoryFsDirectory root;
	protected String scheme;
	protected String authority;
	
	Path workingDir;
	URI uri;
	
	public static void delete() {
		if(root != null)
			root.delete();
	}

	@Override
	public FSDataOutputStream append(Path arg0, int arg1, Progressable arg2)
			throws IOException {
		throw new IOException("Appending not supported");
	}
	
	@Override
	public void initialize(URI name, Configuration conf) throws IOException {
		scheme = name.getScheme();
		authority = name.getAuthority();
		uri = URI.create(scheme + "://" + authority);
		if(root == null) {
			root = new MemoryFsDirectory("/", null, FsPermission.getDefault());
			root.setFileSystem(this);
		}
		workingDir = new Path(name.toString()).getParent();
		mkdirs(workingDir);
    }

	public void setRepeat(Path p, int count) {
		Path file = makeAbsolute(p);
		MemoryFsFile f = (MemoryFsFile) root.getNode(file);
		f.setRepeat(count);
	}
	
    private Path makeAbsolute(Path path) {
        if (path.isAbsolute())
            return path;
        return new Path(workingDir, path);
    }

	@Override
	public FSDataOutputStream create(Path file, FsPermission permission,
			boolean overwrite, int bufferSize, short replication,
			long blockSize, Progressable progress) throws IOException {
		file = makeAbsolute(file);
		if (exists(file)) {
            if (overwrite) {
                delete(file);
            } else {
                throw new IOException("File already exists: " + file);
            }
        }
		
        Path parent = file.getParent();
        if (parent != null && !mkdirs(parent)) {
            throw new IOException("Could not create parents" + parent);
        }
        
        MemoryFsDirectory parentDir = (MemoryFsDirectory) root.getNode(parent);
        MemoryFsFile newFile = null;
        
        if(file.getName().startsWith("null-")) {
        	newFile = new MemoryFsFileNullWrite(file.getName(), parentDir);
        } else {
        	newFile =new MemoryFsFile(file.getName(), parentDir, bufferSize, replication,
				permission, blockSize);
        }
        
        parentDir.addNode(newFile);

		return newFile.getOutputStream();
	}

	@Override
	public boolean delete(Path arg0) throws IOException {
		return delete(arg0, true);
	}

	@Override
	/* Will always delete recursive */
	public boolean delete(Path path, boolean recursive) throws IOException {
		path = makeAbsolute(path);
		
		MemoryFsNode node = root.getNode(path);
		if(node != null)
			node.delete();
		
		return true;
	}

	@Override
	public FileStatus getFileStatus(Path path) throws IOException {
		path = makeAbsolute(path);
		Path qualified = path.makeQualified(this);
		MemoryFsNode node = root.getNode(qualified);
		
		if(node == null) {
			throw new FileNotFoundException();
		} else {
			return node.getFileStatus();
		}
	}

	@Override
	public URI getUri() {
		return uri;
	}

	@Override
	public Path getWorkingDirectory() {
		return workingDir;
	}

	@Override
	public FileStatus[] listStatus(Path arg0) throws IOException {
		throw new IOException("This operation is not supported on directories");
	}

	@Override
	public boolean mkdirs(Path path, FsPermission permissions) throws IOException {
		path = makeAbsolute(path);
		
		//Check whether directory already exists
		if(root.getNode(path) != null) {
			return true;
		}
		
		MemoryFsDirectory parent = (MemoryFsDirectory) root.getNode(path.getParent());
		if(parent == null) {
			mkdirs(path.getParent(), permissions);
			parent = (MemoryFsDirectory) root.getNode(path);
		}
		
		MemoryFsDirectory dir = new MemoryFsDirectory(path.getName(), parent, permissions);
		parent.addNode(dir);
		return true;
	}

	@Override
	public FSDataInputStream open(Path path, int bufferSize) throws IOException {
		path = makeAbsolute(path);
		
		MemoryFsNode node = root.getNode(path);
		return ((MemoryFsFile)node).getInputStream(bufferSize);
	}

	@Override
	public boolean rename(Path oldPath, Path newPath) throws IOException {
		oldPath = makeAbsolute(oldPath);
		newPath = makeAbsolute(newPath);
		
		MemoryFsNode node = root.getNode(oldPath);
		//Remove node from old location
		MemoryFsDirectory oldDirectory = (MemoryFsDirectory) root.getNode(oldPath.getParent());
		oldDirectory.removeNode(oldPath.getName());
		
		//Rename and insert into new location
		node.rename(newPath.getName());
		MemoryFsDirectory newDirectory = (MemoryFsDirectory) root.getNode(newPath.getParent());
		newDirectory.addNode(node);
		return true;
	}

	@Override
	public void setWorkingDirectory(Path newWorkingDirectory) {
        workingDir = makeAbsolute(newWorkingDirectory);
    }

	public static void printFsStructure() {
		printDirectory(root);
	}
	
	public static MemoryFsFile getFile(Path p) {
		return (MemoryFsFile) root.getNode(p);
	}
	
	private static void printDirectory(MemoryFsDirectory dir) {
		System.out.println("(d) "+dir);
		Iterator<Entry<String, MemoryFsNode>> it = dir.nodes.entrySet().iterator();
		while(it.hasNext()) {
			Entry<String, MemoryFsNode> entry = it.next();
			MemoryFsNode node = entry.getValue();
			if(node.isFile()) {
				System.out.println("(f) "+node);
			} else {
				printDirectory((MemoryFsDirectory) node);
			}
		}
	}
}
