package com.ibm.jaql.lang.core;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;

import com.ibm.jaql.json.parser.JsonParser;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonValue;

public class DefaultModule extends Module {
	private final File moduleDir;
	
	private static final FilenameFilter jarFilter = new FilenameFilter() {
		@Override
		public boolean accept(File dir, String name) {
			if(name.toLowerCase().endsWith(".jar")) {
				return true;
			}
			return false;
		}
	};

	   private static final FilenameFilter jaqlFileFilter = new FilenameFilter() {
	        @Override
	        public boolean accept(File dir, String name) 
	        {
	          name = name.toLowerCase();
	          return name.endsWith(".jaql") || name.endsWith(".jql");
	        }
	    };

	public DefaultModule(File dir) {
		if(!isModuleDirectory(dir)) {
			throw new RuntimeException("Directory is not a module directory " + dir);
		}
		moduleDir = dir;
	}
	
	/* (non-Javadoc)
	 * @see com.ibm.jaql.lang.core.IModule#getJaqlFiles()
	 */
	public File[] getJaqlFiles() {
		File jaqlDir = new File(moduleDir, JAQL_DIRECTORY);
		if(!jaqlDir.exists()) {
			return new File[0];
		}
		if(!jaqlDir.canRead()) {
			throw new RuntimeException("Cannot read jaql directory " + jaqlDir);
		}
		
		//Create file list
		ensureMetaData();
		if(!meta.containsKey(SCRIPTS_FIELD)) {
			// All *.jql and *.jaql files inside the jaql directory are assumed to be jaql
		  File[] fs = jaqlDir.listFiles(jaqlFileFilter);
		  Arrays.sort(fs);
			return fs;
		} else {
			//Load files based on meta data
			ArrayList<File> files = new ArrayList<File>();
			JsonArray names = (JsonArray) meta.get(SCRIPTS_FIELD);
			for (JsonValue name : names) {
				File f = new File(jaqlDir, name.toString());
				if(!f.exists()) {
					throw new RuntimeException("File " + f + " is missing");
				}
				files.add(f);
			}
			return files.toArray(new File[0]);
		}
	}
	
	/* (non-Javadoc)
	 * @see com.ibm.jaql.lang.core.IModule#getJarFiles()
	 */
	public File[] getJarFiles() {
		File jarDir = new File(moduleDir, JAR_DIRECTORY);
		if(!jarDir.exists()) {
			return new File[0];
		}
		else if(!jarDir.canRead()) {
			throw new RuntimeException("Cannot read jar directory " + jarDir);
		}
		
		return jarDir.listFiles(jarFilter);
	}
	
	/* (non-Javadoc)
	 * @see com.ibm.jaql.lang.core.IModule#getExampleFiles()
	 */
	public File[] getExampleFiles() {
		File exampleDir = new File(moduleDir, EXAMPLE_DIRECTORY);
		if(!exampleDir.exists()) {
			return new File[0];
		}
		else if(!exampleDir.canRead()) {
			throw new RuntimeException("Cannot read example directory " + exampleDir);
		}
		
		return exampleDir.listFiles(jaqlFileFilter);
	}
	
	/* (non-Javadoc)
	 * @see com.ibm.jaql.lang.core.IModule#getTestDirectory()
	 */
	public File getTestDirectory() {
		return new File(moduleDir, TEST_DIRECTORY);
	}
	
	/* (non-Javadoc)
	 * @see com.ibm.jaql.lang.core.IModule#getTests()
	 */
	public String[] getTests() {
		File testDir = new File(moduleDir, TEST_DIRECTORY);
		if(!testDir.exists()) {
			return new String[0];
		}
		else if(!testDir.canRead()) {
			throw new RuntimeException("Cannot read test directory " + testDir);
		}
		
		FilenameFilter f = new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith("Queries.txt");
			}
		};
		File[] files = testDir.listFiles(f);
		String[] tests = new String[files.length];
		for (int i = 0; i < tests.length; i++) {
			tests[i] = files[i].getName().replace("Queries.txt", "");
		}
		
		return tests;
	}
	
	/* (non-Javadoc)
	 * @see com.ibm.jaql.lang.core.IModule#getModuleDescription()
	 */
	public JsonRecord getModuleDescription() {
		File f = new File(moduleDir, MODULE_FILE);
		if(!f.exists()) {
			return null;
		} else {
			InputStream in = null;
			try {
				in = new FileInputStream(f);
				
				JsonParser p = new JsonParser(in);
		  	JsonValue data = p.JsonVal();
		  	//TODO: use p.Record??
		  	
		  	if(data != null) {
		  		JsonRecord description = (JsonRecord) data;
		  		if(!descriptionSchema.matches(description)) {
							throw new RuntimeException("Description file does not match schema");
		  		} else {
		  			return description;
		  		}
		  	}
			} 
			catch (Exception e) {
				throw new RuntimeException("Error while loading description file", e);
			} 

			
		}
		return null;
	}

	@Override
	protected InputStream getMetaDataStream() {
		//Read meta data
  	File f = new File(moduleDir, NAMESPACE_FILE);
  	if(!f.exists()) {
  		return null;
  	}
  	
  	try {
			return new FileInputStream(f);
		} catch (FileNotFoundException e) { throw new RuntimeException("Will never happen");}
	}
}

