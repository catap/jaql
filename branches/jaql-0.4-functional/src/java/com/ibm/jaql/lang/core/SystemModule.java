package com.ibm.jaql.lang.core;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.util.ClassLoaderMgr;

public class SystemModule extends Module {

	public SystemModule() {
	}

	@Override
	protected InputStream getMetaDataStream() {
		URL file = ClassLoaderMgr.getResource("conf/system.json");
		if(file == null) {
			file = ClassLoaderMgr.getResource("../conf/system.json");
		}
		try {
			return file.openStream();
		} catch (IOException e) {
			throw new RuntimeException("Could not read system namespace ", e);
		}
	}

	@Override
	public File[] getExampleFiles() {
		return new File[0];
	}

	@Override
	public File[] getJaqlFiles() {
		return new File[0];
	}

	@Override
	public File[] getJarFiles() {
		return new File[0];
	}

	@Override
	public JsonRecord getModuleDescription() {
		return null;
	}

	@Override
	public File getTestDirectory() {
		return null;
	}

	@Override
	public String[] getTests() {
		return new String[0];
	}
	
	@Override
	public boolean isSystemModule() {
		return true;
	}
}
