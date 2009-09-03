package com.ibm.jaql.lang.core;

import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import com.ibm.jaql.json.parser.JsonParser;
import com.ibm.jaql.json.parser.ParseException;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.BuiltInExpr;
import com.ibm.jaql.lang.expr.function.BuiltInFunction;
import com.ibm.jaql.lang.expr.function.BuiltInFunctionDescriptor;
import com.ibm.jaql.lang.expr.function.JavaUdfExpr;
import com.ibm.jaql.util.ClassLoaderMgr;

public abstract class Module {
	public static final String SYSTEM_MODULE = "system";
	protected static final String JAQL_DIRECTORY = "scripts";
	protected static final String JAR_DIRECTORY = "jars";
	protected static final String EXAMPLE_DIRECTORY = "examples";
	protected static final String TEST_DIRECTORY = "tests";
	protected static final String NAMESPACE_FILE = "namespace.json";
	protected static final String MODULE_FILE = "description.json";
	protected static final JsonString UDF_FN_FIELD = new JsonString("functions");
	protected static final JsonString JAQL_FIELD = new JsonString("scripts");
	protected static final JsonString EXPORT_FIELD = new JsonString("export");
	protected static final JsonString VERSION_FIELD = new JsonString("version");
	protected static final JsonString IMPORT_FIELD = new JsonString("import");
	protected static String[] searchPath = null;
	protected static Schema namespaceSchema = null;
	protected static Schema descriptionSchema = null;
	protected static Schema javaUDFSchema = null;
	protected static Schema builtinUDFSchema = null;
	
	public static void initSchemas() {
		try {
			Module.namespaceSchema = SchemaFactory.parse( 
					"{" +
					VERSION_FIELD + ": decfloat(value=0.1m)," + "\n" +
					JAQL_FIELD + "?: [ string<0,*> ]," +"\n" +
					EXPORT_FIELD + "?: [ string<0,*> ]," +"\n" +
					UDF_FN_FIELD + "?: [ { \"type\"?: string, * }<0,*> ], " + "\n" +
					IMPORT_FIELD + "?: [ {\"module\": string, \"vars\": [ string<0,*> ] }|string<0,*> ] " + "\n" +
			    "}");
			
			Module.descriptionSchema = SchemaFactory.parse( 
					"{" +
					VERSION_FIELD + "?: decfloat," +
					"date?: date," +
					"title?: string," +
					"author?: string | [ string<0,*> ]," +
					"maintainer?: string | [ string<0,*> ]," +
					"description?: string," +
					"license?: string," +
					"url?: string" +
					"}");
			
			Module.javaUDFSchema = SchemaFactory.parse("{type?: string(value=\"javaudf\"), " +
					"\"class\": string, " +
					"\"name\": string }");
			
			Module.builtinUDFSchema = SchemaFactory.parse("{type: string(value=\"builtin\"), " +
					"\"class\": string }");
			
		} catch (IOException e) {
		}
	}
	
	public static void setSearchPath(String[] searchPath) {
		Module.searchPath = searchPath; 
	}	
	
	public static Module findModule(String name) {
		if(name.equals(Module.SYSTEM_MODULE)) {
			return new SystemModule();
		}
		
		FileFilter filter = new FileFilter() {
			@Override
			public boolean accept(File dir) {
				if(dir.isDirectory()) {
					return isModuleDirectory(dir);
				}
				return false;
			}
		};
		
		for (String dir : searchPath) {
			File d = new File(dir);
			if(!d.isDirectory()) {
				throw new RuntimeException(d.getAbsolutePath() + " is no directory");
			}
			if(isModuleDirectory(d)) {
				if(dir.equals(name)) {
					return new DefaultModule(d);
				}
			}
			else {
				File[] modules = d.listFiles(filter);
				for (File module : modules) {
					if(name.equals(module.getName())) {
						return new DefaultModule(module);
					}
				}
			}
		}
		
		throw new RuntimeException("Could not find module " + name);
	}
	
	public static boolean isModuleDirectory(final File f) {
		if(!f.isDirectory()) {
			return false;
		}
		
		FilenameFilter moduleFilter = new FilenameFilter() {
			@Override
			public boolean accept(File directory, String entry) {
				if(JAQL_DIRECTORY.equals(entry)) {
					return true;
				}
				if(NAMESPACE_FILE.equals(entry)) {
					return true;
				}
				return false;
			}
		};
		
		if(f.list(moduleFilter).length > 0) {
			return true;
		}
		
		return false;
	}
	
	protected JsonRecord parseMetaData(InputStream in) {
		JsonRecord meta = new BufferedJsonRecord();
  	
		JsonParser p = new JsonParser(in);
  	JsonValue data = null;
		try {
			data = p.JsonVal();
			//if(p.JsonVal() != null) {
			//	throw new RuntimeException("Only one record allowed in namespace file");
			//}
		} catch (ParseException e) {
			throw new RuntimeException("Error in metadata file", e);
		}
  	if(data != null) {
  		meta = (JsonRecord) data;
  		//Do schema check
			try {
				if(!Module.namespaceSchema.matches(meta)) {
					//Do some minimal analysis to see whats wrong.
					if(!meta.containsKey(VERSION_FIELD)) {
						throw new RuntimeException("Namespace files is missing version");
					} else {
						throw new RuntimeException("Namespace file does not match schema");
					}
				}
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
  	} 
  	
  	return meta;
	}
	
	protected JsonRecord meta = null;
	
	protected void ensureMetaData(){
		initSchemas();
		if(meta != null) {
			return;
		}
		
		meta = parseMetaData(getMetaDataStream());
	}
	
	/* (non-Javadoc)
	 * @see com.ibm.jaql.lang.core.IModule#getUDFs()
	 */
	public HashMap<String, Expr> getUDFs() throws Exception {
		ensureMetaData();
		HashMap<String, Expr> udfs = new HashMap<String, Expr>();
		
		//When no udf's are declared just return the emtpy set
		if(!meta.containsKey(UDF_FN_FIELD)) {
			return udfs;
		}
		
		JsonArray entries = (JsonArray) meta.get(UDF_FN_FIELD);
		for (JsonValue entry : entries) {
			JsonRecord udf = (JsonRecord) entry;
			String name = null;
			Expr e = null;
			//Figure out which type of udf it is
			if(builtinUDFSchema.matches(udf)) {
				String cls = udf.get(new JsonString("class")).toString();
				Class<? extends Expr> c = (Class<? extends Expr>) ClassLoaderMgr.resolveClass(cls);
				BuiltInFunctionDescriptor d = BuiltInFunction.getDescriptor(c);
				JsonString descriptorClass = new JsonString(d.getClass().getName());
				name = d.getName();
				e = new BuiltInExpr(new ConstExpr(descriptorClass));
			}
			else if(javaUDFSchema.matches(udf)) {
				JsonValue cls = udf.get(new JsonString("class"));
				name = udf.get(new JsonString("name")).toString();
				e = new JavaUdfExpr(new ConstExpr(cls));
			}
			else {
				throw new RuntimeException("Invalid udf function list entry " + udf.toString());
			}
			
			//Do sanity check, every function name should be in the list only once
			if(udfs.containsKey(name)) {
				throw new RuntimeException("Duplicate entry for udf " + name);
			}
			
			udfs.put(name, e);
		}
		
		return udfs;
	}
	
	/* (non-Javadoc)
	 * @see com.ibm.jaql.lang.core.IModule#getNamespaceDescription()
	 */
	public JsonRecord getNamespaceDescription() {
		ensureMetaData();
		return meta;
	}
	
	/* (non-Javadoc)
	 * @see com.ibm.jaql.lang.core.IModule#getExportedFunctions()
	 */
	public HashSet<String> getExportedFunctions() {
		ensureMetaData();
		if(!meta.containsKey(EXPORT_FIELD)) {
			return null;
		}
		else {
			HashSet<String> ids = new HashSet<String>();
			JsonArray values = (JsonArray) meta.get(EXPORT_FIELD);
			for (JsonValue value : values) {
				ids.add(value.toString());
			}
			return ids;
		}
	}
	
	/* (non-Javadoc)
	 * @see com.ibm.jaql.lang.core.IModule#loadImports(com.ibm.jaql.lang.core.NamespaceEnv)
	 */
	public void loadImports(NamespaceEnv namespace) {
		ensureMetaData();
		if(!meta.containsKey(IMPORT_FIELD)) {
			return;
		} else {
			JsonArray values = (JsonArray) meta.get(IMPORT_FIELD);
			for (JsonValue val : values) {
				if(val instanceof JsonString) {
					namespace.imprt(val.toString());
				}
				else if(val instanceof JsonRecord) {
					JsonRecord rec = (JsonRecord) val;
					String name = rec.get(new JsonString("module")).toString();
					JsonArray vars = (JsonArray) rec.get(new JsonString("vars"));
					ArrayList<String> ids = new ArrayList<String>();
					for (JsonValue id : vars) {
						ids.add(id.toString());
					}
					/*
					 * There are two cases here, either it is just a record with one entry "*",
					 * or a list of function names
					 */
					if(ids.size() == 1 && ids.get(0).equals("*")) {
						namespace.include(name);
					} else {
						namespace.include(name, ids);
					}
				}
			}
		}
	}
	
	public boolean isSystemModule() {
		return false;
	}
	
	public abstract File[] getJaqlFiles();
	public abstract File[] getJarFiles();
	public abstract File[] getExampleFiles();
	public abstract File getTestDirectory();
	public abstract String[] getTests();
	public abstract JsonRecord getModuleDescription();
	protected abstract InputStream getMetaDataStream();
}
