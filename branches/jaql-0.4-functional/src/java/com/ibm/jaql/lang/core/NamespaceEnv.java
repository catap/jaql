package com.ibm.jaql.lang.core;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;

import com.ibm.jaql.json.parser.ParseException;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.JavaUdfExpr;
import com.ibm.jaql.lang.expr.top.AssignExpr;
import com.ibm.jaql.lang.parser.JaqlLexer;
import com.ibm.jaql.lang.parser.JaqlParser;
import com.ibm.jaql.util.ClassLoaderMgr;
import com.ibm.jaql.util.shell.ChainedInputStream;

public class NamespaceEnv extends Env {
  private HashMap<String, NamespaceEnv> namespaces = new HashMap<String, NamespaceEnv>();
  private HashSet<String> exports = new HashSet<String>();
  private Module module;

	public NamespaceEnv() {
		super(true);
		imprt("system");
		include("system");
	}
  
  public void imprt(String name) {
  	NamespaceEnv namespace = NamespaceLoader.getNamespace(name);
  	if(namespace != null) {
  		namespaces.put(name, namespace);
  	}
  }
  
  public void include(String name) {
  	NamespaceEnv namespace = NamespaceLoader.getNamespace(name);
  	if(namespace != null) {
    	include(namespace);
  	}
  }
  
  public void include(NamespaceEnv namespace) {
  	//Put all exported variables into scope
  	for (String var: namespace.exports) {
			scope(namespace.inscope(var));
		}
  }
  
  public void include(String name, ArrayList<String> ids) {
  	NamespaceEnv namespace = NamespaceLoader.getNamespace(name);
  	
  	//Put variables in scope
  	for (String id : ids) {
  		if(namespace.exports.contains(id)) {
  			scope(namespace.inscope(id));
  		}
  		else {
  			throw new RuntimeException(id + " could not be found in module " + name);
  		}
		}
  }

	/** 
	 * Creates a new variable with the specified name and puts it into the namespace scope. 
   * The most recent definition of the variable of the specified name is overwritten.
   *  
   * @param varName
   * @return
   */
  public Var scopeNamespace(String varName, Schema schema)
  {
//    Var var = nameMap.get(varName);
//    if (var != null)
//    {
//      unscope(var); // TODO: varName might still be on the globals scope... 
//    }
//    var = new Var(varName, expr.getSchema(), expr, true);
//    scope(var);
//    exports.add(var.name());
//    return var;
    Var var = nameMap.get(varName);
    if (var != null)
    {
      unscope(var);  
    }
    var = new Var(varName, schema, true);
    scope(var);
    exports.add(varName);
    return var;
  }
  
  public Var scopeNamespace(String varName) 
  {
    Var var = nameMap.get(varName);
    if (var != null)
    {
      unscope(var);  
    }
    var = new Var(varName, SchemaFactory.anySchema(), true);
    scope(var);
    exports.add(varName);
    return var;
  }
  
  public Var scopeNamespace(String varName, JsonValue value)
  {
  	Var var = nameMap.get(varName);
    if (var != null)
    {
      unscope(var); // TODO: varName might still be on the globals scope... 
    }
    var = new Var(varName, SchemaFactory.schemaOf(value));
    var.setValue(value);
    var.finalize();
    scope(var);
    exports.add(var.name());
    return var;
  }
  
	public Expr inscopeFunction(Env env, String fnname, ArrayList<Expr> args) {
		//return lib.lookup(env, fnname, args);
		return null;
	}
  
	public Var exportedVar(String varName) {
		if(exports.contains(varName)) {
			return inscope(varName);
		} else {
			throw new IndexOutOfBoundsException("variable is hidden in this scope: "
          + varName);
		}
	}
  
  public NamespaceEnv loadedNamespace(String name) {
  	if(!namespaces.containsKey(name)) {
  		throw new IndexOutOfBoundsException("Namespace is not available in this scope");
  	}
  	return namespaces.get(name);
  }
  
  @Override
  public NamespaceEnv namespaceEnv()
  {
  	throw new RuntimeException("namespaceEnv should not be called on a namespaceEnv");
  }
	
  //HACK: This is only needed for the register function method in the parser, should not be used
  @Deprecated
	public Var importFunction(String name, String cls) {
		exports.add(name);
		Var v = scopeNamespace(name);
		v.setExpr(new JavaUdfExpr(new ConstExpr(new JsonString(cls))));
		return v;
	}
	
	public Module getModule() {
		return module;
	}
	
	@SuppressWarnings("unchecked")
	public HashSet<String> getExports() {
		return (HashSet<String>) exports.clone();
	}
	
	public static NamespaceEnv getNamespace(String name) {
		return NamespaceLoader.getNamespace(name);
	}
	
	/**
	 * This inner class handles all operations related to reading modules from the
	 * filesystem.
	 * 
	 */
	static class NamespaceLoader {
		private static HashMap<String, NamespaceEnv> lib = new HashMap<String, NamespaceEnv>();
	  private static boolean sys_init = false;
	  
		//TODO: Make private
	 	static void addNamespaceToLib(String name, NamespaceEnv e) {
	  	assert !lib.containsKey(name) : "Namespace " + name + " was loaded twice";
	  	lib.put(name, e);
	  }

		static NamespaceEnv getNamespace(String name) {
	  	NamespaceEnv namespace = lib.get(name);
	  	//Load it from file if it was not loaded yet
	  	if(namespace == null) {
				if(sys_init) {
					return null;
				}
	  		namespace = NamespaceLoader.initNamespace(name);
	  		addNamespaceToLib(name, namespace);
	  	}
	  	return namespace;
	  }
		
	  private static NamespaceEnv initNamespace(String name) {
	  	Module module = DefaultModule.findModule(name);
	  	if(module == null) {
	  		throw new RuntimeException("Module not found " + name);
	  	}
	  	
	  	NamespaceEnv namespace = null;
	  	if(module.isSystemModule()) {
	  		if(sys_init) {
	  			return null;
	  		} else {
	  			sys_init = true;
	  		}
	  	}
	  	namespace = new NamespaceEnv();
	  	namespace.module = module;
	  	
	  	try {
				//Load imports
				module.loadImports(namespace);
		  	
		  	//Load jar files
		  	for (File jar: module.getJarFiles()) {
		  		ClassLoaderMgr.addExtensionJar(jar);
				}
		  	
		  	//Register udf
		  	for (Entry<String, Expr> udf : module.getUDFs().entrySet()) {
					String fnname = udf.getKey();
					Expr e = udf.getValue();
					if(sys_init) {
						e.setSystemExpr(true);
					}
					Var v = namespace.scopeNamespace(fnname);
					v.setExpr(e);
				}
		  	
		  	//TODO: This should be deprecated when proper system namespace support is implemented
		  	if(module.isSystemModule()) {
		  		SystemEnv.registerAll(namespace);
		  	}

		  	//Load jaql files
		  	ChainedInputStream jaqlIn = new ChainedInputStream();
		  	for (File jaql : module.getJaqlFiles()) {
					jaqlIn.add(new FileInputStream(jaql));
				}
		  	loadJaqlNamespace(name, jaqlIn, namespace);
		  	
		  	//Set Exports
		  	HashSet<String> ids = module.getExportedFunctions();
		  	if(ids!= null) {
		  		namespace.exports.clear();
		  		for (String id : ids) {
		  			try {
		  				if(namespace.inscope(id) != null) {
		  					namespace.exports.add(id);
		  					continue;
		  				}
		  			} catch(Exception e) {
		  				throw new RuntimeException("Exported variable " + id + " does not exist", e);
		  			}
		  		}
		  	}
		  	
				if(sys_init) {
					sys_init = false;
				}
		  	return namespace;
			} catch (FileNotFoundException e) {
				throw new RuntimeException(e);
			} catch (ParseException e) {
				throw new RuntimeException(e);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
	  }
	  
	  private static void loadJaqlNamespace(String name, InputStream in, NamespaceEnv namespace) {
			JaqlLexer lexer = new JaqlLexer(in);
			
			Context context = new Context();
		 	//TODO: Public variables
		  JaqlParser parser = new JaqlParser(lexer);
		  parser.env.namespaceEnv = namespace;
		  lexer.setFilename(name);
		  
		  //Parse namespace jaql file
		  while(true) {
		  	parser.env.reset();
		    context.reset(); 
		    try {
					Expr expr = parser.parse();
					if(expr != null) {
						if( expr instanceof AssignExpr )
						{
							expr.eval(context);
						}
						else {
							//Allowed namespace expressions should always be handled
							throw new RuntimeException("Expr " + expr + " is parsed but not handled");
						}
					}
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
				if( parser.done )
		    {
		      break;
		    }
		  }
		}
	}
}
