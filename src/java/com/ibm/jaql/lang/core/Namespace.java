/*
 * Copyright (C) IBM Corp. 2008.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.ibm.jaql.lang.core;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.ibm.jaql.json.parser.ParseException;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.top.AssignExpr;
import com.ibm.jaql.lang.parser.JaqlLexer;
import com.ibm.jaql.lang.parser.JaqlParser;
import com.ibm.jaql.util.ClassLoaderMgr;
import com.ibm.jaql.util.shell.ChainedReader;

/** A namespace is a collection of named variables. */
public class Namespace {
  protected String                     name               = null;   // null for global namespace only
  protected Module                     module             = null; // null for system namespace and global namespace 
  protected boolean                    isFinal            = false;
  protected HashMap<String, Var>       variables          = new HashMap<String, Var>();
  protected HashMap<String, Namespace> importedNamespaces = new HashMap<String, Namespace>();
  protected HashMap<String, Var>       importedVariables  = new HashMap<String, Var>();
  protected HashSet<String>            exportedVariables  = new HashSet<String>();
  
  
  // -- construction ------------------------------------------------------------------------------
  
  protected Namespace()
  {
    // used by subclasses 
  }
  
  protected Namespace(String name) {
    this.name = name;
    importNamespace(SystemNamespace.getInstance());
    importAllFrom(SystemNamespace.getInstance());
  }
  

  // -- importing ---------------------------------------------------------------------------------
  
  public void importNamespace(Namespace namespace) {
    ensureNotFinal();
		importedNamespaces.put(namespace.getName(), namespace);
  }
  
  public void importAllFrom(Namespace namespace) {
    ensureNotFinal();
    for (String varName : namespace.exports()) 
    {
  	  // FIXME: check for name clashes
  	  Var var = namespace.inscope(varName);
  	  importedVariables.put(varName, var);
		}
  }
  
  public void importFrom(Namespace namespace, ArrayList<String> varNames) {
    ensureNotFinal();
  	for (String varName : varNames) 
  	{
  		if (namespace.exports(varName)) 
  		{
  		  // FIXME: check for name clashes
  		  Var var = namespace.inscope(varName);
        importedVariables.put(varName, var);
  		}
  		else 
  		{
  			throw new RuntimeException(varName + " could not be found in module " + namespace);
  		}
		}
  }

  
  // -- scoping -----------------------------------------------------------------------------------
  
  /**
   * Place a variable (back) in scope
   */
  public void scope(Var var)
  {
    ensureNotFinal();
    // var.setNamespace(this);
    unscope(var);
    var.varStack = variables.get(var.name());
    variables.put(var.name(), var);
  }

  /** Creates a new local variable with the specified name and puts it into the local scope.
   * Previous definitions of variables of the specified name are hidden but not overwritten.
   * 
   * @param taggedName
   * @return
   */
  public Var scope(String taggedName)
  {
    ensureNotFinal();
    Var var = new Var(taggedName);
    scope(var);
    return var;
  }

  /** Creates a new local variable with the specified name and schema and puts it into the local scope.
   * Previous definitions of variables of the specified name are hidden but not overwritten.
   * 
   * @param taggedName
   * @return
   */
  public Var scope(String taggedName, Schema varSchema)
  {
    ensureNotFinal();
    Var var = new Var(taggedName, varSchema);
    scope(var);
    return var;
  }
  
  /** Removes the specified variable from this scope. */
  public void unscope(Var var)
  {
    ensureNotFinal();
    
    Var oldVar = variables.get(var.name());
    if (oldVar == var)
    {
      if (var.varStack != null)
      {
        variables.put(var.name(), var.varStack);
      }
      else
      {
        variables.remove(var.name());
      }
    }
    else
    {
      // find the same-name variable defined right after var
      while (oldVar != null && oldVar.varStack != var)
      {
        oldVar = oldVar.varStack;
      }
      if (oldVar == null)
      {
        // not in scope
        return;
      }
      
      // and remove var from the chain
      oldVar.varStack = var.varStack;
    }
  }
  
  /** Returns the variable of the specified name, searching in both the local and the
   * imported namespaces (in this order).
   * 
   * @throws IndexOutOfBoundsException if the variable is not defined or hidden
   */
  public Var inscope(String taggedName)
  {
    return inscope(taggedName, false);
  }

  /** Returns the variable of the specified name, searching in only the this namespaces.
   * 
   * @throws IndexOutOfBoundsException if the variable is not defined or hidden
   */
  public Var inscopeLocal(String taggedName)
  {
    return inscope(taggedName, true);
  }
  
  // helper for the above inscope methods
  private Var inscope(String taggedName, boolean local)
  {
    Var var = findVar(variables, taggedName);
    if (!local && var == null)
    {
      var = findVar(importedVariables, taggedName);
    }
    if (var == null)
    {
      throw new IndexOutOfBoundsException("variable not defined: " + taggedName);
    }
    if (var.isHidden())
    {
      throw new IndexOutOfBoundsException("variable is hidden in this scope: "
          + taggedName);
    }
    return var;
  }
  
  /** Returns the variable of the specified name, searching in only the imported namespaces 
   * of the specified name.
   * 
   * @throws IndexOutOfBoundsException if the variable is not defined or hidden
   */
  public Var inscopeImport(String namespace, String varName)
  {
    Namespace ns = importedNamespaces.get(namespace);
    if (ns == null)
    {
      throw new IllegalArgumentException("unknown namespace " + namespace);
    }
    if (!ns.exports(varName))
    {
      throw new IllegalArgumentException("namespace " + namespace 
          + " does not export a variable of name " + varName);
    }
    return ns.inscopeLocal(varName);
  }
  
  /** Search for a variable of the given name and tag in <code>varMap</code> and return it. Return 
   * null if not found. */
  protected static Var findVar(Map<String, Var> varMap, String taggedName)
  {
    String[] split = Var.splitTaggedName(taggedName);
    String name = split[0];
    String tag = split[1];
    
    if (varMap.containsKey(name))
    {
      Var var = varMap.get(name);
      if (tag != null)
      {
        while (var != null && !tag.equals(var.tag()))
        {
          var = var.varStack;
        }
      }
      return var; // might be null
    }
    return null;
  }

  // -- getters -----------------------------------------------------------------------------------
  
	public Module getModule() {
		return module;
	}
	
  public boolean isFinal()
  {
    return isFinal;
  }
  
  public void makeFinal()
  {
    if (isFinal()) return;
    for (Var v : variables.values())
    {
      // v.makeFinal();
      assert v.isFinal();
    }
    isFinal = true;
  }
  
  protected void ensureNotFinal()
  {
    if (isFinal())
    {
      throw new IllegalStateException("the final namespace " + name + " cannot be changed");
    }
  }
  
  public String getName()
  {
    return name;
  }
  
  public Set<String> exports()
  {
    return Collections.unmodifiableSet(exportedVariables);
  }
  
  public boolean exports(String varName)
  {
    return exportedVariables.contains(varName);
  }

  /* Return a list of all inscope variables */
  public Collection<Var> listVariables(boolean local)
  {
    TreeMap<String, Var> map = new TreeMap<String, Var>();
    if( ! local )
    {
      map.putAll(importedVariables);
    }
    map.putAll(variables);
    return map.values();
  }
  
  // -- Namespace loading -------------------------------------------------------------------------
	
	public static Namespace get(String name) {
		return NamespaceLoader.get(name);
	}
	
	public static void clearNamespaces() {
		NamespaceLoader.clear();
	}
	
	/**
	 * This inner class handles all operations related to reading modules from the
	 * filesystem.
	 * 
	 */
	private static final class NamespaceLoader {
		private static HashMap<String, Namespace> namespaces = new HashMap<String, Namespace>();
	  
	 	private static void add(Namespace e) {
	 	  if (e.getName() == null)
	 	  {
	 	    throw new NullPointerException("namespace name must not be null");
	 	  }
	 	  if (namespaces.containsKey(e.getName()))
	 	  {
	 	   throw new IllegalArgumentException("a namespace with name " + e.getName() + " is already loaded");
	 	  }
	  	namespaces.put(e.getName(), e);
	  }

	 	public static void clear() {
	 		namespaces.clear();
	 	}
	 	
		public static Namespace get(String name) {
		  Namespace namespace = namespaces.get(name);
	  	if (namespace == null) {
	  		namespace = init(name);
	  		add(namespace);
	  	}
	  	return namespace;
	  }
		
	  private static Namespace init(String name) {
	    if (SystemNamespace.NAME.equals(name)) 
	    {
	      return SystemNamespace.getInstance();
	    }
	    
	    Module module = DefaultModule.findModule(name);
	  	if (module == null) {
	  		throw new RuntimeException("Module not found " + name);
	  	}
	  	
	  	Namespace namespace = new Namespace(name);
	  	namespace.module = module;
	  	
	  	try {
				// load imports
				module.loadImports(namespace);
		  	
		  	// load jar files
		  	for (File jar : module.getJarFiles()) {
		  		ClassLoaderMgr.addExtensionJar(jar);
				}
		  	
		  	// load jaql files
		  	ChainedReader jaqlIn = new ChainedReader();
		  	for (File jaql : module.getJaqlFiles()) {
              jaqlIn.add(new InputStreamReader(new FileInputStream(jaql), "UTF-8"));
		  	}
		  	load(name, jaqlIn, namespace);
		  	
		  	// set exports
		  	Set<String> varNames = module.exports();
		  	if(!varNames.isEmpty()) {
		  		for (String varName : varNames) {
		  				if (namespace.variables.containsKey(varName)) 
		  				{
		  					namespace.exportedVariables.add(varName);
		  					continue;
		  				}
		  				else {
		  					throw new RuntimeException("export " + varName + " does not exist");
		  				}
		  		}
		  	} else {
		  		namespace.exportedVariables.addAll(namespace.variables.keySet());
		  	}
		  	
		  	// finalize
		  	namespace.makeFinal();		  	
		  	return namespace;
			} catch (FileNotFoundException e) {
				throw new RuntimeException(e);
			} catch (ParseException e) {
				throw new RuntimeException(e);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
	  }
	  
	  private static void load(String name, Reader in, Namespace namespace) {
			JaqlLexer lexer = new JaqlLexer(in);
			
			Context context = new Context();
		  JaqlParser parser = new JaqlParser(lexer);
		  parser.env.globals = namespace;
		  lexer.setFilename(name);
		  
		  // parse jaql file
		  while(true) {
		  	parser.env.reset();
		    context.reset(); 
		    try {
					Expr expr = parser.parse();
					if (expr != null) {
						if ( expr instanceof AssignExpr )
						{
							expr.eval(context);
						}
						else {
							throw new RuntimeException("module files should only contain assignments; expr " 
							    + expr + " is not allowed");
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
