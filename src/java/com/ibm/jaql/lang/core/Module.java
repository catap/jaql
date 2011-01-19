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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Var.Scope;
import com.ibm.jaql.lang.expr.core.Expr;


public class Module extends Namespace
{
  protected final Package pack;
  protected final File script;
  protected final HashMap<String, Module> modules = new HashMap<String, Module>();

  
  public Module(Package pack, String moduleName, File jaqlFile)
  {
    super(moduleName);
    this.pack = pack;
    this.script = jaqlFile;
    
    this.modules.put("", this);
    
    JsonValue jfile = null;
    JsonValue jdir = null;
    if( jaqlFile != null )
    {
      String p = null;
      try
      {
        p = jaqlFile.getParentFile().getCanonicalPath();
      }
      catch( Exception ex )
      {
        p = jaqlFile.getParentFile().getAbsolutePath();
      }
      jfile = new JsonString(jaqlFile.getName());
      jdir = new JsonString(p);
    }
    
    scopeVal("_packageName", new JsonString(pack == null ? "" : pack.getName())); // TODO: place SystemNamespace into root package (means multiple instances)
    scopeVal("_moduleName", new JsonString(moduleName == null ? "" : moduleName));
    scopeVal("_moduleFile", jfile);
    scopeVal("_moduleDirectory", jdir);
    
    if( !(this instanceof SystemNamespace) )
    {
      importModule(SystemNamespace.NAME, SystemNamespace.getInstance());
      importExportedVariables(SystemNamespace.getInstance());
    }
  }
  
  public Module getModule(String moduleAlias)
  {
    Module ns = modules.get(moduleAlias);
    if( ns == null )
    {
      throw new RuntimeException("module not imported: "+moduleAlias);
    }
    return ns;
  }

  public void importModule(String moduleAlias, Module module) 
  {    
    Module old = this.modules.put(moduleAlias, module);
    if( old != null )
    {
      // error? warning?
      // this.modules.put(moduleAlias, old);
      // throw new RuntimeException("module alias already in use: "+moduleAlias);
    }
  }
  
  public Module importModule(String qualifiedPath, String moduleAlias) 
  {    
    Package start = this.pack;
    if( qualifiedPath.startsWith("::") )
    {
      start = this.pack.getRoot();
      qualifiedPath = qualifiedPath.substring(2);
    }
    String path = qualifiedPath.replaceAll("::", "/") + ".jaql";
    
    for( Package pack = start ; pack != null ; pack = pack.parent )
    {
      for( File dir: pack.searchPath )
      {
        if( dir.isDirectory() )
        {
          File jaqlFile = new File(dir, path);
          if( jaqlFile.isFile() )
          {
            return loadModule(qualifiedPath, moduleAlias, pack, jaqlFile);
          }
        }
      }
    }
    
    String err = "Could not find module " + qualifiedPath + " in:";
    for( Package pack = start ; pack != null ; pack = pack.parent )
    {
      for( File dir: pack.searchPath )
      {
        err += "\n" + dir.getAbsolutePath();
      }    
    }
    throw new RuntimeException(err);
  }

  protected Module loadModule(String qualifiedPath, String moduleAlias, Package pack, File jaqlFile)
  {
    // Walk down the package tree initializing as needed
    String[] parts = qualifiedPath.split("::");
    int n = parts.length - 1;
    for(int i = 0 ; i < n ; i++)
    {
      File packDir = jaqlFile.getParentFile();
      for( int j = n - i - 1 ; j > 0 ; j--)
      {
        packDir = packDir.getParentFile();
      }
      pack = pack.loadPackage(parts[i], packDir);
    }
    
    String moduleName = parts[n];
    Module module = pack.modules.get(moduleName);
    if( module != null )
    {
      if( ! module.isFinal )
      {
        throw new RuntimeException("circular dependency from module "+this.name+" to "+moduleName);
      }
      else if( ! jaqlFile.equals( module.script ) )
      {
        throw new RuntimeException("ambiguous reference from module "+this.name+" to "+moduleName 
                + " " + module.script + " and " + jaqlFile );
      }
      // else if module.timestamp != jaqlFile.timestamp, module = null
    }
    
    if( moduleAlias == null )
    {
      moduleAlias = moduleName;
    }
    if( module == null )
    {
      module = pack.parseModule(moduleName, jaqlFile);
      pack.modules.put(moduleAlias, module);
    }
    this.modules.put(moduleAlias, module);
    return module; 
  }
  
  /** Import the listed variables from the other namespace into this one */
  public void importVariables(Module namespace, List<String> varNames)
  {
    for( String varName: varNames )
    {
      Var var = namespace.variables.get(varName);
      if( var == null )
      {
        throw new RuntimeException("Variable "+varName+" not defined in module "+namespace.name);
      }
      variables.put(varName, var);
    }
  }

  /** imports all all variables exported from another namespace */
  public void importExportedVariables(Module namespace)
  {
    for( Var var: namespace.variables.values() )
    {
      if( ! var.name().startsWith("_") )
      {
        variables.put(var.name(), var);
      }
    }
  }

  /** returns the names of all exported variables */
  public Set<String> exports()
  {
    HashSet<String> names = new HashSet<String>();
    for( Var var: variables.values() )
    {
      if( ! var.name().startsWith("_") )
      {
        names.add(var.taggedName());
      }
    }
    return names;
  }

  /**
   * Create a new immutable val variable with the specified name and put it into the global scope. 
   * The most recent definition of the global variable of the specified name is overwritten.
   */
  public final Var scopeVal(String varName, Schema schema)
  {
    ensureNotFinal();
    Var var = findVar(varName);
    if ( var != null )
    {
      unscope(var);
    }
    var = new Var(this, varName, schema, Scope.GLOBAL, Var.State.FINAL);
    scope(var);
    return var;
  }

  public final Var scopeVal(String varName, JsonValue value)
  {
    ensureNotFinal();
    Var var = findVar(varName);
    if ( var != null )
    {
      unscope(var);
    }
    var = new Var(this, varName, SchemaFactory.schemaOf(value), Scope.GLOBAL, Var.State.FINAL);
    scope(var);
    var.setValue(value);
    return var;
  }

  /**
   * Create a new immutable expr variable with the specified name and put it into the global scope. 
   * The most recent definition of the global variable of the specified name is shadowed.
   */
  public Var scopeExpr(String varName, Schema schema, Expr expr)
  {
    ensureNotFinal();
    Var var = findVar(varName);
    if ( var != null )
    {
      unscope(var);
    }
    var = new Var(this, varName, schema, Scope.GLOBAL, Var.State.FINAL);
    //  try
    //  {
    //    expr = expandMacros(expr);
    //  }
    //  catch(Exception ex)
    //  {
    //    JaqlUtil.rethrow(ex);
    //  }
    var.setExpr(expr);
    scope(var);
    return var;
  }

  /**
   * If the varName is bound to a mutable global, return it.
   * Otherwise create a new mutable var variable with the specified name and puts it into the global scope, 
   * and the most recent definition of the immutable global variable of the specified name is shadowed.
   */
  public Var scopeMutable(String varName, Schema schema)
  {
    ensureNotFinal();
    Var var = findVar(varName);
    if ( var != null )
    {
      var = inscope(varName);
      if( var.isMutable() )
      {
        return var;
      }
      unscope(var);
    }
    var = new Var(this, varName, schema, Scope.GLOBAL, Var.State.MUTABLE);
    scope(var);
    return var;
  }

  /** 
   * Creates a new variable with the specified name and puts it into the global scope. 
   * The most recent definition of the global variable of the specified name is overwritten.
   * If varName contains a tag, this method will fail.
   */
  public Var setOrScopeMutable(String varName, JsonValue value)
  {
    Var var = scopeMutable(varName, SchemaFactory.anySchema());
    var.setValue(value);
    return var;
  }

  @Override
  public Package getPackage()
  {
    return pack;
  }

  @Override
  public String getQualifiedName()
  {
    return pack.getName() + "::" + name;
  }

  public List<Var> listVariables(boolean includeModules, boolean onlyExtern, boolean includeSystem)
  {
    Module system = SystemNamespace.getInstance();
    TreeMap<String, Var> sortVars = new TreeMap<String, Var>();
    for( Var v: variables.values() )
    {
      if( ( !onlyExtern || v.isMutable() ) &&
          ( includeSystem || v.getNamespace() != system ) )
      {
        sortVars.put(v.name(), v);
      }
    }
    ArrayList<Var> vars = new ArrayList<Var>(sortVars.values());
    
    if( includeModules )
    {
      TreeMap<String, Collection<Var>> sortMods = new TreeMap<String, Collection<Var>>();
      for( Module m: modules.values() )
      {
        if( includeSystem || m != SystemNamespace.getInstance() )
        {
          Collection<Var> mvars = m.listVariables(false, onlyExtern, false);
          if( ! mvars.isEmpty() )
          {
            sortMods.put( m.getQualifiedName(), mvars);
          }
        }
      }
      for( Collection<Var> mvars: sortMods.values() )
      {
        vars.addAll(mvars);
      }
    }
    
    return vars;
  }

  public List<Var> listVariables(boolean includeModules)
  {
    return listVariables(includeModules, false, false);
  }
  
  public List<Var> listExternal(boolean includeModules)
  {
    return listVariables(includeModules, true, false);
  }

  public List<File> getModulePath()
  {
    ArrayList<File> path = new ArrayList<File>();
    for( Package p = getPackage() ; p != null ; p = p.parent )
    {
      path.addAll( p.searchPath );
    }
    return path;
  }

  public String getModulePathString()
  {
    List<File> path = getModulePath();
    StringBuilder sb = new StringBuilder();
    String sep = "";
    for( File f: path )
    {
      sb.append( sep );
      sb.append( f.getAbsolutePath() );
      sep = ",";
    }
    return sb.toString();
  }

  public String getModuleAlias(Module module)
  {
    for( Map.Entry<String,Module> e: this.modules.entrySet() )
    {
      if( e.getValue() == module )
      {
        return e.getKey();
      }
    }
    throw new RuntimeException("namespace "+module.getQualifiedName()+" is not imported in "+getQualifiedName());
  }
  
}
