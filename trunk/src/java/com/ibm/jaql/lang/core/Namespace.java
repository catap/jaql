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

import java.util.HashMap;


public class Namespace
{
  protected final String name;
  protected final HashMap<String, Var> variables = new HashMap<String, Var>();
  protected boolean isFinal = false; // true once a module is loaded

  public Namespace(String name)
  {
    this.name = name;
  }

  /**
   * @return The module name without any package qualifier.  
   * The script global module name is "".
   * The local namespace name is "*local*".
   */
  public final String getName()
  {
    return name;
  }

  /**
   * @return the module name with package qualifiers, including leading '::'
   * The script global module name is "::".
   * The local namespace name is still just "*local*".
   */
  public String getQualifiedName()
  {
    return name;
  }

  /**
   * Return the package of this module, if global, or null for local namespaces.
   */
  public Package getPackage()
  {
    return null;
  }


  /** Return the named variable or null if not not found */ 
  public Var findVar(String taggedName)
  {
    String[] split = Var.splitTaggedName(taggedName);
    String name = split[0];
    String tag = split[1];

    Var var = variables.get(name);
    if (tag != null)
    {
      while (var != null && !tag.equals(var.tag()))
      {
        var = var.varStack;
      }
    }
    return var; // might be null
  }

  /** Return the named variable, or raise an exception if not found */
  public final Var inscope(String taggedName)
  {
    // TODO: revisit variable tagging.  Is there a better way?
    Var var = findVar(taggedName);
    if( var == null )
    {
      throw new RuntimeException("variable is not defined: " + taggedName + " in namespace " + name); // TODO: full name
    }
    if( var.isHidden() ) 
    {
      throw new RuntimeException("variable is hidden " + var.taggedName() + " in namespace " + name);
    }
    return var;
  }
  
  /** Put a variable into scope -- it better not be inscope somewhere else */
  public final void scope(Var var)
  {
    ensureNotFinal();
    Var oldVar = variables.put(var.name(), var);
    var.setNamespace(this);
    if( oldVar != null && oldVar.isMutable() )
    {
      variables.put(oldVar.name(), oldVar);
      throw new RuntimeException("cannot shadow mutable variable "+oldVar.taggedName() + " in namespace " + name);
    }
    var.varStack = oldVar;
  }
  
  public void unscope(Var var)
  {
    ensureNotFinal();

    Var oldVar = variables.get(var.name());
    if (oldVar == var) // removing head of the list
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
      // find the same-name variable before var on the stack
      while (oldVar != null && oldVar.varStack != var)
      {
        oldVar = oldVar.varStack;
      }
      if (oldVar == null)
      {
        assert false: "variable was not in scope: "+var.taggedName() + " in namespace " + name;
        return;
      }

      // and remove var from the chain
      oldVar.varStack = var.varStack;
    }
  }
    
  public void clear()
  {
    ensureNotFinal();
    variables.clear();
  }
  

  public final boolean isFinal()
  {
    return isFinal;
  }

  public void makeFinal()
  {
//    if( isFinal ) return;
//    for (Var v : variables.values())
//    {
//      // v.makeFinal();
//      assert v.isFinal();
//    }
    isFinal = true;
  }

  protected void ensureNotFinal()
  {
    if( isFinal )
    {
      throw new IllegalStateException("the final namespace " + name + " cannot be changed");
    }
  }

}
///*
// * Copyright (C) IBM Corp. 2008.
// * 
// * Licensed under the Apache License, Version 2.0 (the "License"); you may not
// * use this file except in compliance with the License. You may obtain a copy of
// * the License at
// * 
// * http://www.apache.org/licenses/LICENSE-2.0
// * 
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// * License for the specific language governing permissions and limitations under
// * the License.
// */
//package com.ibm.jaql.lang.core;
//
//import java.io.File;
//import java.util.ArrayList;
//import java.util.Collection;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//import java.util.TreeMap;
//
//import com.ibm.jaql.json.schema.Schema;
//import com.ibm.jaql.json.schema.SchemaFactory;
//import com.ibm.jaql.json.type.JsonString;
//import com.ibm.jaql.json.type.JsonValue;
//import com.ibm.jaql.lang.core.Var.Scope;
//import com.ibm.jaql.lang.expr.core.Expr;
//
//
//public class Module extends NS
//{
//  protected final Package pack;
//  protected final File script;
//  protected final HashMap<String, Module> modules = new HashMap<String, Module>();
//
//  
//  public Namespace(Package pack, String moduleName, File jaqlFile)
//  {
//    super(moduleName);
//    this.pack = pack;
//    this.script = jaqlFile;
//    
//    this.modules.put("", this);
//    
//    JsonValue jfile = null;
//    JsonValue jdir = null;
//    if( jaqlFile != null )
//    {
//      String p = null;
//      try
//      {
//        p = jaqlFile.getParentFile().getCanonicalPath();
//      }
//      catch( Exception ex )
//      {
//        p = jaqlFile.getParentFile().getAbsolutePath();
//      }
//      jfile = new JsonString(jaqlFile.getName());
//      jdir = new JsonString(p);
//    }
//    
//    scopeVal("_packageName", new JsonString(pack == null ? "" : pack.getName())); // TODO: place SystemNamespace into root package (means multiple instances)
//    scopeVal("_moduleName", new JsonString(moduleName == null ? "" : moduleName));
//    scopeVal("_moduleFile", jfile);
//    scopeVal("_moduleDirectory", jdir);
//    
//    if( !(this instanceof SystemNamespace) )
//    {
//      importModule(SystemNamespace.NAME, SystemNamespace.getInstance());
//      importExportedVariables(SystemNamespace.getInstance());
//    }
//  }
//  
//  public Module getModule(String moduleAlias)
//  {
//    Module ns = modules.get(moduleAlias);
//    if( ns == null )
//    {
//      throw new RuntimeException("module not imported: "+moduleAlias);
//    }
//    return ns;
//  }
//
//  public void importModule(String moduleAlias, Module module) 
//  {    
//    Module old = this.modules.put(moduleAlias, module);
//    if( old != null )
//    {
//      // error? warning?
//      // this.modules.put(moduleAlias, old);
//      // throw new RuntimeException("module alias already in use: "+moduleAlias);
//    }
//  }
//  
//  public Module importModule(String qualifiedPath, String moduleAlias) 
//  {    
//    Package start = this.pack;
//    if( qualifiedPath.startsWith("::") )
//    {
//      start = this.pack.getRoot();
//      qualifiedPath = qualifiedPath.substring(2);
//    }
//    String path = qualifiedPath.replaceAll("::", "/") + ".jaql";
//    
//    for( Package pack = start ; pack != null ; pack = pack.parent )
//    {
//      for( File dir: pack.searchPath )
//      {
//        if( dir.isDirectory() )
//        {
//          File jaqlFile = new File(dir, path);
//          if( jaqlFile.isFile() )
//          {
//            return loadModule(qualifiedPath, moduleAlias, pack, jaqlFile);
//          }
//        }
//      }
//    }
//    
//    String err = "Could not find module " + qualifiedPath + " in:";
//    for( Package pack = start ; pack != null ; pack = pack.parent )
//    {
//      for( File dir: pack.searchPath )
//      {
//        err += "\n" + dir.getAbsolutePath();
//      }    
//    }
//    throw new RuntimeException(err);
//  }
//
//  protected Module loadModule(String qualifiedPath, String moduleAlias, Package pack, File jaqlFile)
//  {
//    // Walk down the package tree initializing as needed
//    String[] parts = qualifiedPath.split("::");
//    int n = parts.length - 1;
//    for(int i = 0 ; i < n ; i++)
//    {
//      File packDir = jaqlFile.getParentFile();
//      for( int j = n - i - 1 ; j > 0 ; j--)
//      {
//        packDir = packDir.getParentFile();
//      }
//      pack = pack.loadPackage(parts[i], packDir);
//    }
//    
//    String moduleName = parts[n];
//    Module module = pack.modules.get(moduleName);
//    if( module != null )
//    {
//      if( ! module.isFinal )
//      {
//        throw new RuntimeException("circular dependency from module "+this.name+" to "+moduleName);
//      }
//      else if( ! jaqlFile.equals( module.script ) )
//      {
//        throw new RuntimeException("ambiguous reference from module "+this.name+" to "+moduleName 
//                + " " + module.script + " and " + jaqlFile );
//      }
//      // else if module.timestamp != jaqlFile.timestamp, module = null
//    }
//    
//    if( moduleAlias == null )
//    {
//      moduleAlias = moduleName;
//    }
//    if( module == null )
//    {
//      module = pack.parseModule(moduleName, jaqlFile);
//      pack.modules.put(moduleAlias, module);
//    }
//    this.modules.put(moduleAlias, module);
//    return module; 
//  }
//  
//  /** Import the listed variables from the other namespace into this one */
//  public void importVariables(Module namespace, List<String> varNames)
//  {
//    for( String varName: varNames )
//    {
//      Var var = namespace.variables.get(varName);
//      if( var == null )
//      {
//        throw new RuntimeException("Variable "+varName+" not defined in module "+namespace.name);
//      }
//      variables.put(varName, var);
//    }
//  }
//
//  /** imports all all variables exported from another namespace */
//  public void importExportedVariables(Module namespace)
//  {
//    for( Var var: namespace.variables.values() )
//    {
//      if( ! var.name().startsWith("_") )
//      {
//        variables.put(var.name(), var);
//      }
//    }
//  }
//
//  /** returns the names of all exported variables */
//  public Set<String> exports()
//  {
//    HashSet<String> names = new HashSet<String>();
//    for( Var var: variables.values() )
//    {
//      if( ! var.name().startsWith("_") )
//      {
//        names.add(var.taggedName());
//      }
//    }
//    return names;
//  }
//
//  //protected Namespace importModule(String moduleName, File jaqlFile) 
//  //{
//  //  Namespace namespace = this.getRoot().loadModule(moduleName, jaqlFile);
//  //  importedNamespaces.put(moduleName, namespace);
//  //  return namespace;
//  //}
//
////  public Namespace getNamespace(String name)
////  {
////    if( name == null )
////    {
////      return this;
////    }
////    if( "".equals(name) )
////    {
////      return getRoot();
////    }
////    Namespace ns = importedNamespaces.get(name);
////    if( ns == null )
////    {
////      throw new RuntimeException("namespace "+name+" not found in namespace "+getName());
////    }
////    return ns;
////  }
//
//
//  /**
//   * Create a new immutable val variable with the specified name and put it into the global scope. 
//   * The most recent definition of the global variable of the specified name is overwritten.
//   */
//  public final Var scopeVal(String varName, Schema schema)
//  {
//    ensureNotFinal();
//    Var var = findVar(varName);
//    if ( var != null )
//    {
//      unscope(var);
//    }
//    var = new Var(this, varName, schema, Scope.GLOBAL, Var.State.FINAL);
//    scope(var);
//    return var;
//  }
//
//  public final Var scopeVal(String varName, JsonValue value)
//  {
//    ensureNotFinal();
//    Var var = findVar(varName);
//    if ( var != null )
//    {
//      unscope(var);
//    }
//    var = new Var(this, varName, SchemaFactory.schemaOf(value), Scope.GLOBAL, Var.State.FINAL);
//    scope(var);
//    var.setValue(value);
//    return var;
//  }
//
//  /**
//   * Create a new immutable expr variable with the specified name and put it into the global scope. 
//   * The most recent definition of the global variable of the specified name is shadowed.
//   */
//  public Var scopeExpr(String varName, Schema schema, Expr expr)
//  {
//    ensureNotFinal();
//    Var var = findVar(varName);
//    if ( var != null )
//    {
//      unscope(var);
//    }
//    var = new Var(this, varName, schema, Scope.GLOBAL, Var.State.FINAL);
//    //  try
//    //  {
//    //    expr = expandMacros(expr);
//    //  }
//    //  catch(Exception ex)
//    //  {
//    //    JaqlUtil.rethrow(ex);
//    //  }
//    var.setExpr(expr);
//    scope(var);
//    return var;
//  }
//
//  /**
//   * If the varName is bound to a mutable global, return it.
//   * Otherwise create a new mutable var variable with the specified name and puts it into the global scope, 
//   * and the most recent definition of the immutable global variable of the specified name is shadowed.
//   */
//  public Var scopeMutable(String varName, Schema schema)
//  {
//    ensureNotFinal();
//    Var var = findVar(varName);
//    if ( var != null )
//    {
//      var = inscope(varName);
//      if( var.isMutable() )
//      {
//        return var;
//      }
//      unscope(var);
//    }
//    var = new Var(this, varName, schema, Scope.GLOBAL, Var.State.MUTABLE);
//    scope(var);
//    return var;
//  }
//
//  /** 
//   * Creates a new variable with the specified name and puts it into the global scope. 
//   * The most recent definition of the global variable of the specified name is overwritten.
//   * If varName contains a tag, this method will fail.
//   */
//  public Var setOrScopeMutable(String varName, JsonValue value)
//  {
//    Var var = scopeMutable(varName, SchemaFactory.anySchema());
//    var.setValue(value);
//    return var;
//  }
//
//  @Override
//  public Package getPackage()
//  {
//    return pack;
//  }
//
//  @Override
//  public String getQualifiedName()
//  {
//    return pack.getName() + "::" + name;
//  }
//
//  public List<Var> listVariables(boolean includeModules, boolean onlyExtern, boolean includeSystem)
//  {
//    Module system = SystemNamespace.getInstance();
//    TreeMap<String, Var> sortVars = new TreeMap<String, Var>();
//    for( Var v: variables.values() )
//    {
//      if( ( !onlyExtern || v.isMutable() ) &&
//          ( includeSystem || v.getNamespace() != system ) )
//      {
//        sortVars.put(v.name(), v);
//      }
//    }
//    ArrayList<Var> vars = new ArrayList<Var>(sortVars.values());
//    
//    if( includeModules )
//    {
//      TreeMap<String, Collection<Var>> sortMods = new TreeMap<String, Collection<Var>>();
//      for( Module m: modules.values() )
//      {
//        if( includeSystem || m != SystemNamespace.getInstance() )
//        {
//          Collection<Var> mvars = m.listVariables(false, onlyExtern, false);
//          if( ! mvars.isEmpty() )
//          {
//            sortMods.put( m.getQualifiedName(), mvars);
//          }
//        }
//      }
//      for( Collection<Var> mvars: sortMods.values() )
//      {
//        vars.addAll(mvars);
//      }
//    }
//    
//    return vars;
//  }
//
//  public List<Var> listVariables(boolean includeModules)
//  {
//    return listVariables(includeModules, false, false);
//  }
//  
//  public List<Var> listExternal(boolean includeModules)
//  {
//    return listVariables(includeModules, true, false);
//  }
//
//  public List<File> getModulePath()
//  {
//    ArrayList<File> path = new ArrayList<File>();
//    for( Package p = getPackage() ; p != null ; p = p.parent )
//    {
//      path.addAll( p.searchPath );
//    }
//    return path;
//  }
//
//  public String getModulePathString()
//  {
//    List<File> path = getModulePath();
//    StringBuilder sb = new StringBuilder();
//    String sep = "";
//    for( File f: path )
//    {
//      sb.append( sep );
//      sb.append( f.getAbsolutePath() );
//      sep = ",";
//    }
//    return sb.toString();
//  }
//
//  public String getModuleAlias(Module module)
//  {
//    for( Map.Entry<String,Module> e: this.modules.entrySet() )
//    {
//      if( e.getValue() == module )
//      {
//        return e.getKey();
//      }
//    }
//    throw new RuntimeException("namespace "+module.getQualifiedName()+" is not imported in "+getQualifiedName());
//  }
//  
//}
//
//
//
/////** A namespace is a collection of named variables. */
////public class Namespace extends NS
////{
////  protected final Namespace            parent;                     // null for root namespace only
////  protected File                       directory;                  // only the root can change this
////  protected final HashMap<String, Namespace> importedNamespaces = new HashMap<String, Namespace>();
////  
////  // -- construction ------------------------------------------------------------------------------
////
////  protected Namespace(Namespace parent, String name, File directory) 
////  {
////    super(name);
////    this.parent = parent;
////    this.directory = directory;
////  }
////
////  public RootNamespace getRoot()
////  {
////    if( parent == null )
////    {
////      throw new IllegalStateException("Namespace tree was not rooted with an Env!");
////    }
////    Namespace ns = parent;
////    while( ns.parent != null )
////    {
////      ns = ns.parent;
////    }
////    return ns.getRoot();
////  }
////
////
////  public final Namespace loadModule(String moduleName)
////  {
////    Namespace ns = importedNamespaces.get(moduleName);
////    if( ns == null )
////    {
////      ns = getRoot().locateModule(moduleName, directory);
////    }
////    return ns;
////  }
////
////  /** load a jaql module from the given file */
////  protected Namespace parseModule(String moduleName, File jaqlFile) 
////  {
////    assert ! children.contains(moduleName);
////    if( ! jaqlFile.isFile() || ! jaqlFile.canRead() )
////    {
////      throw new RuntimeException("jaql module cannot be read from file: "+jaqlFile);
////    }
////
////    Namespace namespace = new Namespace(this, moduleName, jaqlFile.getParentFile());
////    children.put(moduleName, namespace);
////
////    try
////    {
////      InputStreamReader reader = new InputStreamReader(new FileInputStream(jaqlFile), "UTF-8");
////      JaqlLexer lexer = new JaqlLexer(reader);
////      Context context = new Context();
////      JaqlParser parser = new JaqlParser(lexer);
////      parser.env.globals = namespace;
////      lexer.setFilename(moduleName);
////
////      // parse jaql file
////      while( ! parser.done )
////      {
////        parser.env.reset();
////        context.reset(); 
////        Expr expr = parser.parse();
////        if (expr != null) {
////          if ( expr instanceof AssignExpr )
////          {
////            expr.eval(context);
////          }
////          else {
////            throw new RuntimeException("module files should only contain assignments; expr " 
////                    + expr + " is not allowed");
////          }
////        }
////      }
////
////      return namespace;
////    }
////    catch( Exception ex )
////    {
////      throw new RuntimeException("error loading module: "+name, ex);
////    }
////  }
////
////  protected File findChildModuleFile(String moduleName)
////  {
////    for( File dir: searchPath )
////    {
////      if( dir != null && dir.isDirectory() )
////      {
////        String fileName = moduleName + ".jaql";
////        File file = new File(directory, fileName);
////        if( file.isFile() )
////        {
////          return file;
////        }
////
////        File subdir = new File(directory, moduleName);
////        if( subdir.isDirectory() )
////        {
////          file = new File(subdir, fileName);
////          if( file.isFile() )
////          {
////            return file;
////          }
////        }
////      }
////    }
////      
////    return null;
////  }
////  
////  protected Namespace findChildModule(String moduleName)
////  {
////    Namespace ns = children.get(moduleName);
////    if( ns != null )
////    {
////      return ns;
////    }
////
////    File jaqlFile = findChildModuleFile(moduleName);
////    if( jaqlFile != null )
////    {
////      ns = parseModule(moduleName, jaqlFile);
////      children.put(moduleName, ns);
////      return ns;
////    }
////    
////    return null;
////  }
////  
////  protected final Namespace importModule(String moduleName)
////  {
////    Namespace ns = importedNamespaces.get(moduleName);
////    if( ns == null )
////    {
////      return ns;
////    }
////    
////    if( findChildModuleFile(moduleName) != null )
////    {
////      throw new RuntimeException("parent module "+this.name+" cannot refer to child module "+moduleName);
////    }
////    
////    for( Namespace ancestor = parent ; ancestor != null ; ancestor = ancestor.parent )
////    {
////      ns = ancestor.findChildModule(moduleName);
////      if( ns != null )
////      {
////        if( ! ns.isFinal() )
////        {
////          throw new RuntimeException("circular dependency from module "+this.name+" to "+moduleName);
////        }
////        importedNamespaces.put(moduleName, ns);
////        return ns;
////      }
////    }
////    
////    String err = "Could not find module " + moduleName + " in:";
////    for( Namespace ancestor = parent ; ancestor != null ; ancestor = ancestor.parent )
////    {
////      for( File dir: ancestor.searchPath )
////      {
////        err += "\n" + dir.getAbsolutePath();
////      }
////    }
////    throw new RuntimeException(err);
////  }
////
////  /** Import the listed variables from the other namespace into this one */
////  public void importVariables(Namespace namespace, List<String> varNames)
////  {
////    for( String varName: varNames )
////    {
////      Var var = namespace.variables.get(varName);
////      if( var == null )
////      {
////        throw new RuntimeException("Variable "+var+" not defined in module "+namespace.name);
////      }
////      variables.put(varName, var);
////    }
////  }
////
////  /** imports all all variables exported from another namespace */
////  public void importExportedVariables(Namespace namespace)
////  {
////    for( Var var: namespace.variables.values() )
////    {
////      if( ! var.name().startsWith("_") )
////      {
////        variables.put(var.name(), var);
////      }
////    }
////  }
////
////  /** returns the names of all exported variables */
////  public Set<String> exports()
////  {
////    HashSet<String> names = new HashSet<String>();
////    for( Var var: variables.values() )
////    {
////      if( ! var.name().startsWith("_") )
////      {
////        names.add(var.taggedName());
////      }
////    }
////    return names;
////  }
////
//////  protected Namespace importModule(String moduleName, File jaqlFile) 
//////  {
//////    Namespace namespace = this.getRoot().loadModule(moduleName, jaqlFile);
//////    importedNamespaces.put(moduleName, namespace);
//////    return namespace;
//////  }
////
////  public Namespace getNamespace(String name)
////  {
////    if( name == null )
////    {
////      return this;
////    }
////    if( "".equals(name) )
////    {
////      return getRoot();
////    }
////    Namespace ns = importedNamespaces.get(name);
////    if( ns == null )
////    {
////      throw new RuntimeException("namespace "+name+" not found in namespace "+getName());
////    }
////    return ns;
////  }
////
////  
////  /**
////   * Create a new immutable val variable with the specified name and put it into the global scope. 
////   * The most recent definition of the global variable of the specified name is overwritten.
////   */
////  public final Var scopeVal(String varName, Schema schema)
////  {
////    ensureNotFinal();
////    Var var = findVar(varName);
////    if ( var != null )
////    {
////      unscope(var);
////    }
////    var = new Var(this, varName, schema, Scope.GLOBAL, Var.State.FINAL);
////    scope(var);
////    return var;
////  }
////
////  /**
////   * Create a new immutable expr variable with the specified name and put it into the global scope. 
////   * The most recent definition of the global variable of the specified name is shadowed.
////   */
////  public Var scopeExpr(String varName, Schema schema, Expr expr)
////  {
////    ensureNotFinal();
////    Var var = findVar(varName);
////    if ( var != null )
////    {
////      unscope(var);
////    }
////    var = new Var(this, varName, schema, Scope.GLOBAL, Var.State.FINAL);
//////    try
//////    {
//////      expr = expandMacros(expr);
//////    }
//////    catch(Exception ex)
//////    {
//////      JaqlUtil.rethrow(ex);
//////    }
////    var.setExpr(expr);
////    scope(var);
////    return var;
////  }
////
////  /**
////   * If the varName is bound to a mutable global, return it.
////   * Otherwise create a new mutable var variable with the specified name and puts it into the global scope, 
////   * and the most recent definition of the immutable global variable of the specified name is shadowed.
////   */
////  public Var scopeMutable(String varName, Schema schema)
////  {
////    ensureNotFinal();
////    Var var = findVar(varName);
////    if ( var != null )
////    {
////      var = inscope(varName);
////      if( var.isMutable() )
////      {
////        return var;
////      }
////      unscope(var);
////    }
////    var = new Var(this, varName, schema, Scope.GLOBAL, Var.State.MUTABLE);
////    scope(var);
////    return var;
////  }
////
////  /** 
////   * Creates a new variable with the specified name and puts it into the global scope. 
////   * The most recent definition of the global variable of the specified name is overwritten.
////   * If varName contains a tag, this method will fail.
////   */
////  public Var setOrScopeMutable(String varName, JsonValue value)
////  {
////    Var var = scopeMutable(varName, SchemaFactory.anySchema());
////    var.setValue(value);
////    return var;
////  }
////
////  public String getName()
////  {
////    if( parent == null )
////    {
////      return name;
////    }
////    return parent.getName() + "::" + name;
////  }
////
////}
////
//////
//////
//////  
//////  protected Namespace()
//////  {
//////    // used by subclasses 
//////  }
//////  
//////  protected Namespace(String name) {
//////    this.name = name;
//////    importNamespace(SystemNamespace.getInstance());
//////    importAllFrom(SystemNamespace.getInstance());
//////  }
//////  
//////
//////  // -- importing ---------------------------------------------------------------------------------
//////
//////  public Namespace importModule(String moduleName)
//////  {
//////    ensureNotFinal();
//////    Module module = Module.findModule(this.module, moduleName);
//////    
//////  }
//////  
//////
//////  public void importNamespace(Namespace namespace) {
//////    ensureNotFinal();
//////		importedNamespaces.put(namespace.getName(), namespace);
//////  }
//////  
//////  public void importAllFrom(Namespace namespace) {
//////    ensureNotFinal();
//////    for (String varName : namespace.exports()) 
//////    {
//////  	  // FIXME: check for name clashes
//////  	  Var var = namespace.inscope(varName);
//////  	  importedVariables.put(varName, var);
//////		}
//////  }
//////  
//////  public void importFrom(Namespace namespace, ArrayList<String> varNames) {
//////    ensureNotFinal();
//////  	for (String varName : varNames) 
//////  	{
//////  		if (namespace.exports(varName)) 
//////  		{
//////  		  // FIXME: check for name clashes
//////  		  Var var = namespace.inscope(varName);
//////        importedVariables.put(varName, var);
//////  		}
//////  		else 
//////  		{
//////  			throw new RuntimeException(varName + " could not be found in module " + namespace);
//////  		}
//////		}
//////  }
//////
//////  
//////  // -- scoping -----------------------------------------------------------------------------------
//////  
//////  /**
//////   * Place a variable (back) in scope
//////   */
//////  public void scope(Var var)
//////  {
//////    ensureNotFinal();
//////    // var.setNamespace(this);
//////    unscope(var);
//////    var.varStack = variables.get(var.name());
//////    variables.put(var.name(), var);
//////  }
//////
//////  /** Creates a new local variable with the specified name and puts it into the local scope.
//////   * Previous definitions of variables of the specified name are hidden but not overwritten.
//////   * 
//////   * @param taggedName
//////   * @return
//////   */
//////  public Var scope(String taggedName)
//////  {
//////    ensureNotFinal();
//////    Var var = new Var(taggedName);
//////    scope(var);
//////    return var;
//////  }
//////
//////  /** Creates a new local variable with the specified name and schema and puts it into the local scope.
//////   * Previous definitions of variables of the specified name are hidden but not overwritten.
//////   * 
//////   * @param taggedName
//////   * @return
//////   */
//////  public Var scope(String taggedName, Schema varSchema)
//////  {
//////    ensureNotFinal();
//////    Var var = new Var(taggedName, varSchema);
//////    scope(var);
//////    return var;
//////  }
//////  
//////  /** Removes the specified variable from this scope. */
//////  public void unscope(Var var)
//////  {
//////    ensureNotFinal();
//////    
//////    Var oldVar = variables.get(var.name());
//////    if (oldVar == var)
//////    {
//////      if (var.varStack != null)
//////      {
//////        variables.put(var.name(), var.varStack);
//////      }
//////      else
//////      {
//////        variables.remove(var.name());
//////      }
//////    }
//////    else
//////    {
//////      // find the same-name variable defined right after var
//////      while (oldVar != null && oldVar.varStack != var)
//////      {
//////        oldVar = oldVar.varStack;
//////      }
//////      if (oldVar == null)
//////      {
//////        // not in scope
//////        return;
//////      }
//////      
//////      // and remove var from the chain
//////      oldVar.varStack = var.varStack;
//////    }
//////  }
//////  
//////  /** Returns the variable of the specified name, searching in both the local and the
//////   * imported namespaces (in this order).
//////   * 
//////   * @throws IndexOutOfBoundsException if the variable is not defined or hidden
//////   */
//////  public Var inscope(String taggedName)
//////  {
//////    return inscope(taggedName, false);
//////  }
//////
//////  /** Returns the variable of the specified name, searching in only the this namespaces.
//////   * 
//////   * @throws IndexOutOfBoundsException if the variable is not defined or hidden
//////   */
//////  public Var inscopeLocal(String taggedName)
//////  {
//////    return inscope(taggedName, true);
//////  }
//////  
//////  // helper for the above inscope methods
//////  private Var inscope(String taggedName, boolean local)
//////  {
//////    Var var = findVar(variables, taggedName);
//////    if (!local && var == null)
//////    {
//////      var = findVar(importedVariables, taggedName);
//////    }
//////    if (var == null)
//////    {
//////      throw new IndexOutOfBoundsException("variable not defined: " + taggedName);
//////    }
//////    if (var.isHidden())
//////    {
//////      throw new IndexOutOfBoundsException("variable is hidden in this scope: "
//////          + taggedName);
//////    }
//////    return var;
//////  }
//////  
//////  /** Returns the variable of the specified name, searching in only the imported namespaces 
//////   * of the specified name.
//////   * 
//////   * @throws IndexOutOfBoundsException if the variable is not defined or hidden
//////   */
//////  public Var inscopeImport(String namespace, String varName)
//////  {
//////    Namespace ns = importedNamespaces.get(namespace);
//////    if (ns == null)
//////    {
//////      throw new IllegalArgumentException("unknown namespace " + namespace);
//////    }
//////    if (!ns.exports(varName))
//////    {
//////      throw new IllegalArgumentException("namespace " + namespace 
//////          + " does not export a variable of name " + varName);
//////    }
//////    return ns.inscopeLocal(varName);
//////  }
//////  
//////  /** Search for a variable of the given name and tag in <code>varMap</code> and return it. Return 
//////   * null if not found. */
//////  protected static Var findVar(Map<String, Var> varMap, String taggedName)
//////  {
//////    String[] split = Var.splitTaggedName(taggedName);
//////    String name = split[0];
//////    String tag = split[1];
//////    
//////    if (varMap.containsKey(name))
//////    {
//////      Var var = varMap.get(name);
//////      if (tag != null)
//////      {
//////        while (var != null && !tag.equals(var.tag()))
//////        {
//////          var = var.varStack;
//////        }
//////      }
//////      return var; // might be null
//////    }
//////    return null;
//////  }
//////
//////  // -- getters -----------------------------------------------------------------------------------
//////  
//////	public Module getModule() {
//////		return module;
//////	}
//////	
//////  public boolean isFinal()
//////  {
//////    return isFinal;
//////  }
//////  
//////  public void makeFinal()
//////  {
//////    if (isFinal()) return;
//////    for (Var v : variables.values())
//////    {
//////      // v.makeFinal();
//////      assert v.isFinal();
//////    }
//////    isFinal = true;
//////  }
//////  
//////  protected void ensureNotFinal()
//////  {
//////    if (isFinal())
//////    {
//////      throw new IllegalStateException("the final namespace " + name + " cannot be changed");
//////    }
//////  }
//////  
//////  public String getName()
//////  {
//////    return name;
//////  }
//////  
//////  public Set<String> exports()
//////  {
//////    return Collections.unmodifiableSet(exportedVariables);
//////  }
//////  
//////  public boolean exports(String varName)
//////  {
//////    return exportedVariables.contains(varName);
//////  }
//////
//////  /* Return a list of all inscope variables */
//////  public Collection<Var> listVariables(boolean local)
//////  {
//////    TreeMap<String, Var> map = new TreeMap<String, Var>();
//////    if( ! local )
//////    {
//////      map.putAll(importedVariables);
//////    }
//////    map.putAll(variables);
//////    return map.values();
//////  }
//////  
//////  // -- Namespace loading -------------------------------------------------------------------------
//////	
//////	public static Namespace get(String name) {
//////		return NamespaceLoader.get(name);
//////	}
//////	
//////	public static void clearNamespaces() {
//////		NamespaceLoader.clear();
//////	}
//////	
//////	/**
//////	 * This inner class handles all operations related to reading modules from the
//////	 * filesystem.
//////	 * 
//////	 */
//////	private static final class NamespaceLoader {
//////		private static HashMap<String, Namespace> namespaces = new HashMap<String, Namespace>();
//////	  
//////	 	private static void add(Namespace e) {
//////	 	  if (e.getName() == null)
//////	 	  {
//////	 	    throw new NullPointerException("namespace name must not be null");
//////	 	  }
//////	 	  if (namespaces.containsKey(e.getName()))
//////	 	  {
//////	 	   throw new IllegalArgumentException("a namespace with name " + e.getName() + " is already loaded");
//////	 	  }
//////	  	namespaces.put(e.getName(), e);
//////	  }
//////
//////	 	public static void clear() {
//////	 		namespaces.clear();
//////	 	}
//////	 	
//////		public static Namespace get(String name) {
//////		  Namespace namespace = namespaces.get(name);
//////	  	if (namespace == null) {
//////	  		namespace = init(name);
//////	  		add(namespace);
//////	  	}
//////	  	return namespace;
//////	  }
//////		
//////	  private static Namespace init(String name) {
//////	    if (SystemNamespace.NAME.equals(name)) 
//////	    {
//////	      return SystemNamespace.getInstance();
//////	    }
//////	    
//////	    Module module = DefaultModule.findModule(name);
//////	  	if (module == null) {
//////	  		throw new RuntimeException("Module not found " + name);
//////	  	}
//////	  	
//////	  	Namespace namespace = new Namespace(name);
//////	  	namespace.module = module;
//////	  	
//////	  	try {
//////	  		// load imports
//////	  		module.loadImports(namespace);
//////		  	
//////		  	// load jar files
//////	  		ClassLoaderMgr.addExtensionJars(module.getJarFiles());
//////		  	
//////		  	// load jaql files
//////		  	ChainedReader jaqlIn = new ChainedReader();
//////		  	for (File jaql : module.getJaqlFiles()) {
//////              jaqlIn.add(new InputStreamReader(new FileInputStream(jaql), "UTF-8"));
//////		  	}
//////		  	load(name, jaqlIn, namespace);
//////		  	
//////		  	// set exports
//////		  	Set<String> varNames = module.exports();
//////		  	if(!varNames.isEmpty()) {
//////		  		for (String varName : varNames) {
//////		  				if (namespace.variables.containsKey(varName)) 
//////		  				{
//////		  					namespace.exportedVariables.add(varName);
//////		  					continue;
//////		  				}
//////		  				else {
//////		  					throw new RuntimeException("export " + varName + " does not exist");
//////		  				}
//////		  		}
//////		  	} else {
//////		  		namespace.exportedVariables.addAll(namespace.variables.keySet());
//////		  	}
//////		  	
//////		  	// finalize
//////		  	namespace.makeFinal();		  	
//////		  	return namespace;
//////			} catch (FileNotFoundException e) {
//////				throw new RuntimeException(e);
//////			} catch (ParseException e) {
//////				throw new RuntimeException(e);
//////			} catch (Exception e) {
//////				throw new RuntimeException(e);
//////			}
//////	  }
//////	  
//////	  private static void load(String name, Reader in, Namespace namespace)
//////	  {
//////			JaqlLexer lexer = new JaqlLexer(in);
//////			
//////			Context context = new Context();
//////		  JaqlParser parser = new JaqlParser(lexer);
//////		  parser.env.globals = namespace;
//////		  lexer.setFilename(name);
//////		  
//////		  // parse jaql file
//////		  while(true) {
//////		  	parser.env.reset();
//////		    context.reset(); 
//////		    try {
//////					Expr expr = parser.parse();
//////					if (expr != null) {
//////						if ( expr instanceof AssignExpr )
//////						{
//////							expr.eval(context);
//////						}
//////						else {
//////							throw new RuntimeException("module files should only contain assignments; expr " 
//////							    + expr + " is not allowed");
//////						}
//////					}
//////				} catch (Exception e) {
//////					throw new RuntimeException(e);
//////				}
//////				if( parser.done )
//////		    {
//////		      break;
//////		    }
//////		  }
//////		}
//////	}
//////
//////}
