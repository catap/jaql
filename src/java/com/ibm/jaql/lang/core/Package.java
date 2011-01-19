/*
 * Copyright (C) IBM Corp. 2011.
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
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;

import com.ibm.jaql.lang.Jaql;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.parser.JaqlLexer;
import com.ibm.jaql.lang.parser.JaqlParser;

/** Packages are containers of modules */
public class Package
{
  protected final String name;
  protected final Package parent;
  protected final ArrayList<File> searchPath = new ArrayList<File>();
  protected final HashMap<String,Package> packs = new HashMap<String, Package>();
  protected final HashMap<String,Module> modules = new HashMap<String, Module>();
  
  
  protected Package(Package parent, String packName, File packDir)
  {
    assert packName != null && packName.length() > 0;
    this.name = packName;
    this.parent = parent;
    this.searchPath.add(packDir);
  }
  
  public Package()
  {
    this.name = "";
    this.parent = null;
    
    // default module path: either 
    //   1) java property jaql.module.path
    //   2) $JAQL_PATH
    //   3) $HOME/.jaql, $HOME/jaql/modules
    // We always add: $JAQL_HOME/jaql/modules
    String path = System.getProperty("jaql.module.path", System.getenv("JAQL_PATH"));
    if( path != null )
    {
      setModulePath(path);
    }
    else
    {
      String home = System.getProperty("user.home");
      addPath(home, "/.jaql");
      addPath(home, "/jaql/modules");
    }
    addPath(System.getenv(Jaql.ENV_JAQL_HOME), "/jaql/modules");
  }
  
  private void addPath(String path1, String path2)
  {
    if( path1 != null && path2.length() > 0 )
    {
      this.searchPath.add(new File(path1 + path2));
    }
  }

  public String getName()
  {
    if( parent == null )
    {
      return name;
    }
    return parent.getName() + "::" + name;
  }

  public Package getRoot()
  {
    Package p = this;
    while( p.parent != null )
    {
      p = p.parent;
    }
    return p;
  }
  
  protected Package loadPackage(String packName, File packDir)
  {
    Package p = packs.get(packName);
    if( p == null )
    {
      p = new Package(this, packName, packDir);
      String initModName = "_" + packName;
      File initFile = new File(packDir, initModName + ".jaql");
      if( initFile.isFile() )
      {
        p.parseModule(initModName, initFile);
      }
      // only register package if initialization succeeded.
      packs.put(packName, p);
    }
    else if( ! packDir.equals( p.searchPath.get(0) ) )
    {
      // ambiguous package
      throw new RuntimeException("ambiguous reference to package "+packName 
              + " " + p.searchPath.get(0) + " and " + packDir );
    }
    return p;
  }


  protected Module parseModule(String moduleName, File jaqlFile)
  {
    assert ! modules.containsKey(moduleName);
    if( ! jaqlFile.isFile() || ! jaqlFile.canRead() )
    {
      throw new RuntimeException("jaql module cannot be read from file: "+jaqlFile);
    }

    Module module = new Module(this, moduleName, jaqlFile);
    modules.put(moduleName, module);

    try
    {
      InputStreamReader reader = new InputStreamReader(new FileInputStream(jaqlFile), "UTF-8");
      JaqlLexer lexer = new JaqlLexer(reader);
      Context context = new Context();
      JaqlParser parser = new JaqlParser(lexer);
      parser.env.globals = module;
      lexer.setFilename(moduleName);

      // parse jaql file
      while( ! parser.done )
      {
        parser.env.reset();
        context.reset(); 
        Expr expr = parser.parse();
        if (expr != null) 
        {
          expr.eval(context);
        }
      }
      
      module.isFinal = true;
      return module;
    }
    catch( Exception ex )
    {
      modules.remove(moduleName);
      throw new RuntimeException("error loading module: "+moduleName+" from "+jaqlFile, ex);
    }
  }

  public void setModulePath(String[] searchPath)
  {
    this.searchPath.clear();
    String home = System.getProperty("user.home");
    for( String s: searchPath )
    {
      if( home != null && s.startsWith("~") )
      {
        s = home + "/" + s.substring(1); 
      }
      this.searchPath.add( new File(s) );
    }
  }
  
  public void setModulePath(String path)
  {
    setModulePath(path.split("[:;,]")); 
  }
}
