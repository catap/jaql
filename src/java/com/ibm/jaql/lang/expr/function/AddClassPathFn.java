/*
 * Copyright (C) IBM Corp. 2010.
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
package com.ibm.jaql.lang.expr.function;

import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.lang.core.Env;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.MacroExpr;


/** 
 * Add to the java class path
 * 
 *  supports wildcards on classpath items as described here:
 *     http://download.oracle.com/javase/6/docs/technotes/tools/windows/classpath.html
 *     
 *     Class path entries can contain the basename wildcard character *, which is 
 *     considered equivalent to specifying a list of all the files in the directory 
 *     with the extension .jar or .JAR. For example, the class path entry foo/* specifies
 *     all JAR files in the directory named foo. A classpath entry consisting simply 
 *     of * expands to a list of all the jar files in the current directory.
 *     
 *     A class path entry that contains * will not match class files. To match both 
 *     classes and JAR files in a single directory foo, use either foo;foo/* or foo/*;foo. 
 *     The order chosen determines whether the classes and resources in foo are 
 *     loaded before JAR files in foo, or vice versa.
 *     
 *     Subdirectories are not searched recursively. For example, foo/* looks for JAR 
 *     files only in foo, not in foo/bar, foo/baz, etc.
 *     
 *     The order in which the JAR files in a directory are enumerated in the expanded 
 *     class path is not specified and may vary from platform to platform and even 
 *     from moment to moment on the same machine. A well-constructed application should
 *     not depend upon any particular order. If a specific order is required then the
 *     JAR files can be enumerated explicitly in the class path.
 */
public class AddClassPathFn extends MacroExpr
{
	public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11
	{
	  public Descriptor()
	  {
	    super("addClassPath", AddClassPathFn.class);
	  }	   
	}
  
  public AddClassPathFn(Expr ... exprs)
  {
    super(exprs);
  }

  @Override
  public Expr expand(Env env) throws Exception
  {
    // TODO: need a way to let any Expr reference the current module
    String path = env.globals.getModulePathString();
    return new AddRelativeClassPathFn(new ConstExpr(new JsonString(path)), exprs[0]);
  }
}
