/*
 * Copyright (C) IBM Corp. 2009.
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
package com.ibm.jaql.lang.expr.udf;

import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.Expr;


public class ScriptBlock extends Expr
{

  public ScriptBlock(Expr[] exprs)
  {
    super(exprs);
  }

  public ScriptBlock(String lang, String block)
  {
    super(new ConstExpr(new JsonString(lang)), new ConstExpr(new JsonString(block)));
  }

  @Override
  public JsonValue eval(Context context) throws Exception
  {
    throw new UnsupportedOperationException("scripting is disabled");
  }
}

//========================================================
// This version uses javax.scripting
//public class ScriptBlock extends Expr
//{
//
//  public ScriptBlock(Expr[] exprs)
//  {
//    super(exprs);
//  }
//
//  public ScriptBlock(String lang, String block)
//  {
//    super(new ConstExpr(new JString(lang)), new ConstExpr(new JString(block)));
//  }
//
//  @Override
//  public Item eval(Context context) throws Exception
//  {
//    JString lang   = (JString)exprs[0].eval(context).getNonNull();
//    JString jblock = (JString)exprs[1].eval(context).get();
//    String block = jblock.toString();
//    System.err.println("running "+lang+":\n"+block);
//    
//    //com.sun.script.jython.JythonScriptEngineFactory f = new com.sun.script.jython.JythonScriptEngineFactory();
//    //System.out.println(f.getLanguageName());
//    
//    ScriptEngineManager manager = new ScriptEngineManager();
//    ScriptEngine engine = manager.getEngineByName(lang.toString());
//    if( engine == null )
//    {
//      throw new RuntimeException("scripting language not found: "+lang);
//    }
//
//    Object result = engine.eval(block);
//    System.out.println("got: >>"+result+"<<");
//
////    Invocable inv = (Invocable)engine;
////    result = inv.invokeFunction("foo", lang.toString(), "jaql");
////    System.out.println("got2: >>"+result+"<<");
////    result = toJaql(result);
////    if( result instanceof List )
////    {
////      List<Object> a = (List)result;
////    }
//
//    //result = inv.invokeFunction("cnt", new long[]{7,8,9});
////    System.out.println("got3: >>"+result+"<<");
//
////    ArrayList<String> al = new ArrayList<String>(); al.add("one"); al.add("two"); al.add("three");
////    result = inv.invokeFunction("cnt", al);
////    System.out.println("got4: >>"+result+"<<");
//
//    return JBool.trueItem;
//  }
//
//  interface Converter
//  {
//    Object convert(Object obj);
//  }
//  
//  static HashMap<Class<?>, Converter> toJaql = new HashMap<Class<?>, Converter>();
//  static {
//    try
//    {
//      toJaql.put( Class.forName("sun.org.mozilla.javascript.internal.NativeArray"), new JavascriptNativeArrayToJaql() );
//    }
//    catch (ClassNotFoundException e)
//    {
//    }
//  }
//
//  static class JavascriptNativeArrayToJaql implements Converter
//  {
//    public ArrayList<Object> convert(Object obj)
//    {
//      sun.org.mozilla.javascript.internal.NativeArray na = (sun.org.mozilla.javascript.internal.NativeArray)obj;
//      long n = na.getLength();
//      ArrayList<Object> out = new ArrayList<Object>((int)n);
//      for(int i = 0 ; i < n ; i++)
//      {
//        Object x = na.get(i, null);
//        // x = toJaql(x);
//        out.add(x);
//      }
//      return out;
//    }
//  }
//  
//  
//  static Object toJaql(Object obj)
//  {
//    return toJaql.get(obj.getClass()).convert(obj);
//  }
//}

//========================================================
// This version uses Jython directly
//public class ScriptBlock extends Expr
//{
//
//  public ScriptBlock(Expr[] exprs)
//  {
//    super(exprs);
//  }
//
//  public ScriptBlock(String lang, String block)
//  {
//    super(new ConstExpr(new JString(lang)), new ConstExpr(new JString(block)));
//  }
//
//  @Override
//  public Item eval(Context context) throws Exception
//  {
//    JString jblock = (JString)exprs[1].eval(context).get();
//    String block = jblock.toString();
//    System.err.println("running:\n"+block);
//    // PythonInterpreter interp = new PythonInterpreter();
//    // interp.set("a", new PyInteger(42));
//    //interp.exec(block);
//    ////interp.exec("x = 2+2");
//    //PyObject x = interp.get("x");
//    
//    //PyObject x = interp.eval(block);
//    // TODO: convert PyObject directly to Item without parsing
//    //String s = x.toString();
//    //interp.cleanup();
//
//    String kind = "exec"; // eval, exec, single
//    String filename = "<jaql>";
//    CompilerFlags cflags = new CompilerFlags();
//    
////    modType node = parser.parse( 
////        new ByteArrayInputStream(PyString.to_bytes(block)),
////        kind, filename, cflags);
////    
////    if (node == null)
////    {
////      return Item.nil;
////    }
//    
//    PyObject code = Py.compile_flags(block, filename, kind, cflags);
//    if( code == Py.None )
//    {
//      return Item.nil;
//    }
//    
//    PyModule module = context.getPyModule();
//    PyObject locals = module.__dict__;
//    Py.exec(code, locals, locals);
//    
//    System.out.println("locals: [\n"+locals+"\n]");
//
//    //JsonParser parser = new JsonParser();
//    //Item item = parser.parse(s);
//    //return item;
//    return JBool.trueItem;
//  }
//
//}
