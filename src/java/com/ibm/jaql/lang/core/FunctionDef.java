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
//import java.io.ByteArrayOutputStream;
//import java.io.DataInput;
//import java.io.DataOutput;
//import java.io.IOException;
//import java.io.PrintStream;
//import java.io.StringReader;
//import java.util.ArrayList;
//import java.util.HashSet;
//
//import com.ibm.jaql.json.type.Item;
//import com.ibm.jaql.json.util.Iter;
//import com.ibm.jaql.lang.expr.core.BindingExpr;
//import com.ibm.jaql.lang.expr.core.DefineFunctionExpr;
//import com.ibm.jaql.lang.expr.core.DoExpr;
//import com.ibm.jaql.lang.expr.core.Expr;
//import com.ibm.jaql.lang.parser.JaqlLexer;
//import com.ibm.jaql.lang.parser.JaqlParser;
//import com.ibm.jaql.util.BaseUtil;
//
///**
// * This class is immutable because it is shared // TODO: when is it immutable?
// * not during compile...? At runtime, yes.
// */
//public class FunctionDef
//{
//  protected DefineFunctionExpr fn;
//  protected boolean ownFn;
//  protected String fnText;
//
//  /**
//   * @param params
//   * @param body
//   * @throws Exception
//   */
//  public void set(DefineFunctionExpr fn, boolean ownFn) throws Exception
//  {
//    this.fn = fn;
//    this.ownFn = ownFn;
//
//    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
//    PrintStream ps = new PrintStream(outStream);
//    HashSet<Var> capturedVars = new HashSet<Var>();
//    fn.decompile(ps, capturedVars);
//    assert capturedVars.size() == 0;
//    this.fnText = outStream.toString();
//  }
//  
//  /**
//   * @return
//   */
//  public int getNumParameters()
//  {
//    return fn.numParams();
//  }
//
//  /**
//   * @param i
//   * @return
//   */
//  public Var param(int i)
//  {
//    return fn.param(i).var;
//  }
//
//  /**
//   * @return
//   */
//  public Expr getBody()
//  {
//    return fn.body();
//  }
//
//  /**
//   * @param context
//   * @param capturedValues
//   * @param args
//   * @return
//   * @throws Exception 
//   */
//  protected Context initEval(Context outerContext, Item[] capturedValues, Expr[] args)
//    throws Exception
//  {
//    if (params.length != args.length)
//    {
//      throw new RuntimeException(
//          "wrong number of arguments to function.  Expected " + params.length
//              + " but given " + args.length);
//    }
//    for (int i = 0; i < params.length; i++)
//    {
//      if( usage[i] == ParamUsage.STREAM && args[i].isArray().always() )
//      {
//        // TODO: the array check could be deferred until runtime by just setting the variable
//        // to the expression (plus the context) to capture more cases, especially a VarExpr arg.
//        context.setVar(params[i], args[i].iter(outerContext));
//      }
//      else if( usage[i] != ParamUsage.UNUSED ) // EVAL or STREAM
//      {
//        context.setVar(params[i], args[i].eval(outerContext));
//      }
//    }
//    return context;
//  }
//
//  /**
//   * @param context
//   * @param capturedValues
//   * @param args
//   * @return
//   */
//  protected Context initEval(Context context, Item[] capturedValues, Item[] args)
//  {
//    context = new Context(); // TODO: memory
//    if (params.length != args.length)
//    {
//      throw new RuntimeException(
//          "wrong number of arguments to function.  Expected " + params.length
//              + " but given " + args.length);
//    }
//    // context.push();
//    for (int i = 0; i < capturedVars.length; i++)
//    {
//      context.setVar(capturedVars[i], capturedValues[i]);
//    }
//    for (int i = 0; i < params.length; i++)
//    {
//      context.setVar(params[i], args[i]);
//    }
//    return context;
//  }
//
//  // TODO: need compile-time detection of streaming functions too...
//  /**
//   * @param context
//   * @param capturedValues
//   * @param args
//   * @return
//   * @throws Exception
//   */
//  public Item eval(Context context, Item[] capturedValues, Item[] args)
//      throws Exception
//  {
//    context = initEval(context, capturedValues, args);
//    Item item = body.eval(context);
//    //context.pop();
//    return item;
//  }
//
//  /**
//   * 
//   * @param context
//   * @param capturedValues
//   * @param args
//   * @return
//   * @throws Exception
//   */
//  public Item eval(Context context, Item[] capturedValues, Expr[] args)
//      throws Exception
//  {
//    context = initEval(context, capturedValues, args);
//    Item item = body.eval(context);
//    //context.pop();
//    return item;
//  }
//
//  /**
//   * @param context
//   * @param capturedValues
//   * @param args
//   * @return
//   * @throws Exception
//   */
//  public Iter iter(Context context, Item[] capturedValues, Item[] args)
//      throws Exception
//  {
//    context = initEval(context, capturedValues, args);
//    Iter iter = body.iter(context);
//    //context.pop();
//    return iter;
//  }
//
//  /**
//   * 
//   * @param context
//   * @param capturedValues
//   * @param args
//   * @return
//   * @throws Exception
//   */
//  public Iter iter(Context context, Item[] capturedValues, Expr[] args)
//      throws Exception
//  {
//    context = initEval(context, capturedValues, args);
//    Iter iter = body.iter(context);
//    //context.pop();
//    return iter;
//  }
//  /**
//   * @param out
//   * @param capturedValues
//   * @throws Exception
//   */
//  public void print(PrintStream out, Item[] capturedValues) throws Exception
//  {
//    boolean haveComputedCaptures = false;
//    for (int i = 0; i < capturedVars.length; i++)
//    {
//      if (capturedValues[i] != null)
//      {
//        haveComputedCaptures = true;
//      }
//    }
//    //    if( haveComputedCaptures )
//    //    {
//    //      // write the captured vars
//    //      String sep = "let ";
//    //      for( int i = 0 ; i < capturedVars.length ; i++ )
//    //      {
//    //        if( capturedValues[i] != null )
//    //        {
//    //          out.print(sep);
//    //          out.print(capturedVars[i].name);
//    //          out.print(" = (");
//    //          capturedValues[i].print(out, 4);
//    //          out.print(")");
//    //          sep = ",\n";
//    //        }
//    //      }
//    //      out.print("\nreturn ");
//    //    }
//    //    out.print("fn ");
//    //    if( fnVar != null )
//    //    {
//    //      out.print(fnVar.name);
//    //    }
//    //    out.print("(");
//    //    String sep = "";
//    //    for (Var p : params)
//    //    {
//    //      out.print(sep);
//    //      out.print(p.name);
//    //      sep = ", ";
//    //    }
//    //    out.println(") {");
//    //    out.print(bodyText);
//    //    out.println("\n}");
//
//    out.print("fn");
//    out.print("(");
//    String sep = "";
//    for (Var p : params)
//    {
//      out.print(sep);
//      out.print(p.name);
//      sep = ", ";
//    }
//    out.println(") (");
//
//    if (haveComputedCaptures)
//    {
//      // write the captured vars
//      for (int i = 0; i < capturedVars.length; i++)
//      {
//        if (capturedValues[i] != null)
//        {
//          out.print(capturedVars[i].name);
//          out.print(" = ");
//          capturedValues[i].print(out, 4);
//          out.println(",");
//        }
//      }
//      out.println();
//    }
//    out.println(bodyText);
//    out.println(")");
//  }
//
//  /**
//   * @param in
//   * @param capturedValues
//   * @return
//   * @throws Exception
//   */
//  public Item[] read(DataInput in, Item[] capturedValues) throws Exception
//  {
//    bodyText = in.readUTF();
//
//    JaqlLexer lexer = new JaqlLexer(new StringReader(bodyText)); // TODO: memory
//    JaqlParser parser = new JaqlParser(lexer); // TODO: memory
//    Env env = parser.env;
//
//    int numCaptures = BaseUtil.readVUInt(in);
//    if (numCaptures != capturedVars.length)
//    {
//      if (numCaptures == 0)
//      {
//        capturedVars = Var.NO_VARS;
//      }
//      else
//      {
//        capturedVars = new Var[numCaptures];
//      }
//    }
//    if (capturedValues == null || capturedValues.length != numCaptures)
//    {
//      if (numCaptures == 0)
//      {
//        capturedValues = Item.NO_ITEMS;
//      }
//      else
//      {
//        capturedValues = new Item[numCaptures];
//      }
//    }
//    for (int i = 0; i < numCaptures; i++)
//    {
//      String name = in.readUTF();
//      capturedVars[i] = env.scope(name);
//      capturedValues[i] = new Item(); // TODO: memory        
//      capturedValues[i].readFields(in);
//    }
//
//    int numParams = BaseUtil.readVUInt(in);
//    if (numParams != params.length)
//    {
//      if (numParams == 0)
//      {
//        params = Var.NO_VARS;
//      }
//      else
//      {
//        params = new Var[numParams];
//      }
//    }
//    for (int i = 0; i < numParams; i++)
//    {
//      String name = in.readUTF();
//      params[i] = env.scope(name);
//    }
//
//    body = parser.parse();
//    // Sadly, we need to decompile the expression because variables (viz the parameters) get renamed during parsing.
//    // TODO: revisit this.  Functions are pretty slow to move around.
//
//    annotate();
//    
//    return capturedValues;
//  }
//
//  /**
//   * @param out
//   * @param capturedValues
//   * @throws IOException
//   */
//  public void write(DataOutput out, Item[] capturedValues) throws IOException
//  {
//    out.writeUTF(bodyText);
//    BaseUtil.writeVUInt(out, capturedVars.length);
//    for (int i = 0; i < capturedVars.length; i++)
//    {
//      out.writeUTF(capturedVars[i].name);
//      capturedValues[i].write(out);
//    }
//    BaseUtil.writeVUInt(out, params.length);
//    for (int i = 0; i < params.length; i++)
//    {
//      out.writeUTF(params[i].name);
//    }
//  }
//
//}
