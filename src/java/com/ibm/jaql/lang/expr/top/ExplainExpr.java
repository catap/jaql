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
package com.ibm.jaql.lang.expr.top;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import com.ibm.jaql.io.Adapter;
import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Env;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.core.VarMap;
import com.ibm.jaql.lang.expr.core.ArrayExpr;
import com.ibm.jaql.lang.expr.core.BindingExpr;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.RecordExpr;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.lang.expr.function.DefineJaqlFunctionExpr;
import com.ibm.jaql.lang.expr.function.Function;
import com.ibm.jaql.lang.expr.function.JaqlFunction;
import com.ibm.jaql.lang.expr.hadoop.MRAggregate;
import com.ibm.jaql.lang.expr.hadoop.MapReduceBaseExpr;
import com.ibm.jaql.lang.expr.hadoop.MapReduceFn;
import com.ibm.jaql.lang.expr.io.AbstractReadExpr;
import com.ibm.jaql.lang.expr.io.AbstractWriteExpr;
import com.ibm.jaql.lang.expr.io.HadoopTempExpr;
import com.ibm.jaql.util.FastPrintBuffer;
import com.ibm.jaql.util.FastPrinter;

/**
 * 
 */
public class ExplainExpr extends EnvExpr
{
  public static final JsonString ID_KEY = new JsonString("id");;
  public static final JsonString LABEL_KEY = new JsonString("label");
  public static final JsonString QUERY_KEY = new JsonString("query");
  public static final JsonString FROM_KEY = new JsonString("from");
  public static final JsonString TO_KEY = new JsonString("to");
  public static final JsonString DIR_KEY = new JsonString("dir");

  // edge directions
  public static final JsonString DIRECTED   = new JsonString("dir");
  public static final JsonString UNDIRECTED = new JsonString("undi");
  public static final JsonString BIDIRECTED = new JsonString("bidi");

  /**
   * boolean explain expr
   * 
   * @param exprs
   */
  public ExplainExpr(Env env, Expr expr)
  {
    super(env, expr);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#decompile(java.io.PrintStream,
   *      java.util.HashSet)
   */
  public void decompile(FastPrinter exprText, HashSet<Var> capturedVars)
      throws Exception
  {
    exprText.print(kw("explain") + " ");
    exprs[0].decompile(exprText, capturedVars);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  // FIXME: Now done by ExplainHandler
  public JsonValue eval(Context context) throws Exception
  {
    FastPrintBuffer exprText = new FastPrintBuffer();
    HashSet<Var> capturedVars = new HashSet<Var>();
    exprs[0].decompile(exprText, capturedVars);
    if (!capturedVars.isEmpty()) // FIXME: change root expr from NoopExpr to QueryStmt
    {
      Iterator<Var> iter = capturedVars.iterator();
      while (iter.hasNext())
      {
        Var var = iter.next();
        if (var.isGlobal())
        {
          iter.remove();
        }
      }
      if (!capturedVars.isEmpty())
      {
        System.err.println("Invalid query... Undefined variables:");
        for (Var key : capturedVars)
        {
          System.err.println(key.taggedName());
        }
        System.err.println(exprText.toString());
        throw new RuntimeException("undefined variables");
      }
    }
    if( true )     // NOW: temporary hacking:
    {
      String query = exprText.toString();
      return new JsonString(query);
    }
    else
    {
      buildGraph();
      return graph;
    }
  }

  FastPrintBuffer exprText;
  HashSet<Var> capturedVars;
  BufferedJsonArray graph;
  HashMap<Expr,JsonRecord> exprNodeMap;
  HashMap<JsonValue,JsonRecord> constNodeMap;
   
  // FIXME: Move to the GraphExplainHandler
  public JsonArray buildGraph() throws Exception
  {
    exprText = new FastPrintBuffer();
    capturedVars = new HashSet<Var>();
    graph = new BufferedJsonArray();
    exprNodeMap = new HashMap<Expr,JsonRecord>();
    constNodeMap = new HashMap<JsonValue,JsonRecord>();
    Expr dup = exprs[0].clone(new VarMap());
    JsonRecord queryNode = makeNode("stmt", dup);
    buildGraphAux(getNodeId(queryNode), dup);
    return graph;
  }

  protected String decompile(Expr expr) throws Exception
  {
    exprText.reset();
    expr.decompile(exprText, capturedVars);
    exprText.flush();
    String query = exprText.toString();
    return query;
  }

  public static Expr findFileDescriptor(Expr expr)
  {
    while( true )
    {
      Expr prev = expr;
      if( expr instanceof VarExpr )
      {
        BindingExpr bind = ((VarExpr)expr).findVarDef();
        if( bind != null )
        {
          expr = bind.eqExpr();
        }
      }
      else if( expr instanceof AbstractWriteExpr )
      {
        expr = ((AbstractWriteExpr)expr).descriptor();
      }
      else if( expr instanceof MapReduceBaseExpr )
      {
        expr = ((MapReduceBaseExpr)expr).findArgument(MapReduceBaseExpr.OUTPUT_KEY);
      }
      else
      {
        return expr;
      }
      if( expr == null )
      {
        return prev;
      }
    }
  }
  
  protected void buildGraphAux(JsonString currentNodeId, Expr... exprs) throws Exception
  {
    for(Expr expr: exprs)
    {
      if( expr instanceof AbstractReadExpr )
      {
        AbstractReadExpr read = (AbstractReadExpr)expr;
        buildGraphAux(currentNodeId, read.children());
        JsonRecord fileNode = makeFdNode(findFileDescriptor(read.descriptor()));
        JsonRecord readNode = makeNode(expr.getClass().getSimpleName(), expr);
        makeEdge(fileNode, readNode, DIRECTED);
        makeEdge(readNode, currentNodeId, DIRECTED);
      }
      else if( expr instanceof AbstractWriteExpr )
      {
        AbstractWriteExpr write = (AbstractWriteExpr)expr;
        buildGraphAux(currentNodeId, allBut(write.children(), write.dataExpr()));
        JsonRecord fileNode = makeFdNode(findFileDescriptor(write.descriptor()));
        JsonRecord writeNode = makeNode(expr.getClass().getSimpleName(), expr);
        buildGraphAux(getNodeId(writeNode), write.dataExpr());
        makeEdge(currentNodeId, writeNode, DIRECTED);
        makeEdge(writeNode, fileNode, DIRECTED);
      }
      else if( expr instanceof MapReduceBaseExpr )
      {
        MapReduceBaseExpr mr = (MapReduceBaseExpr)expr;
        buildGraphAux(currentNodeId, mr.children());
        Expr inFd = mr.findArgument(MapReduceFn.INPUT_KEY);
        Expr outFd = mr.findArgument(MapReduceFn.OUTPUT_KEY);
        JsonRecord inputNode = makeFdNode(findFileDescriptor(inFd));
        JsonRecord outputNode = makeFdNode(findFileDescriptor(outFd));
        JsonRecord mrNode = makeNode(expr.getClass().getSimpleName(), expr);
        makeEdge(inputNode, mrNode, DIRECTED);
        makeEdge(mrNode, outputNode, DIRECTED);
        JsonString id = getNodeId(mrNode);
        
        if( mr instanceof MapReduceFn )
        {
          MapReduceFn mrfn = (MapReduceFn)mr;
          buildFnUseGraph( id, "map", mrfn.findArgument(MapReduceFn.MAP_KEY) );
          buildFnUseGraph( id, "combine", mrfn.findArgument(MapReduceFn.COMBINE_KEY) );
          buildFnUseGraph( id, "reduce", mrfn.findArgument(MapReduceFn.REDUCE_KEY) );
        }
        else if( mr instanceof MRAggregate )
        {
          buildFnUseGraph( id, "map", mr.findArgument(MRAggregate.MAP_KEY) );
          buildFnUseGraph( id, "combine", mr.findArgument(MRAggregate.AGGREGATE_KEY) );
          buildFnUseGraph( id, "reduce", mr.findArgument(MRAggregate.FINAL_KEY) );
        }
        else 
        {

        }
      }
      else if( expr instanceof DefineJaqlFunctionExpr )
      {
        JsonRecord fnNode = makeNode("fn", expr);
        exprNodeMap.put(expr, fnNode);
        buildGraphAux(getNodeId(fnNode), expr.children());
      }
      else if( expr instanceof ConstExpr )
      {
        buildFnGraph(currentNodeId, ((ConstExpr)expr).value);
      }
      else
      {
        buildGraphAux(currentNodeId, expr.children());
      }
    }
  }
  
  private static Expr[] allBut(Expr[] exprs, Expr toRemove)
  {
    Expr[] res = new Expr[exprs.length-1];
    for(int i = 0 ; i < exprs.length ; i++)
    {
      if( toRemove == exprs[i] ) 
      {
        System.arraycopy(exprs, i+1, res, i, res.length - i);
        break;
      }
      else
      {
        res[i] = exprs[i];
      }
    }
    return res;
  }

  private void buildFnUseGraph(JsonString id, String label, Expr fnExpr) throws Exception
  {
    if( fnExpr != null )
    {
      JsonRecord fnNode;
      
      if( fnExpr instanceof ArrayExpr )
      {
        for( Expr e: fnExpr.children() )
        {
          fnNode = exprNodeMap.get( e );
          if( fnNode == null )
          {
            if( e instanceof ConstExpr )
            {
              fnNode = constNodeMap.get( ((ConstExpr)e).value );
            }
          }
          makeEdge(id, getNodeId(fnNode), UNDIRECTED);
        }
        fnNode = makeNode("complex fn", fnExpr);
      }
      else if( fnExpr instanceof ConstExpr )
      {
        JsonValue v = ((ConstExpr)fnExpr).value;
        fnNode = constNodeMap.get( v );
        if( fnNode == null )
        {
          if( v instanceof JsonArray )
          {
            for( JsonValue f: ((JsonArray)v).iter() )
            {
              fnNode = constNodeMap.get( f );
              if( fnNode == null )
              {
                fnNode = makeNode("complex fn", fnExpr);
              }
              makeEdge(id, getNodeId(fnNode), UNDIRECTED);
            }
            return;
          }
          fnNode = makeNode("complex fn", fnExpr);
        }
      }
      else
      {
        fnNode = exprNodeMap.get(fnExpr);
        if( fnNode == null )
        {
          fnNode = makeNode("complex fn", fnExpr);
        }
      }
      // FIXME: hack for now 
      ((BufferedJsonRecord)fnNode).set(LABEL_KEY, 
          new JsonString(label + ": "+fnNode.get(LABEL_KEY).toString()));
      makeEdge(id, getNodeId(fnNode), UNDIRECTED);
    }
  }

  protected void buildFnGraph(JsonString currentNodeId, JsonValue value) throws Exception
  {
    if( value instanceof JsonArray )
    {
      for( JsonValue v: ((JsonArray)value).iter() )
      {
        buildFnGraph(currentNodeId, v);
      }
    }
    else if( value instanceof JsonRecord )
    {
      for( JsonValue v: JsonRecord.valueIter(((JsonRecord)value).iterator()) )
      {
        buildFnGraph(currentNodeId, v);
      }
    }
    else if( value instanceof Function )
    {
      if( value instanceof JaqlFunction )
      {
        buildGraphAux(currentNodeId, ((JaqlFunction)value).body());
      }
      else
      {
        JsonString fnStr = new JsonString( value.toString() ); 
        if( ! constNodeMap.containsKey(fnStr) )
        {
          constNodeMap.put(fnStr, makeNode("fn", new ConstExpr(value)));
        }
      }
    }
    // else atom ignored
  }
  
  protected JsonRecord makeNode(String label, Expr expr) throws Exception
  {
    String id = expr.getClass().getSimpleName()+"_"+System.identityHashCode(expr);
    String query = decompile(expr);
    BufferedJsonRecord node = new BufferedJsonRecord();
    node.add(ID_KEY, new JsonString(id));
    node.add(LABEL_KEY, new JsonString(label));
    node.add(QUERY_KEY, new JsonString(query));
    graph.add(node);
    return node;
  }

  protected JsonRecord makeFdNode(Expr fdExpr) throws Exception
  {
    JsonRecord node = exprNodeMap.get(fdExpr);
    if( node != null )
    {
      return node;
    }
    if( fdExpr instanceof ConstExpr )
    {
      JsonValue constFd = ((ConstExpr)fdExpr).value;
      node = constNodeMap.get(constFd);
      if( node != null )
      {
        return node;
      }
      if( constFd instanceof JsonArray )
      {
        node = makeNode("composite", fdExpr);
        for( JsonValue i: ((JsonArray) constFd).iter() )
        {
          JsonRecord part = makeFdNode(new ConstExpr(i));
          makeEdge(part, node, UNDIRECTED);
        }
      }
      else
      {
        String label = "opaque fd";
        if( constFd instanceof JsonRecord )
        {
          JsonRecord rec = (JsonRecord)constFd;
          JsonValue v = rec.get(Adapter.LOCATION_NAME);
          if( v instanceof JsonString )
          {
            label = ((JsonString)v).toString();
            int i = label.lastIndexOf('/');
            if( i > 20 )
            {
              label = label.substring(i+1);
            }
          }
        }
        node = makeNode(label, fdExpr);
      }
      constNodeMap.put(constFd, node);
    }
    else if( fdExpr instanceof ArrayExpr )
    {
      node = makeNode("composite", fdExpr);
      for( Expr e: fdExpr.children() )
      {
        JsonRecord part = makeFdNode(findFileDescriptor(e));
        makeEdge(part, node, UNDIRECTED);
      }
    }
    else if( fdExpr instanceof RecordExpr )
    {
      String label = "opaque fd";
      Expr e = ((RecordExpr)fdExpr).findStaticFieldValue(Adapter.LOCATION_NAME);
      if( e instanceof ConstExpr )
      {
        JsonValue v = ((ConstExpr)e).value;
        if( v instanceof JsonString )
        {
          label = ((JsonString)v).toString();
          int i = label.lastIndexOf('/');
          if( i > 20 )
          {
            label = label.substring(i+1);
          }
        }
      }
      node = makeNode(label, fdExpr);
    }
    else if( fdExpr instanceof HadoopTempExpr )
    {
      node = makeNode("temp", fdExpr);
    }
    else
    {
      node = makeNode("computed fd", fdExpr);
    }
    exprNodeMap.put(fdExpr, node);
    return node;
  }

  protected void makeEdge(JsonString fromId, JsonString toId, JsonString direction) throws Exception
  {
    BufferedJsonRecord edge = new BufferedJsonRecord(3);
    edge.add(FROM_KEY, fromId);
    edge.add(TO_KEY, toId);
    edge.add(DIR_KEY, direction);
    graph.add(edge);
  }

  protected void makeEdge(JsonRecord fromNode, JsonRecord toNode, JsonString direction) throws Exception
  {
    makeEdge(getNodeId(fromNode), getNodeId(toNode), direction);
  }

  protected void makeEdge(JsonString fromNodeId, JsonRecord toNode, JsonString direction) throws Exception
  {
    makeEdge(fromNodeId, getNodeId(toNode), direction);
  }

  protected void makeEdge(JsonRecord fromNode, JsonString toNodeId, JsonString direction) throws Exception
  {
    makeEdge(getNodeId(fromNode), toNodeId, direction);
  }

  protected static JsonString getNodeId(JsonRecord node)
  {
    return (JsonString)node.getRequired(ID_KEY);
  }
}
