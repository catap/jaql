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
package com.ibm.jaql.lang.expr.io;

import java.lang.reflect.UndeclaredThrowableException;

import com.ibm.jaql.io.Adapter;
import com.ibm.jaql.io.InputAdapter;
import com.ibm.jaql.io.OutputAdapter;
import com.ibm.jaql.io.hadoop.HadoopInputAdapter;
import com.ibm.jaql.io.hadoop.HadoopOutputAdapter;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.expr.core.ArrayExpr;
import com.ibm.jaql.lang.expr.core.BindingExpr;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.RecordExpr;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.lang.expr.hadoop.MapReduceBaseExpr;
import com.ibm.jaql.lang.util.JaqlUtil;
import com.ibm.jaql.util.ClassLoaderMgr;

/**
 * Utility methods for MapReduce and rewrites
 */
public class MapReducibleUtil
{

  /**
   * Test whether the given expression can be used as an argument to mapReduce.
   * You must specify if the expression is an input or an output expression.
   * 
   * @param input
   * @param e
   * @return
   */
  public static boolean isMapReducible(boolean input, Expr e)
  {
    if( e instanceof VarExpr )
    {
      VarExpr ve = (VarExpr)e;
      BindingExpr def = ve.findVarDef();
      e = def.eqExpr();
    }
    if (e instanceof HadoopTempExpr || e instanceof MapReduceBaseExpr)
    {
      return true;
    }
    if (e instanceof WriteFn)
    {
      return ((WriteFn) e).isMapReducible();
    }

    if (e instanceof AbstractHandleFn) return ((AbstractHandleFn) e).isMapReducible();
    if (e instanceof ArrayExpr) return isMapReducible(input, (ArrayExpr) e);
    if (e instanceof JaqlTempFn) return ((JaqlTempFn) e).isMapReducible();
    if (e instanceof RecordExpr) return isMapReducible(input, (RecordExpr) e);
    if (e instanceof ConstExpr) return isMapReducible(input, ((ConstExpr) e).value);

    return false;

    // FIXME: this is for unions (ie, stRead( [{fd1},{fd2}] ), but we need to allow nested arrays in map/reduce then.
    //    boolean mapReducible = false;
    //    if( e instanceof ListExpr) {
    //      ListExpr le = (ListExpr) e;
    //      int numChildren = le.exprs.length;
    //      if (numChildren > 0) mapReducible = true;
    //      for(int i = 0; i < numChildren; i++) {
    //        if( !(le.exprs[i] instanceof RecordExpr) ) {
    //          mapReducible = false;
    //          break;
    //        } else if (!isMapReducible(input, (RecordExpr)le.exprs[i])) {
    //          mapReducible = false;
    //          break;
    //        }
    //      }
    //    }
    //    return mapReducible;
  }
  
  /**
   * @param input
   * @param recExpr
   * @return
   */
  public static boolean isMapReducible(boolean input, RecordExpr recExpr)
  {
    // walk the expressions, being conservative

    // expect the top-level expression to be a RecordExpression
    // TODO: look for a map-reduce expression
    // TODO: write map-reducible input
    try
    {
      Expr adapterField = null;
      // expect the RecordExpression to have one of the following or both
      // 1. a field name that is a ConstExpr with the String 'type'
      Expr typeField = recExpr.findStaticFieldValue(Adapter.TYPE_NAME);
      // 2. a field name that is a ConstExpr with the String INOPTIONS_NAME or OUTOPTIONS_NAME 
      //    that is of type RecordExpr that has a field name that is a ConstExpr with the String ADAPTER_NAME
      Expr optField = null;
      if (input)
        optField = recExpr.findStaticFieldValue(Adapter.INOPTIONS_NAME);
      else
        optField = recExpr.findStaticFieldValue(Adapter.OUTOPTIONS_NAME);

      if (optField instanceof RecordExpr)
      {
        adapterField = ((RecordExpr) optField)
            .findStaticFieldValue(Adapter.ADAPTER_NAME);
      }
      if (adapterField == null)
      {
        // test it from the registry
        if (typeField instanceof ConstExpr)
        {
          String typeName = ((ConstExpr) typeField).value.toString();
          if (input)
          {
            Class<?> c = JaqlUtil.getAdapterStore().input
                .getAdapterClass(typeName);
            if (HadoopInputAdapter.class.isAssignableFrom(c)) return true;
          }
          else
          {
            Class<?> c = JaqlUtil.getAdapterStore().output
                .getAdapterClass(typeName);
            if (HadoopOutputAdapter.class.isAssignableFrom(c)) return true;
          }
        }
        return false;
      }
      else
      {
        // get the class string
        if (adapterField instanceof ConstExpr)
        {
          String adapterName = ((ConstExpr) adapterField).value.toString();
          Class<?> adapterClass = ClassLoaderMgr.resolveClass(adapterName);
          //Class<?> adapterClass = Class.forName(adapterName);
          if (input)
          {
            if (HadoopInputAdapter.class.isAssignableFrom(adapterClass))
              return true;
          }
          else
          {
            if (HadoopOutputAdapter.class.isAssignableFrom(adapterClass))
              return true;
          }
        }
      }
    }
    catch (Exception x)
    {
    }
    return false;
  }
  
  /**
   * An array descriptor is MapReducible if every descriptor in the array is.
   * @param input
   * @param arrayExpr
   * @return
   */
  public static boolean isMapReducible(boolean input, ArrayExpr arrayExpr)
  {
    if( arrayExpr.numChildren() == 0 )
    {
      return false;
    }
    for( Expr e: arrayExpr.children() )
    {
      if( ! isMapReducible(input, e) )
      {
        return false;
      }
    }
    return true;
  }

  public static boolean isMapReducible(boolean input, JsonValue descriptor) // TODO: throws Exception
  {
    try
    {
      if(input)
      {
        InputAdapter adapter = (InputAdapter) JaqlUtil.getAdapterStore().input.getAdapter(descriptor);
        return adapter instanceof HadoopInputAdapter;
      }
      else
      {
        OutputAdapter adapter = (OutputAdapter) JaqlUtil.getAdapterStore().output.getAdapter(descriptor);
        return adapter instanceof HadoopOutputAdapter;
      }
    }
    catch(Exception e)
    {
      throw new UndeclaredThrowableException(e); // TODO: shouldn't need wrapper here
    }
  }
}
