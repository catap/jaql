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

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.SpilledJsonArray;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.expr.core.Expr;

/**
 * 
 */
public class Var extends Object
{
  public enum Usage
  {
    EVAL(),    // Variable may be referenced multiple times (value must be stored) 
    STREAM(),  // Variable is an array that is only referenced once (value may be streamed)
    UNUSED()   // Variable is never referenced
  };
  
  public static final Var[] NO_VARS = new Var[0];
  public static final Var UNUSED = new Var("$__unused__");

  public String             name;
  public boolean            hidden  = false;     // variable not accessible in current parse context (this could reuse Usage)
  public Var                varStack;            // Used during parsing for vars of the same name; contains the a list of previous definitions of this variable
  public Expr               expr;                // only for global variables
  public Usage              usage = Usage.EVAL;
  
  public boolean            isDefined = false;   // variable defined?
  public Object             value;               // Runtime value: JsonValue (null allowed) or JsonIterator(null disallowed)
  private Schema       schema;             // schema of the variable; not to be changed at runtime
  
  public Var(String name, final Schema schema)
  {
    this.name = name;
    this.schema = schema;
  }
  /**
   * @param name
   */
  public Var(String name)
  {
    this(name, SchemaFactory.anyOrNullSchema());
  }

  /**
   * @return
   */
  public String name()
  {
    return name;
  }
  
  /**
   * @return
   */
  public boolean isGlobal()
  {
    return expr != null;
  }

  /*** Returns the name of this variable without the leading $, if present.
   */
  public String nameAsField()
  {
    return name.charAt(0) == '$' ? name.substring(1) : name;
  }

  /**
   * @param varMap
   * @return
   */
  public Var clone(VarMap varMap)
  {
    Var v = new Var(name);
    v.usage = usage;
    // Cloning a Var does NOT clone its value!
    // It is NOT safe to share an iter unless one var is never evaluated.
    if (expr != null)
    {
      // TODO: do we need to clone a Var with an expr? Do we need to clone the Expr?
      throw new RuntimeException("cannot clone variable with Expr");
      // v.expr = expr.clone(varMap); // TODO: could we share the expr? 
    }
    return v;
  }

  @Override
  public String toString()
  {
    return name + " @" + System.identityHashCode(this);
  }

  /**
   * Unset the runtime value
   */
  public void undefine()
  {
    this.value = null;
    isDefined = false;
  }
  
  /**
   * Set the runtime value.
   * 
   * @param value
   */
  public void setValue(JsonValue value)
  {
    assert schema.matchesUnsafe(value) : name + " has invalid schema: " + "found " + value + ", expected " + schema;
    this.value = value;
    isDefined = true;
  }

  /**
   * Set the runtime value.
   * 
   * @param var
   * @param value
   */
  public void setIter(JsonIterator iter)
  {
    assert iter != null;
    assert schema.isArray().maybe();
    value = iter;
    isDefined = true;
  }

  /**
   * Set the variable's value to the result of the expression.
   * If the variable is unused, the expression is not evaluated.
   * If the variable is streamable and the expression is known to produce an array,
   *   the expr is evaluated lazily using an Iter. 
   * 
   * @param expr
   * @param context
   * @throws Exception
   */
  public void setEval(Expr expr, Context context) throws Exception
  {
    if( usage == Usage.STREAM && expr.getSchema().isArrayOrNull().always() ) 
    {
      setIter(expr.iter(context));
    }
    else if( usage != Usage.UNUSED )
    {
      setValue(expr.eval(context));
    }
  }
  
  /**
   * 
   * @param var
   * @param value
   * @throws Exception
   */
  public void setGeneral(Object value, Context context) throws Exception
  {
    if( value instanceof JsonValue )
    {
      setValue((JsonValue)value);
    }
    else if( value instanceof JsonIterator )
    {
      setIter((JsonIterator)value);
    }
    else if( value instanceof Expr )
    {
      setEval((Expr)value, context);
    }
    else
    {
      throw new InternalError("invalid variable value: "+value);
    }
  }

  
  /**
   * Get the runtime value of the variable.
   * 
   * @return
   * @throws Exception
   */
  public JsonValue getValue(Context context) throws Exception
  {
    if (!isDefined)
    {
      throw new NullPointerException("undefined variable: "+name());
    }
    
    if( value instanceof JsonValue )
    {
      // assert schema.matchesUnsafe(v); // already checked
      return (JsonValue)value; 
    }
    else if( value instanceof JsonIterator )
    {
      JsonIterator iter = (JsonIterator)value;
      JsonValue result;
      if( iter.isNull() )
      {
        result = null;
      }
      else
      {
        SpilledJsonArray arr = new SpilledJsonArray();
        arr.setCopy(iter);
        result = arr;
      }
      // TODO: remove assertion? check can be expensive when large arrays are put in var's
      assert schema.matchesUnsafe(result); 
      value = result;
      return result;
    }
    else if( expr != null ) // TODO: merge value and expr? value is run-time; expr is compile-time
    {
      JsonValue v = expr.eval(context);
      expr = null;
      value = v;
      assert schema.matchesUnsafe(v);
      return v;
    }
    else if( value == null ) // value has been set to null explicitly 
    {
      return null;
    }
    throw new InternalError("bad variable value: "+name()+"="+value);
  }
  
  /**
   * Return an iterator over the (array) value.
   * If the value is not an array, an exception is raised.
   * If this variable has STREAM usage, it is NOT safe to request the value multiple times.
   * 
   * @return
   * @throws Exception
   */
  public JsonIterator iter(Context context) throws Exception
  {
    if( usage == Usage.STREAM && value instanceof JsonIterator )
    {
      JsonIterator iter = (JsonIterator)value;
      value = null; // set undefined
      return iter;
    }
    JsonArray arr = (JsonArray) getValue(context); // cast error intentionally possible
    if( arr == null )
    {
      return JsonIterator.NULL;
    }
    return arr.iter();
  }

  public Schema getSchema()
  {
    return schema;
  }
  
  /** Don't use at runtime! */
  public void setSchema(Schema schema)
  {
    this.schema = schema;
  }
}
