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
import static com.ibm.jaql.json.type.JsonType.*;

/**
 * 
 */
public final class Var extends Object
{
  public enum Usage
  {
    EVAL,     // Variable may be referenced multiple times (value must be stored)
    STREAM,   // Variable is an array that is only referenced once (value may be streamed)
    UNUSED    // Variable is never referenced
  };
  
  public enum Type
  {
    UNDEFINED,
    VALUE,
    ITERATOR,
    EXPR
  }

  public static final Var[] NO_VARS = new Var[0];
  public static final Var UNUSED = new Var("$__unused__");

  private String name;
  private boolean hidden = false; // variable not accessible in current parse
                                 // context (this could reuse Usage)
  public Var varStack; // Used during parsing for vars of the same name;
                       // contains the a list of previous definitions of this
                       // variable
  private Expr expr; // only for global variables
  private Usage usage = Usage.EVAL;
  private Type type = Type.UNDEFINED;
  
  private Object value; // Runtime value: JsonValue (null allowed) or
                       // JsonIterator(null disallowed)
  private Schema schema; // schema of the variable; not to be changed at runtime

  private boolean isGlobal = false;   // the variable is global, i.e., it
  private boolean isFinal = false;
  private Namespace namespace = null;
  
  public Var(String name, Schema schema)
  {
    assert schema != null;
    this.name = name;
    this.schema = schema;
  }

  public Var(String name)
  {
    this(name, SchemaFactory.anySchema());
  }

  /** Constant variable */
  public Var(String name, boolean isGlobal)
  {
    this(name, SchemaFactory.anySchema());
    this.type = Type.UNDEFINED;
    this.isGlobal = isGlobal;
  }

  public Var(String name, Schema schema, boolean isGlobal)
  {
    this(name, schema);
    this.type = Type.UNDEFINED;
    this.isGlobal = isGlobal;
  }

  /**
   * @return
   */
  public String name()
  {
    return name;
  }

  public void setName(String name)
  {
    this.name = name;
  }
  
  /**
   * @return
   */
  public boolean isGlobal()
  {
    return isGlobal;
  }

  public boolean isFinal()
  {
    return isFinal;
  }

  public void finalize()
  {
    if (type != Type.VALUE && type != Type.EXPR)
    {
      throw new IllegalStateException("final variables must be of type VALUE or EXPR");
    }
    isFinal = true;
  }
  
  /**
   * @param varMap
   * @return
   */
  public Var clone(VarMap varMap)
  {
    Var v = new Var(name, schema);
    v.usage = usage;
    // Cloning a Var does NOT clone its value!
    // It is NOT safe to share an iter unless one var is never evaluated.
    //if (type == Type.EXPR)
    //{
      // TODO: do we need to clone a Var with an expr? Do we need to clone the
      // Expr?
      //throw new RuntimeException("cannot clone variable with Expr");
      // v.expr = expr.clone(varMap); // TODO: could we share the expr?
    //}
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
    this.type = Type.UNDEFINED;
  }

  /**
   * Set the runtime value.
   * 
   * @param value
   */
  public void setValue(JsonValue value)
  {
    if (isFinal()) throw new IllegalStateException("final variables cannot be modified");
    assert schema.matchesUnsafe(value) : name + " has invalid schema: "
        + "found " + value + ", expected " + schema;
    this.value = value;
    this.type = Type.VALUE;
  }

  public void setExpr(Expr expr)
  {
    if (isFinal()) throw new IllegalStateException("final variables cannot be modified");
    this.expr = expr;
    this.type = Type.EXPR;
  }
  
  /**
   * Set the runtime value.
   * 
   * @param var
   * @param bytes
   */
  public void setIter(JsonIterator iter)
  {
    if (isFinal()) throw new IllegalStateException("final variables cannot be modified");
    assert iter != null;
    assert schema.is(ARRAY).maybe();
    value = iter;
    type = Type.ITERATOR;
  }

  /**
   * Set the variable's value to the result of the expression. If the variable
   * is unused, the expression is not evaluated. If the variable is streamable
   * and the expression is known to produce an array, the expr is evaluated
   * lazily using an Iter.
   * 
   * @param expr
   * @param context
   * @throws Exception
   */
  public void setEval(Expr expr, Context context) throws Exception
  {
    if (isFinal()) throw new IllegalStateException("final variables cannot be modified");
    if (usage == Usage.STREAM && expr.getSchema().is(ARRAY, NULL).always())
    {
      setIter(expr.iter(context));
    } else if (usage != Usage.UNUSED)
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
    if (value instanceof JsonValue)
    {
      setValue((JsonValue) value);
    } else if (value instanceof JsonIterator)
    {
      setIter((JsonIterator) value);
    } else if (value instanceof Expr)
    {
      setEval((Expr) value, context);
    } else
    {
      throw new InternalError("invalid variable value: " + value);
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
    switch (type)
    {
    case VALUE:
      // assert schema.matchesUnsafe(v); // already checked
      return (JsonValue) value;
    case ITERATOR:
      JsonIterator iter = (JsonIterator) value;
      JsonValue result;
      if (iter.isNull())
      {
        result = null;
      } else
      {
        SpilledJsonArray arr = new SpilledJsonArray();
        arr.setCopy(iter);
        result = arr;
      }
      // TODO: remove assertion? check can be expensive when large arrays are
      // put in var's
      assert schema.matchesUnsafe(result);
      if (!isGlobal())
      {
        value = result;
        type = Type.VALUE;
      }
      return result;
    case EXPR:
      JsonValue v = expr.eval(context);
      if (!isGlobal())
      {
        expr = null;
        value = v;
        type = Type.VALUE;
      }
      assert schema.matchesUnsafe(v);
      return v;
    }
    throw new IllegalStateException("undefined variable: " + name());
  }

  /**
   * Return an iterator over the (array) value. If the value is not an array, an
   * exception is raised. If this variable has STREAM usage, it is NOT safe to
   * request the value multiple times.
   * 
   * @return
   * @throws Exception
   */
  public JsonIterator iter(Context context) throws Exception
  {
    if (usage == Usage.STREAM && type == Type.ITERATOR)
    {
      JsonIterator iter = (JsonIterator) value;
      value = null; // set undefined
      return iter;
    }
    JsonArray arr = (JsonArray) getValue(context); // cast error intentionally
                                                   // possible
    if (arr == null)
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
    assert schema != null;
    this.schema = schema;
  }
  
  public Usage usage()
  {
    return usage;
  }
  
  public void setUsage(Usage usage)
  {
    this.usage = usage;
  }
  
  public Type type()
  {
    return type;
  }
  
  public JsonValue value()
  {
    return (JsonValue)value;
  }
  
  public Expr expr()
  {
    return expr;
  }
  
  public boolean isHidden()
  {
    return hidden;
  }
  
  public void setHidden(boolean hide)
  {
    this.hidden = hide;
  }
  
  public Namespace getNamespace()
  {
    return namespace;
  }
  
  public void setNamespace(Namespace namespace)
  {
    if (this.namespace != null && this.namespace != namespace)
    {
      throw new IllegalStateException("variable already has a namespace");
    }
    this.namespace = namespace;
  }
}
