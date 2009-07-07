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
package com.ibm.jaql.lang.expr.core;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;

/** Describes the formal parameters of a function. Each function has up to a number of required 
 * parameters, followed by a number of non-required parameters. All parameters are named and
 * non-required parameters have a default value.
 */
public class Parameters
{
  private static final JsonString[] NO_PARS =  new JsonString[0];
  private static final Schema[] NO_PARS_SCHEMATA =  new Schema[0];
  private static final Map<JsonString, Integer> NO_PARS_MAP 
    = Collections.unmodifiableMap(new HashMap<JsonString, Integer>(0));
  
  private JsonString[] names = NO_PARS;                       // all arguments
  private Schema[] schemata = NO_PARS_SCHEMATA;
  private JsonValue[] defaultValues = NO_PARS;                // non-required arguments only
  private int noRequired = 0;
  private Map<JsonString, Integer> positions = NO_PARS_MAP;   // all arguments
  
  
  // -- construction ------------------------------------------------------------------------------
  
  /** Constructor.
   * 
   * @param names the parameter names
   * @param schemata the schema of the arguments
   * @param defaultValues the default values. The number of required parameters is determined by 
   *        <code>names.length-defaultValue.length</code>
   */
  public Parameters(JsonString[] names, Schema[] schemata, JsonValue[] defaultValues)
  {
    init(names, schemata, defaultValues);
  }
  
  /** Constructor.
   * 
   * @param names the parameter names
   * @param schemata the schema of the arguments
   * @param defaultValues the default values. The number of required parameters is determined by 
   *        <code>names.length-defaultValue.length</code>
   */
  public Parameters(JsonString[] names, String[] schemata, JsonValue[] defaultValues) 
  {
    Schema[] s = new Schema[schemata.length];
    for (int i=0; i<schemata.length; i++)
    {
      try
      {
        s[i] = Schema.parse(schemata[i]);
      } catch (IOException e)
      {
        throw new RuntimeException(e);
      }
    }
    init(names, s, defaultValues);
  }
  
  /** Constructor, empty parameter list. */
  public Parameters()
  {
  }
  
  /** Check validity of parameters and, if valid, initialize the internal data structures */
  private void init(JsonString[] names, Schema[] schemata, JsonValue[] defaultValues)
  {
    this.names = names;
    this.schemata = schemata;
    this.defaultValues = defaultValues;
    this.noRequired = names.length - defaultValues.length;

    // initial checks
    if (names.length != schemata.length)
    {
      throw new IllegalArgumentException(
          "number of parameters does not match number of supplied schemata");
    }
    if (noRequired < 0)
    {
      throw new IllegalArgumentException("there are more default values than parameters");
    }
    
    // copy all argument names & check schemata
    if (names.length > 0)
    {
      positions = new HashMap<JsonString, Integer>(0);
      for (int i=0; i<names.length; i++) 
      {
        JsonString name = names[i];
        if (name == null)
        {
          throw new IllegalArgumentException("parameter name cannot be null");
        }
        if (positions.put(name, i) != null)
        {
          throw new IllegalArgumentException("parameter name not unique: " + name);
        }
        if (schemata[i]==null) // be friendly with emtpy schemata
        {
          schemata[i] = SchemaFactory.anyOrNullSchema();
        }
      }
    
    }
  }
  
  
  // -- getters -----------------------------------------------------------------------------------

    
  /** Total number of parameters */
  public int noParameters()
  {
    return names.length;
  }

  /** Number of required parameters */
  public int noRequiredParamters()
  {
    return noRequired;
  }
  
  /** Returns the position of the specified parameter or <code>null</code> if the parameter does
   * not exist. */
  public Integer positionOf(JsonString name)
  {
    return positions.get(name);
  }

  /** Checks whether the parameter at the specified position is required */
  public boolean isRequired(int position)
  {
    return position<noRequired;
  }
  
  /** Returns the name of the parameter at the specified position */
  public JsonString nameOf(int position)
  {
    return names[position];
  }
  
  /** Returns the default value of the non-required parameter at the specified position */
  public JsonValue defaultOf(int position)
  {
    return defaultValues[position-noRequired]; 
  }

  /** Returns the default value of the non-required parameter with the specified name */
  public JsonValue defaultOf(JsonString name)
  {
    return defaultOf(positionOf(name)); 
  }

  /** Returns the schema expected for the parameter at the specified position */
  public Schema schemaOf(int position)
  {
    return schemata[position];
  }
  
  /** Returns the schema expected for the parameter with the specified name  */
  public Schema schemaOf(JsonString name)
  {
    return schemaOf(positionOf(name));
  }
  
  
  // -- utils -------------------------------------------------------------------------------------
 
  /** If <code>args</code> is non-null and contains a field of name <code>parName</code>, returns
   * the value of this field. Otherwise, returns the default value of the specified parameter
   * (or throws an exception, if this parameter does not have a default value). */
  public JsonValue argumentOrDefault(JsonString parName, JsonRecord args)
  {
    int index = args == null ? -1 : args.findName(parName); 
    if (index >= 0)
    {
      return args.getValue(index);
    }
    else
    {
      int position = positions.get(parName); // intentional null pointer exception
      return defaultOf(position);
    }
  }
}
