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
package com.ibm.jaql.lang.expr.function;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonString;

/** Describes the formal parameters of a function. Each function has a list of required 
 * parameters, followed by either a list of optional parameters or a repeating parameter. All 
 * parameters are named and optional parameters have a default value.
 * 
 * @param T type of the default value
 */
@SuppressWarnings("unchecked")
public abstract class Parameters<T>
{
  private static final Parameter[] NO_PARS =  new Parameter[0];
  private static final Map<JsonString, Integer> NO_PARS_MAP 
    = Collections.unmodifiableMap(new HashMap<JsonString, Integer>(0));
  
  /** List of formal parameters in order */
  private Parameter<T>[] parameters = NO_PARS;                
  
  /** Number of required parameters */
  private int noRequired = 0;
  
  /** Map from paramter names to positions */
  private Map<JsonString, Integer> positions = NO_PARS_MAP;   
  
  // -- construction ------------------------------------------------------------------------------
  
  /** Constructor, empty parameter list. */
  public Parameters()
  {
    init(NO_PARS);
  }
  
  /** Constructor. 
   * 
   * @param parameters the parameters in order (required parameters must go first)
   */
  public Parameters(Parameter<T> ... parameters)
  {
    init(parameters);
  }

  /** Constructor for parameter list without default values and arbitrary schema.
   * 
   * @param names the parameter names
   * @param schemata the schema of the arguments
   */
  public Parameters(JsonString ... names)
  {
    init(names, new Schema[names.length], newArrayOfT(0));
  }

  /** Constructor for parameter list without default values and arbitrary schema.
   * 
   * @param names the parameter names
   * @param schemata the schema of the arguments
   */
  public Parameters(String ... names)
  {
    init(names, new Schema[names.length], newArrayOfT(0));
  }
  
  
  /** Constructor. The first <code>min</code> arguments have to be provided, the other
   * arguments default to the specified value.
   * 
   * @param names the parameter names
   * @param schemata the schema of the arguments
   * @param defaultValues the default values. The number of required parameters is determined by 
   *        <code>names.length-defaultValue.length</code>
   */
  public Parameters(JsonString[] names, int min, T defaultValue) 
  {
    Schema[] schemata = new Schema[names.length];
    Arrays.fill(schemata, SchemaFactory.anySchema());
    T[] defaultValues = newArrayOfT(names.length-min);
    Arrays.fill(defaultValues, defaultValue);
    init(names, schemata, defaultValues);
  }

  /** Check validity of parameters and, if valid, initialize the internal data structures */
  private void init(JsonString[] names, Schema[] schemata, T[] defaultValues)
  {
    // initial checks
    if (names.length != schemata.length)
    {
      throw new IllegalArgumentException(
          "number of names does not match number of supplied schemata");
    }
    int noRequired = names.length - defaultValues.length; 
    if (noRequired < 0)
    {
      throw new IllegalArgumentException("there are more default values than parameters");
    }
    
    // construct parameters 
    Parameter<T>[] parameters = new Parameter[names.length];
    for (int i=0; i<names.length; i++) 
    {
      if (i<noRequired)
      {
        parameters[i] = createParameter(names[i], schemata[i]);
      }
      else
      {
        parameters[i] = createParameter(names[i], schemata[i], defaultValues[i-noRequired]);
      }
    }
    
    // initialize
    init(parameters);
  }
  
  /** Check validity of parameters and, if valid, initialize the internal data structures */
  private void init(String[] names, Schema[] schemata, T[] defaultValues)
  {
    JsonString jnames[] = new JsonString[names.length];
    for (int i=0; i<names.length; i++)
    {
      jnames[i] = new JsonString(names[i]);
    }
    init(jnames, schemata, defaultValues);    
  }
  
  /** Check validity of parameters and, if valid, initialize the internal data structures */
  private void init(Parameter<T> ... parameters)
  {
    // check arguments
    noRequired = 0;
    Parameter.Type type = Parameter.Type.REQUIRED;
    if (parameters.length > 0)
    {
      positions = new HashMap<JsonString, Integer>(0);
      for (int i=0; i<parameters.length; i++) 
      {
        if (type == Parameter.Type.REPEATING)
        {
          throw new IllegalArgumentException("no parameters allowed after a repeating parameter");
        }
        
        Parameter parameter = parameters[i];
        if (positions.put(parameter.getName(), i) != null)
        {
          throw new IllegalArgumentException("parameter name not unique: " + parameter.getName());
        }
        if (parameter.isRequired())
        {
          if (type != Parameter.Type.REQUIRED)
          {
            throw new IllegalArgumentException("required parameters must be listed before optional parameters");
          }
          ++noRequired;
        }
        else if (parameter.isOptional())
        {
          type = Parameter.Type.OPTIONAL;
        }
        else
        {
          type = Parameter.Type.REPEATING;
        }
      }
    }

    // accept
    this.parameters = new Parameter[parameters.length];
    System.arraycopy(parameters, 0, this.parameters, 0, parameters.length);
  }
  
  /** Create a required parameter. */
  protected abstract Parameter<T> createParameter(JsonString name, Schema schema);
  
  /** Create an optional parameter. */
  protected abstract Parameter<T> createParameter(JsonString name, Schema schema, T defaultValue);
  
  /** Create an array of the specified size. */
  protected abstract T[] newArrayOfT(int size);
  
  
  // -- getters -----------------------------------------------------------------------------------

    
  /** Total number of parameters */
  public int numParameters()
  {
    return parameters.length;
  }

  /** Number of required parameters */
  public int numRequiredParameters()
  {
    return noRequired;
  }
  
  /** Checks whether the last parameter is repeating. */
  public boolean hasRepeatingParameter()
  {
    return numParameters() > 0 && parameters[parameters.length-1].isRepeating();
  }
  
  /** Retrieve a parameter */
  public Parameter<T> get(int i)
  {
    return parameters[i];
  }
  
  /** Retrieve a parameter */
  public Parameter get(JsonString name)
  {
    return get(positionOf(name));
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
    return parameters[position].getName();
  }
  
  public boolean hasDefault(int position)
  {
    return position >= noRequired;
  }
  
  /** Returns the default value of the non-required parameter at the specified position */
  public T defaultOf(int position)
  {
    return parameters[position].getDefaultValue(); 
  }

  /** Returns the default value of the non-required parameter with the specified name */
  public T defaultOf(JsonString name)
  {
    return defaultOf(positionOf(name)); 
  }

  /** Returns the schema expected for the parameter at the specified position */
  public Schema schemaOf(int position)
  {
    return parameters[position].getSchema();
  }
  
  /** Returns the schema expected for the parameter with the specified name  */
  public Schema schemaOf(JsonString name)
  {
    return schemaOf(positionOf(name));
  }
}
