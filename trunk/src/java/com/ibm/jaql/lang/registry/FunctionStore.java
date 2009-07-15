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
package com.ibm.jaql.lang.registry;

import java.lang.reflect.UndeclaredThrowableException;

import org.apache.log4j.Logger;

import com.ibm.jaql.io.converter.FromJson;
import com.ibm.jaql.io.converter.ToJson;
import com.ibm.jaql.io.registry.JsonRegistryFormat;
import com.ibm.jaql.io.registry.Registry;
import com.ibm.jaql.io.registry.RegistryFormat;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonUtil;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.FunctionLib;
import com.ibm.jaql.util.ClassLoaderMgr;

/**
 * Manage a directory of function extensions.
 */
public class FunctionStore extends Registry<JsonString, JsonString>
{
  static final Logger LOG = Logger.getLogger(FunctionStore.class.getName());

  /**
   * 
   */
  public static class DefaultRegistryFormat extends JsonRegistryFormat<JsonString, JsonString>
  {

    final static FromJson<JsonString> tic = new FromJson<JsonString>()
    {
      public JsonString convert(JsonValue src, JsonString tgt)
      {
        tgt.set(src.toString());
        return tgt;
      }

      public JsonString createTarget()
      {
        return new JsonString();
      }

    };
    final static ToJson<JsonString> fic = new ToJson<JsonString>()
    {

      public JsonString convert(JsonString src, JsonValue tgt)
      {
        try
        {
          return JsonUtil.getCopy(src, tgt);
        } catch (Exception e)
        {
          throw new UndeclaredThrowableException(e);
        }
      }

      public JsonValue createTarget()
      {
        return new JsonString();
      }
      
      public Schema getSchema()
      {
        return SchemaFactory.stringOrNullSchema();
      }
    };

    /**
     * 
     */
    public DefaultRegistryFormat()
    {
      super();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.ibm.jaql.io.registry.JsonRegistryFormat#createFromKeyConverter()
     */
    @Override
    protected FromJson<JsonString> createFromKeyConverter()
    {
      return tic;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.ibm.jaql.io.registry.JsonRegistryFormat#createFromValConverter()
     */
    @Override
    protected FromJson<JsonString> createFromValConverter()
    {
      return tic;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.ibm.jaql.io.registry.JsonRegistryFormat#createToKeyConverter()
     */
    @Override
    protected ToJson<JsonString> createToKeyConverter()
    {
      return fic;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.ibm.jaql.io.registry.JsonRegistryFormat#createToValConverter()
     */
    @Override
    protected ToJson<JsonString> createToValConverter()
    {
      return fic;
    }
  }

  /**
   * @param fmt
   */
  public FunctionStore(RegistryFormat<JsonString, JsonString, ? extends JsonValue> fmt)
  {
    super(fmt);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.registry.Registry#register(java.lang.Object,
   *      java.lang.Object)
   */
  @Override
  public void register(JsonString fnName, JsonString className)
  {
    Class<?> c = ClassLoaderMgr.resolveClass(className.toString());
    FunctionLib.add(fnName.toString(), c);
    super.register(fnName, className);
  }
}
