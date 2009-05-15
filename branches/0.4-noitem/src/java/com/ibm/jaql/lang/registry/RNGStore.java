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

import com.ibm.jaql.io.converter.ToJson;
import com.ibm.jaql.io.converter.FromJson;
import com.ibm.jaql.io.registry.Registry;
import com.ibm.jaql.io.registry.RegistryFormat;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.JaqlFunction;

/**
 * 
 */
public class RNGStore extends Registry<JsonValue, RNGStore.RNGEntry>
{

  /**
   * 
   */
  public static class RNGEntry
  {
    JaqlFunction seed;
    Object    rng;

    /**
     * 
     */
    RNGEntry()
    {
    }

    /**
     * @param seed
     */
    RNGEntry(JaqlFunction seed)
    {
      this.seed = seed;
      this.rng = null;
    }

    /**
     * @return
     */
    public JaqlFunction getSeed()
    {
      return seed;
    }

    /**
     * @param seed
     */
    public void setSeed(JaqlFunction seed)
    {
      this.seed = seed;
    }

    /**
     * @return
     */
    public Object getRng()
    {
      return rng;
    }

    /**
     * @param obj
     */
    public void setRng(Object obj)
    {
      this.rng = obj;
    }
  }

  /**
   * 
   */
  public final static class DefaultRegistryFormat
      extends
        JaqlRegistryFormat<JsonValue, RNGStore.RNGEntry>
  {
    /*
     * (non-Javadoc)
     * 
     * @see com.ibm.jaql.io.registry.JsonRegistryFormat#createFromKeyConverter()
     */
    @Override
    protected FromJson<JsonValue> createFromKeyConverter()
    {
      return new FromJson<JsonValue>() {

        public JsonValue convert(JsonValue src, JsonValue tgt)
        {
          try
          {
            return JsonValue.getCopy(src, tgt);
          }
          catch (Exception e)
          {
            throw new UndeclaredThrowableException(e);
          }
        }

        public JsonValue createInitialTarget()
        {
          return null;
        }

      };
    }

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
     * @see com.ibm.jaql.io.registry.JsonRegistryFormat#createFromValConverter()
     */
    @Override
    protected FromJson<RNGEntry> createFromValConverter()
    {
      return new FromJson<RNGEntry>() {

        public RNGEntry convert(JsonValue src, RNGEntry tgt)
        {
          JsonValue val = src;
          if (val != null && val instanceof JaqlFunction)
          {
            tgt.setSeed((JaqlFunction) val);
            return tgt;
          }
          else
          {
            throw new UndeclaredThrowableException(new Exception(
                "Expected non-null JFunction, got: " + val));
          }
        }

        public RNGEntry createInitialTarget()
        {
          return new RNGEntry();
        }

      };
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.ibm.jaql.io.registry.JsonRegistryFormat#createToKeyConverter()
     */
    @Override
    protected ToJson<JsonValue> createToKeyConverter()
    {
      return new ToJson<JsonValue>() {

        public JsonValue convert(JsonValue src, JsonValue tgt)
        {
          try
          {
            if ( src == null ) 
            {
              return null;
            }
            else
            {
              return JsonValue.getCopy(src, tgt);
            }
          }
          catch (Exception e)
          {
            throw new UndeclaredThrowableException(e);
          }
        }

        public JsonValue createInitialTarget()
        {
          return null;
        }

      };
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.ibm.jaql.io.registry.JsonRegistryFormat#createToValConverter()
     */
    @Override
    protected ToJson<RNGEntry> createToValConverter()
    {
      return new ToJson<RNGEntry>() {

        public JsonValue convert(RNGEntry src, JsonValue tgt)
        {
          JaqlFunction seed = src.getSeed();
          if (seed == null)
            throw new UndeclaredThrowableException(new Exception(
                "seed function is null"));
          return seed;
        }

        public JsonValue createInitialTarget()
        {
          return null;
        }

      };
    }
  }

  /**
   * @param fmt
   */
  public RNGStore(RegistryFormat<JsonValue, RNGStore.RNGEntry, ? extends JsonValue> fmt)
  {
    super(fmt);
  }

  /**
   * @param key
   * @param seed
   */
  public void register(JsonValue key, JaqlFunction seed)
  {
    JsonValue nkey;
    try
    {
      nkey = JsonValue.getCopy(key, null);
    }
    catch (Exception e)
    {
      throw new RuntimeException();
    }
    super.register(nkey, new RNGEntry(seed));
  }
}
