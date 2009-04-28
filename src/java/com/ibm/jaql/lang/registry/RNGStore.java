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

import com.ibm.jaql.io.converter.FromItem;
import com.ibm.jaql.io.converter.ToItem;
import com.ibm.jaql.io.registry.Registry;
import com.ibm.jaql.io.registry.RegistryFormat;
import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JValue;
import com.ibm.jaql.lang.core.JFunction;

/**
 * 
 */
public class RNGStore extends Registry<Item, RNGStore.RNGEntry>
{

  /**
   * 
   */
  public static class RNGEntry
  {
    JFunction seed;
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
    RNGEntry(JFunction seed)
    {
      this.seed = seed;
      this.rng = null;
    }

    /**
     * @return
     */
    public JFunction getSeed()
    {
      return seed;
    }

    /**
     * @param seed
     */
    public void setSeed(JFunction seed)
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
        JaqlRegistryFormat<Item, RNGStore.RNGEntry>
  {
    /*
     * (non-Javadoc)
     * 
     * @see com.ibm.jaql.io.registry.JsonRegistryFormat#createFromKeyConverter()
     */
    @Override
    protected ToItem<Item> createFromKeyConverter()
    {
      return new ToItem<Item>() {

        public void convert(Item src, Item tgt)
        {
          try
          {
            tgt.setCopy(src);
          }
          catch (Exception e)
          {
            throw new UndeclaredThrowableException(e);
          }
        }

        public Item createTarget()
        {
          return new Item();
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
    protected ToItem<RNGEntry> createFromValConverter()
    {
      return new ToItem<RNGEntry>() {

        public void convert(Item src, RNGEntry tgt)
        {
          JValue val = src.get();
          if (val != null && val instanceof JFunction)
          {
            tgt.setSeed((JFunction) val);
          }
          else
          {
            throw new UndeclaredThrowableException(new Exception(
                "Expected non-null JFunction, got: " + val));
          }
        }

        public RNGEntry createTarget()
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
    protected FromItem<Item> createToKeyConverter()
    {
      return new FromItem<Item>() {

        public void convert(Item src, Item tgt)
        {
          try
          {
            tgt.setCopy(src);
          }
          catch (Exception e)
          {
            throw new UndeclaredThrowableException(e);
          }
        }

        public Item createTarget()
        {
          return new Item();
        }

      };
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.ibm.jaql.io.registry.JsonRegistryFormat#createToValConverter()
     */
    @Override
    protected FromItem<RNGEntry> createToValConverter()
    {
      return new FromItem<RNGEntry>() {

        public void convert(RNGEntry src, Item tgt)
        {
          JFunction seed = src.getSeed();
          if (seed == null)
            throw new UndeclaredThrowableException(new Exception(
                "seed function is null"));
          tgt.set(seed);
        }

        public Item createTarget()
        {
          return new Item();
        }

      };
    }
  }

  /**
   * @param fmt
   */
  public RNGStore(RegistryFormat<Item, RNGStore.RNGEntry, ? extends JValue> fmt)
  {
    super(fmt);
  }

  /**
   * @param key
   * @param seed
   */
  public void register(Item key, JFunction seed)
  {
    Item nkey = new Item();
    try
    {
      nkey.setCopy(key);
    }
    catch (Exception e)
    {
      throw new RuntimeException();
    }
    super.register(nkey, new RNGEntry(seed));
  }
}
