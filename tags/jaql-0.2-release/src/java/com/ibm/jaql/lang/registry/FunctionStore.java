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

import org.apache.log4j.Logger;

import com.ibm.jaql.io.converter.FromItem;
import com.ibm.jaql.io.converter.ToItem;
import com.ibm.jaql.io.registry.JsonRegistryFormat;
import com.ibm.jaql.io.registry.Registry;
import com.ibm.jaql.io.registry.RegistryFormat;
import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.json.type.JValue;
import com.ibm.jaql.lang.core.FunctionLib;
import com.ibm.jaql.util.ClassLoaderMgr;

/**
 * Manage a directory of function extensions.
 */
public class FunctionStore extends Registry<JString, JString>
{
  static final Logger LOG = Logger.getLogger(FunctionStore.class.getName());

  /**
   * 
   */
  public static class DefaultRegistryFormat
      extends
        JsonRegistryFormat<JString, JString>
  {

    final static ToItem<JString>   tic = new ToItem<JString>() {

                                         public void convert(Item src,
                                             JString tgt)
                                         {
                                           tgt.set(src.get().toString());
                                         }

                                         public JString createTarget()
                                         {
                                           return new JString();
                                         }

                                       };
    final static FromItem<JString> fic = new FromItem<JString>() {

                                         public void convert(JString src,
                                             Item tgt)
                                         {
                                           ((JString) tgt.get()).set(src
                                               .toString());
                                         }

                                         public Item createTarget()
                                         {
                                           return new Item(new JString());
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
    protected ToItem<JString> createFromKeyConverter()
    {
      return tic;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.ibm.jaql.io.registry.JsonRegistryFormat#createFromValConverter()
     */
    @Override
    protected ToItem<JString> createFromValConverter()
    {
      return tic;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.ibm.jaql.io.registry.JsonRegistryFormat#createToKeyConverter()
     */
    @Override
    protected FromItem<JString> createToKeyConverter()
    {
      return fic;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.ibm.jaql.io.registry.JsonRegistryFormat#createToValConverter()
     */
    @Override
    protected FromItem<JString> createToValConverter()
    {
      return fic;
    }
  }

  /**
   * @param fmt
   */
  public FunctionStore(RegistryFormat<JString, JString, ? extends JValue> fmt)
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
  public void register(JString fnName, JString className)
  {
    Class<?> c = ClassLoaderMgr.resolveClass(className.toString());
    FunctionLib.add(fnName.toString(), c);
    super.register(fnName, className);
  }
}
