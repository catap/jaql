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
package com.ibm.jaql.io;

import org.apache.log4j.Logger;

import com.ibm.jaql.io.converter.FromJson;
import com.ibm.jaql.io.converter.ToJson;
import com.ibm.jaql.io.hadoop.CompositeInputAdapter;
import com.ibm.jaql.io.registry.JsonRegistryFormat;
import com.ibm.jaql.io.registry.Registry;
import com.ibm.jaql.io.registry.RegistryFormat;
import com.ibm.jaql.io.registry.RegistryUtil;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.util.ClassLoaderMgr;

/**
 * Manage the mapping between storage names and their default input and output
 * options.
 */
public class AdapterStore
    extends Registry<JsonString, AdapterStore.AdapterRegistry>
{

  static final Logger  LOG                        = Logger.getLogger(AdapterStore.class.getName());

  public static String DEFAULT_REGISTRY_FILE_NAME = "storage-default.jql";

  /**
   * Option JRecords for input and output
   */
  public static class AdapterRegistry
  {
    private JsonRecord input;
    private JsonRecord output;

    /**
     * Empty adapter registry
     * 
     */
    public AdapterRegistry()
    {
    }

    /**
     * Default input and output options.
     * 
     * @param input
     * @param output
     */
    public AdapterRegistry(JsonRecord input, JsonRecord output)
    {
      this.input = input;
      this.output = output;
    }

    public void setInput(JsonRecord r)
    {
      this.input = r;
    }

    public JsonRecord getInput()
    {
      return this.input;
    }

    public void setOutput(JsonRecord r)
    {
      this.output = r;
    }

    public JsonRecord getOutput()
    {
      return this.output;
    }
  }

  public static class DefaultRegistryFormat
      extends
        JsonRegistryFormat<JsonString, AdapterStore.AdapterRegistry>
  {

    public DefaultRegistryFormat()
    {
      super();
    }

    @Override
    protected FromJson<JsonString> createFromKeyConverter()
    {
      return new FromJson<JsonString>() {

        public JsonString convert(JsonValue src, JsonString target)
        {
          target.set(src.toString());
          return target;
        }

        public JsonString createInitialTarget()
        {
          return new JsonString();
        }
      };
    }

    @Override
    protected FromJson<AdapterRegistry> createFromValConverter()
    {
      return new FromJson<AdapterRegistry>() {

        public AdapterRegistry convert(JsonValue src, AdapterRegistry tgt)
        {
          JsonRecord r = (JsonRecord) src;
          JsonRecord iR = (JsonRecord)r.getValue(Adapter.INOPTIONS_NAME);
          JsonRecord oR = (JsonRecord)r.getValue(Adapter.OUTOPTIONS_NAME);
          tgt.setInput(iR);
          tgt.setOutput(oR);
          return tgt;
        }

        public AdapterRegistry createInitialTarget()
        {
          return new AdapterRegistry();
        }

      };
    }

    @Override
    protected ToJson<JsonString> createToKeyConverter()
    {
      return new ToJson<JsonString>() {

        public JsonValue convert(JsonString src, JsonValue target)
        {
          ((JsonString) target).set(src.toString());
          return target;
        }

        public JsonValue createInitialTarget()
        {
          return new JsonString();
        }

      };
    }

    @Override
    protected ToJson<AdapterRegistry> createToValConverter()
    {
      return new ToJson<AdapterRegistry>() {

        public JsonValue convert(AdapterRegistry src, JsonValue tgt)
        {
          JsonRecord iR = src.getInput();
          JsonRecord oR = src.getOutput();
          BufferedJsonRecord tR = (BufferedJsonRecord) tgt;
          tR.clear();
          if (iR != null) tR.add(Adapter.INOPTIONS_NAME, iR);
          if (oR != null) tR.add(Adapter.OUTOPTIONS_NAME, oR);
          return tgt;
        }

        public JsonValue createInitialTarget()
        {
          return new BufferedJsonRecord();
        }

      };
    }
  }

  private static AdapterStore store;

  public static AdapterStore initStore()
  {
    store = initStore(new DefaultRegistryFormat());
    return store;
  }

  public static AdapterStore initStore(
      RegistryFormat<JsonString, AdapterStore.AdapterRegistry, ? extends JsonValue> fmt)
  {
    store = new AdapterStore(fmt);
    try
    {
      RegistryUtil.readFile(DEFAULT_REGISTRY_FILE_NAME, store);
    }
    catch (Exception e)
    {
      // TODO: consider logging this. jaql can proceed even without storage entries.
      LOG.info("instantiating empty AdapterStore");
    }
    return store;
  }

  public static AdapterStore getStore()
  {
    if (store == null) initStore();
    return store;
  }

  public AdapterStore(
      RegistryFormat<JsonString, AdapterStore.AdapterRegistry, ? extends JsonValue> fmt)
  {
    super(fmt);
  }

  protected abstract class OptionHandler
  {

    public abstract JsonRecord getOption(String name);

    public abstract JsonRecord getOverride(JsonRecord args);

    public abstract void replaceOption(BufferedJsonRecord args, JsonRecord options);

    public BufferedJsonRecord getOption(JsonRecord args)
    {
      JsonValue tValue = args.getValue(Adapter.TYPE_NAME);
      JsonRecord defaultOptions = null;
      if (tValue != null)
      {
        defaultOptions = getOption(tValue.toString()); // FIXME: memory
      }
      BufferedJsonRecord overrideOptions = (BufferedJsonRecord) getOverride(args);
      overrideOptions = unionOptions(defaultOptions, overrideOptions);
      return overrideOptions;
    }

    public BufferedJsonRecord unionOptions(JsonRecord src, BufferedJsonRecord tgt)
    {
      if (tgt == null)
      {
        tgt = new BufferedJsonRecord();
      }

      if (src == null)
      {
        return tgt;
      }

      int numFields = src.arity();
      for (int i = 0; i < numFields; i++)
      {
        JsonString name = src.getName(i);
        int aIdx = tgt.findName(name);
        if (aIdx < 0)
        {
          tgt.add(name, src.getValue(i));
        }
      }
      return tgt;
    }

    public boolean exists(String name)
    {
      return getOption(name) != null;
    }

    public Class<?> getAdapterClass(String name) throws Exception
    {
      JsonRecord r = getOption(name);
      if (r == null) return null;

      return getClassFromRecord(r, Adapter.ADAPTER_NAME, null);
    }

    public Adapter getAdapter(JsonValue value) throws Exception
    {
      JsonRecord args = (JsonRecord) value;
      JsonRecord options = getOption(args);
      Class<?> adapterClass = getClassFromRecord(options, Adapter.ADAPTER_NAME,
          null);
      Adapter adapter = (Adapter) adapterClass.newInstance();
      adapter.init(value);
      return adapter;
    }
  }

  public class InputHandler extends OptionHandler
  {

    public JsonRecord getOption(String name)
    {
      AdapterRegistry a = get(new JsonString(name));
      if (a == null) return null;
      return a.getInput();
    }

    public JsonRecord getOverride(JsonRecord args)
    {
      JsonValue i = args.getValue(Adapter.INOPTIONS_NAME);
      if (i == null) {
        // handle the case where OPTIONS_NAME is used instead
        i = args.getValue(Adapter.OPTIONS_NAME);
        // can still be null
      }
      return (JsonRecord) i;
    }

    public void replaceOption(BufferedJsonRecord args, JsonRecord options)
    {
      args.set(Adapter.INOPTIONS_NAME, options);
//      Item tmp = args.getValue(Adapter.INOPTIONS_NAME);
//
//      if (tmp != Item.NIL)
//      {
//        tmp.set(options);
//      }
//      else
//      {
//        tmp = new Item(options);
//        args.add(Adapter.INOPTIONS_NAME, tmp);
//      }
    }

    @Override
    public Adapter getAdapter(JsonValue value) throws Exception
    {
      if (value instanceof JsonRecord)
      {
        return super.getAdapter(value);
      }
      else
      {
        // Assume its an array which can only be handled by a CompositeInputAdapter
        // TODO: abstract this to let other array handler plug-ins
        CompositeInputAdapter adapter = new CompositeInputAdapter();
        adapter.init(value);
        return adapter;
      }
    }
  }

  public class OutputHandler extends OptionHandler
  {

    public JsonRecord getOption(String name)
    {
      AdapterRegistry a = get(new JsonString(name));
      if (a == null) return null;
      return a.getOutput();
    }

    public JsonRecord getOverride(JsonRecord args)
    {
      JsonValue i = args.getValue(Adapter.OUTOPTIONS_NAME);
      if (i == null) {
        // handle the case where OPTIONS_NAME is used instead
        i = args.getValue(Adapter.OPTIONS_NAME);
        // can still be null
      }
      return (JsonRecord) i;
    }

    public void replaceOption(BufferedJsonRecord args, JsonRecord options)
    {
      args.set(Adapter.OUTOPTIONS_NAME, options);
//      Item tmp = args.getValue(Adapter.OUTOPTIONS_NAME);
//
//      if (tmp != Item.NIL)
//      {
//        tmp.set(options);
//      }
//      else
//      {
//        tmp = new Item(options);
//        args.add(Adapter.OUTOPTIONS_NAME, tmp);
//
//      }
    }
  }

  public InputHandler  input  = new InputHandler();
  public OutputHandler output = new OutputHandler();

  /**
   * Get the class from the string that is stored under the given name in the
   * options record. If the class name does not exist, then use a defaultValue.
   * 
   * @param options
   * @param name
   * @param defaultValue
   * @return
   * @throws Exception
   */
  public Class<?> getClassFromRecord(JsonRecord options, String name,
      Class<?> defaultValue) throws Exception
  {
    Class<?> c = defaultValue;
    if (options != null && name != null)
    {
      JsonValue wName = options.getValue(name);
      if (wName != null && wName instanceof JsonString)
      {
        c = ClassLoaderMgr.resolveClass(wName.toString());
      }
    }

    return c;
  }

  /**
   * Utility method for getting the location from a JRecord. If the location is
   * not set, has a null value, or an empty value, null is returned
   * 
   * @param args
   * @return
   * @throws Exception
   */
  public String getLocation(JsonRecord args) throws Exception
  {
    JsonValue lValue = args.getValue(Adapter.LOCATION_NAME);
    if (lValue == null || "".equals(lValue.toString()))
      return null;
    return lValue.toString();
  }
}
