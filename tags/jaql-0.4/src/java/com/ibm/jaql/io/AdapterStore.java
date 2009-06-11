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

import com.ibm.jaql.io.converter.ToItem;
import com.ibm.jaql.io.converter.FromItem;
import com.ibm.jaql.io.hadoop.CompositeInputAdapter;
import com.ibm.jaql.io.registry.JsonRegistryFormat;
import com.ibm.jaql.io.registry.Registry;
import com.ibm.jaql.io.registry.RegistryFormat;
import com.ibm.jaql.io.registry.RegistryUtil;
import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JRecord;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.json.type.JValue;
import com.ibm.jaql.json.type.MemoryJRecord;
import com.ibm.jaql.util.ClassLoaderMgr;

/**
 * Manage the mapping between storage names and their default input and output
 * options.
 */
public class AdapterStore
    extends Registry<JString, AdapterStore.AdapterRegistry>
{

  static final Logger  LOG                        = Logger.getLogger(AdapterStore.class.getName());

  public static String DEFAULT_REGISTRY_FILE_NAME = "storage-default.jql";

  /**
   * Option JRecords for input and output
   */
  public static class AdapterRegistry
  {
    private JRecord input;
    private JRecord output;

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
    public AdapterRegistry(JRecord input, JRecord output)
    {
      this.input = input;
      this.output = output;
    }

    public void setInput(JRecord r)
    {
      this.input = r;
    }

    public JRecord getInput()
    {
      return this.input;
    }

    public void setOutput(JRecord r)
    {
      this.output = r;
    }

    public JRecord getOutput()
    {
      return this.output;
    }
  }

  public static class DefaultRegistryFormat
      extends
        JsonRegistryFormat<JString, AdapterStore.AdapterRegistry>
  {

    public DefaultRegistryFormat()
    {
      super();
    }

    @Override
    protected FromItem<JString> createFromKeyConverter()
    {
      return new FromItem<JString>() {

        public void convert(Item src, JString tgt)
        {
          tgt.set(src.get().toString());
        }

        public JString createTarget()
        {
          return new JString();
        }

      };
    }

    @Override
    protected FromItem<AdapterRegistry> createFromValConverter()
    {
      return new FromItem<AdapterRegistry>() {

        public void convert(Item src, AdapterRegistry tgt)
        {
          JRecord r = (JRecord) src.get();
          JRecord iR = null;
          JRecord oR = null;
          Item id = r.getValue(Adapter.INOPTIONS_NAME);

          if (id != null)
          {
            iR = (JRecord) id.get();
          }
          Item od = r.getValue(Adapter.OUTOPTIONS_NAME);
          if (od != null)
          {
            oR = (JRecord) od.get();
          }
          tgt.setInput(iR);
          tgt.setOutput(oR);
        }

        public AdapterRegistry createTarget()
        {
          return new AdapterRegistry();
        }

      };
    }

    @Override
    protected ToItem<JString> createToKeyConverter()
    {
      return new ToItem<JString>() {

        public void convert(JString src, Item tgt)
        {
          ((JString) tgt.get()).set(src.toString());
        }

        public Item createTarget()
        {
          Item d = new Item();
          d.set(new JString());
          return d;
        }

      };
    }

    @Override
    protected ToItem<AdapterRegistry> createToValConverter()
    {
      return new ToItem<AdapterRegistry>() {

        public void convert(AdapterRegistry src, Item tgt)
        {
          JRecord iR = src.getInput();
          JRecord oR = src.getOutput();
          MemoryJRecord tR = (MemoryJRecord) tgt.get();
          tR.clear();
          if (iR != null) tR.add(Adapter.INOPTIONS_NAME, iR);
          if (oR != null) tR.add(Adapter.OUTOPTIONS_NAME, oR);
        }

        public Item createTarget()
        {
          Item d = new Item();
          d.set(new MemoryJRecord());
          return d;
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
      RegistryFormat<JString, AdapterStore.AdapterRegistry, ? extends JValue> fmt)
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
      RegistryFormat<JString, AdapterStore.AdapterRegistry, ? extends JValue> fmt)
  {
    super(fmt);
  }

  protected abstract class OptionHandler
  {

    public abstract JRecord getOption(String name);

    public abstract JRecord getOverride(JRecord args);

    public abstract void replaceOption(MemoryJRecord args, JRecord options);

    public MemoryJRecord getOption(JRecord args)
    {
      Item tItem = args.getValue(Adapter.TYPE_NAME);
      JRecord defaultOptions = null;
      if (tItem != null && tItem.get() != null)
      {
        defaultOptions = getOption(tItem.get().toString()); // FIXME: memory
      }
      MemoryJRecord overrideOptions = (MemoryJRecord) getOverride(args);
      overrideOptions = unionOptions(defaultOptions, overrideOptions);
      return overrideOptions;
    }

    public MemoryJRecord unionOptions(JRecord src, MemoryJRecord tgt)
    {
      if (tgt == null)
      {
        tgt = new MemoryJRecord();
      }

      if (src == null)
      {
        return tgt;
      }

      int numFields = src.arity();
      for (int i = 0; i < numFields; i++)
      {
        JString name = src.getName(i);
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
      JRecord r = getOption(name);
      if (r == null) return null;

      return getClassFromRecord(r, Adapter.ADAPTER_NAME, null);
    }

    public Adapter getAdapter(Item item) throws Exception
    {
      JRecord args = (JRecord) item.get();
      JRecord options = getOption(args);
      Class<?> adapterClass = getClassFromRecord(options, Adapter.ADAPTER_NAME,
          null);
      Adapter adapter = (Adapter) adapterClass.newInstance();
      adapter.initializeFrom(item);
      return adapter;
    }
  }

  public class InputHandler extends OptionHandler
  {

    public JRecord getOption(String name)
    {
      AdapterRegistry a = get(new JString(name));
      if (a == null) return null;
      return a.getInput();
    }

    public JRecord getOverride(JRecord args)
    {
      Item i = args.getValue(Adapter.INOPTIONS_NAME);
      if (i == Item.NIL) {
        // handle the case where OPTIONS_NAME is used instead
        i = args.getValue(Adapter.OPTIONS_NAME);
        if (i == Item.NIL) 
          return null;
      }
      return (JRecord) i.get();
    }

    public void replaceOption(MemoryJRecord args, JRecord options)
    {
      Item tmp = args.getValue(Adapter.INOPTIONS_NAME);

      if (tmp != Item.NIL)
      {
        tmp.set(options);
      }
      else
      {
        tmp = new Item(options);
        args.add(Adapter.INOPTIONS_NAME, tmp);
      }
    }

    @Override
    public Adapter getAdapter(Item item) throws Exception
    {
      if (item.get() instanceof JRecord)
      {
        return super.getAdapter(item);
      }
      else
      {
        // Assume its an array which can only be handled by a CompositeInputAdapter
        // TODO: abstract this to let other array handler plug-ins
        CompositeInputAdapter adapter = new CompositeInputAdapter();
        adapter.initializeFrom(item);
        return adapter;
      }
    }
  }

  public class OutputHandler extends OptionHandler
  {

    public JRecord getOption(String name)
    {
      AdapterRegistry a = get(new JString(name));
      if (a == null) return null;
      return a.getOutput();
    }

    public JRecord getOverride(JRecord args)
    {
      Item i = args.getValue(Adapter.OUTOPTIONS_NAME);
      if (i == Item.NIL) {
        // handle the case where OPTIONS_NAME is used instead
        i = args.getValue(Adapter.OPTIONS_NAME);
        if (i == Item.NIL) 
          return null;
      }
      return (JRecord) i.get();
    }

    public void replaceOption(MemoryJRecord args, JRecord options)
    {
      Item tmp = args.getValue(Adapter.OUTOPTIONS_NAME);

      if (tmp != Item.NIL)
      {
        tmp.set(options);
      }
      else
      {
        tmp = new Item(options);
        args.add(Adapter.OUTOPTIONS_NAME, tmp);

      }
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
  public Class<?> getClassFromRecord(JRecord options, String name,
      Class<?> defaultValue) throws Exception
  {
    Class<?> c = defaultValue;
    if (options != null && name != null)
    {
      JValue wName = options.getValue(name).get();
      if (wName != null && wName instanceof JString)
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
  public String getLocation(JRecord args) throws Exception
  {
    Item lItem = args.getValue(Adapter.LOCATION_NAME);
    if (lItem == null || lItem.isNull() || "".equals(lItem.get().toString()))
      return null;
    return lItem.get().toString();
  }
}
