package com.foobar.store;

import java.util.Vector;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;

import com.ibm.jaql.io.Adapter;
import com.ibm.jaql.io.InputAdapter;
import com.ibm.jaql.io.ItemReader;
import com.ibm.jaql.io.hadoop.DefaultHadoopInputAdapter;
import com.ibm.jaql.io.hadoop.FileInputConfigurator;
import com.ibm.jaql.io.hadoop.HadoopInputAdapter;
import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JLong;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.json.type.JValue;
import com.ibm.jaql.json.type.MemoryJRecord;

public class DirectSequenceFileReader {
  
  public static ItemReader readJSONUsingAdapter(String fileName) throws Exception
  {
    // TODO: API needs to be cleaned up to make this easier to use! In addition, this will produce
    
    // setup adapter options
    MemoryJRecord args = new MemoryJRecord ();
    MemoryJRecord options = new MemoryJRecord ();
    options.add(InputAdapter.ADAPTER_NAME, new JString(DefaultHadoopInputAdapter.class.getName()));
    options.add(InputAdapter.FORMAT_NAME, new JString(SequenceFileInputFormat.class.getName()));
    options.add(HadoopInputAdapter.CONFIGURATOR_NAME, new JString(FileInputConfigurator.class.getName()));
    args.add(Adapter.LOCATION_NAME, new JString(fileName));
    args.add(Adapter.INOPTIONS_NAME, options);
    
    // create adapter
    InputAdapter adapter = new DefaultHadoopInputAdapter<Item, Item>();
    adapter.initializeFrom(new Item(args));
    
    // open adapter
    adapter.open();
    
    // get record reader
    return adapter.getItemReader();
  }
  
  public static final void main(String[] args) throws Exception {
    
    // TODO: fix in writer
    // the nested directory is due to the behavior of DirectSequenceFileWriter
    String fileName = "fileUsingAdapter/fileUsingAdapter/fileUsingAdapter";
    
    // sample data (copied from DirectSequenceFileWriter)
    Vector<Item> v = new Vector<Item>();
    MemoryJRecord r = new MemoryJRecord();
    r.add("a", new JString("sample"));
    r.add("b", new JString("something else"));
    v.add(new Item(r));
    r = new MemoryJRecord();
    r.add("a", new JLong(123));
    r.add("c", new JString("back to string"));
    v.add(new Item(r));
    
    DirectSequenceFileWriter.writeJSONUsingAdapter(v, fileName);
    
    // now read it back in
    ItemReader iter = DirectSequenceFileReader.readJSONUsingAdapter(fileName);
    Item value = new Item();
    while(iter.next(value)) {
      // so something with val (for now just print it out)
      System.out.println(value.toString());
    }
    iter.close();
  }
}