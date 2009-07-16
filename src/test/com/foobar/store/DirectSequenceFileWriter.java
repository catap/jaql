package com.foobar.store;

import java.util.Vector;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

import com.ibm.jaql.io.Adapter;
import com.ibm.jaql.io.ItemWriter;
import com.ibm.jaql.io.OutputAdapter;
import com.ibm.jaql.io.hadoop.DefaultHadoopOutputAdapter;
import com.ibm.jaql.io.hadoop.FileOutputConfigurator;
import com.ibm.jaql.io.hadoop.HadoopOutputAdapter;
import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JLong;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.json.type.JValue;
import com.ibm.jaql.json.type.MemoryJRecord;

public class DirectSequenceFileWriter {
  
  public static void writeJSONUsingAdapter(Iterable<Item> values, String fileName) throws Exception {
    
    // TODO: API needs to be cleaned up to make this easier to use! In addition, this will produce
    // a "_temporary" directory and will nest "fileName" under a directory named "fileName" making the
    // result more difficult to use (by exposing differences between map-reduce and serial environments) 
    
    // setup adapter options
    MemoryJRecord args = new MemoryJRecord ();
    MemoryJRecord options = new MemoryJRecord ();
    options.add(OutputAdapter.ADAPTER_NAME, new JString(DefaultHadoopOutputAdapter.class.getName()));
    options.add(OutputAdapter.FORMAT_NAME, new JString(SequenceFileOutputFormat.class.getName()));
    options.add(HadoopOutputAdapter.CONFIGURATOR_NAME, new JString(FileOutputConfigurator.class.getName()));
    args.add(Adapter.LOCATION_NAME, new JString(fileName));
    args.add(Adapter.OUTOPTIONS_NAME, options);
    
    // create adapter
    OutputAdapter adapter = new DefaultHadoopOutputAdapter<Item, Item>();
    adapter.initializeFrom(new Item(args));
    
    // open adapter
    adapter.open();
    
    // get record writer
    ItemWriter writer = adapter.getItemWriter();
    
    // write items
    for(Item v : values) {
      writer.write(v);
    }
    
    // close writer
    writer.close();
    
    // close adapter
    adapter.close();
  }
  
  public static void writeJSONUsingSequenceFile(Iterable<Item> values, String fileName) throws Exception {
    
    // interrogate the environment
    JobConf conf = new JobConf();
    Path p = new Path(fileName);
    FileSystem fs = p.getFileSystem(conf);
    
    // get the writer
    Writer writer = SequenceFile.createWriter(fs, conf, p, Item.class, Item.class);
    
    // write values
    Item keyHolder = new Item(null);
    Item valueHolder = new Item();
    for(Item v : values) {
      writer.append(keyHolder, v);
    }
    
    // close writer
    writer.close();
  }
  
  public static final void main(String[] args) throws Exception {
    // sample data
    Vector<Item> v = new Vector<Item>();
    MemoryJRecord r = new MemoryJRecord();
    r.add("a", new JString("sample"));
    r.add("b", new JString("something else"));
    v.add(new Item(r));
    r = new MemoryJRecord();
    r.add("a", new JLong(123));
    r.add("c", new JString("back to string"));
    v.add(new Item(r));
    
    DirectSequenceFileWriter.writeJSONUsingAdapter(v, "fileUsingAdapter");
    DirectSequenceFileWriter.writeJSONUsingSequenceFile(v, "fileUsingSequenceFile");
  }
}