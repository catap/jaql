package com.foobar.store;

import java.util.Vector;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

import com.ibm.jaql.io.Adapter;
import com.ibm.jaql.io.ClosableJsonWriter;
import com.ibm.jaql.io.OutputAdapter;
import com.ibm.jaql.io.hadoop.DefaultHadoopOutputAdapter;
import com.ibm.jaql.io.hadoop.FileOutputConfigurator;
import com.ibm.jaql.io.hadoop.HadoopOutputAdapter;
import com.ibm.jaql.io.hadoop.HadoopSerialization;
import com.ibm.jaql.io.hadoop.JsonHolder;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;

public class DirectSequenceFileWriter {
  
  public static void writeJSONUsingAdapter(Iterable<JsonValue> values, String fileName) throws Exception {
    
    // TODO: API needs to be cleaned up to make this easier to use! In addition, this will produce
    // a "_temporary" directory and will nest "fileName" under a directory named "fileName" making the
    // result more difficult to use (by exposing differences between map-reduce and serial environments) 
    
    // setup adapter options
    BufferedJsonRecord args = new BufferedJsonRecord ();
    BufferedJsonRecord options = new BufferedJsonRecord ();
    options.add(OutputAdapter.ADAPTER_NAME, new JsonString(DefaultHadoopOutputAdapter.class.getName()));
    options.add(OutputAdapter.FORMAT_NAME, new JsonString(SequenceFileOutputFormat.class.getName()));
    options.add(HadoopOutputAdapter.CONFIGURATOR_NAME, new JsonString(FileOutputConfigurator.class.getName()));
    args.add(Adapter.LOCATION_NAME, new JsonString(fileName));
    args.add(Adapter.OUTOPTIONS_NAME, options);
    
    // create adapter
    OutputAdapter adapter = new DefaultHadoopOutputAdapter<JsonHolder, JsonHolder>();
    adapter.init(args);
    
    // open adapter
    adapter.open();
    
    // get record writer
    ClosableJsonWriter writer = adapter.getJsonWriter();
    
    // write items
    for(JsonValue v : values) {
      writer.write(v);
    }
    
    // close writer
    writer.close();
    
    // close adapter
    adapter.close();
  }
  
  public static void writeJSONUsingSequenceFile(Iterable<JsonValue> values, String fileName) throws Exception {
    
    // interrogate the environment
    JobConf conf = new JobConf();
    HadoopSerialization.register(conf);
    Path p = new Path(fileName);
    FileSystem fs = p.getFileSystem(conf);
    
    // get the writer
    Writer writer = SequenceFile.createWriter(fs, conf, p, JsonHolder.class, JsonHolder.class);
    
    // write values
    JsonHolder keyHolder = new JsonHolder(null);
    JsonHolder valueHolder = new JsonHolder();
    for(JsonValue v : values) {
      valueHolder.value = v;
      writer.append(keyHolder, valueHolder);
    }
    
    // close writer
    writer.close();
  }
  
  public static final void main(String[] args) throws Exception {
    // sample data
    Vector<JsonValue> v = new Vector<JsonValue>();
    BufferedJsonRecord r = new BufferedJsonRecord();
    r.add(new JsonString("a"), new JsonString("sample"));
    r.add(new JsonString("b"), new JsonString("something else"));
    v.add(r);
    r = new BufferedJsonRecord();
    r.add(new JsonString("a"), new JsonLong(123));
    r.add(new JsonString("c"), new JsonString("back to string"));
    v.add(r);
    
    DirectSequenceFileWriter.writeJSONUsingAdapter(v, "fileUsingAdapter");
    DirectSequenceFileWriter.writeJSONUsingSequenceFile(v, "fileUsingSequenceFile");
  }
}