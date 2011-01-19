package com.ibm.jaql.benchmark.fs;

import java.net.URI;
import java.util.Vector;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

import com.ibm.jaql.io.Adapter;
import com.ibm.jaql.io.ClosableJsonIterator;
import com.ibm.jaql.io.ClosableJsonWriter;
import com.ibm.jaql.io.InputAdapter;
import com.ibm.jaql.io.OutputAdapter;
import com.ibm.jaql.io.hadoop.DefaultHadoopInputAdapter;
import com.ibm.jaql.io.hadoop.DefaultHadoopOutputAdapter;
import com.ibm.jaql.io.hadoop.FileInputConfigurator;
import com.ibm.jaql.io.hadoop.FileOutputConfigurator;
import com.ibm.jaql.io.hadoop.HadoopInputAdapter;
import com.ibm.jaql.io.hadoop.HadoopOutputAdapter;
import com.ibm.jaql.io.hadoop.HadoopSerializationDefault;
import com.ibm.jaql.io.hadoop.JsonHolderDefault;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;

public class MemoryWriterReaderTest {
  
  public static void writeJSONUsingAdapter(Iterable<JsonValue> values, String fileName) throws Exception {
    //TODO: Error when using root file
	URI u = new URI("memory://test/abc/"+fileName);
    // TODO: API needs to be cleaned up to make this easier to use! In addition, this will produce
    // a "_temporary" directory and will nest "fileName" under a directory named "fileName" making the
    // result more difficult to use (by exposing differences between map-reduce and serial environments) 
	
    // setup adapter options
    BufferedJsonRecord args = new BufferedJsonRecord ();
    BufferedJsonRecord outOptions = new BufferedJsonRecord ();
    outOptions.add(OutputAdapter.ADAPTER_NAME, new JsonString(DefaultHadoopOutputAdapter.class.getName()));
    outOptions.add(OutputAdapter.FORMAT_NAME, new JsonString(SequenceFileOutputFormat.class.getName()));
    outOptions.add(HadoopOutputAdapter.CONFIGURATOR_NAME, new JsonString(FileOutputConfigurator.class.getName()));
    BufferedJsonRecord inOptions = new BufferedJsonRecord ();
    inOptions.add(InputAdapter.ADAPTER_NAME, new JsonString(DefaultHadoopInputAdapter.class.getName()));
    inOptions.add(InputAdapter.FORMAT_NAME, new JsonString(SequenceFileInputFormat.class.getName()));
    inOptions.add(HadoopInputAdapter.CONFIGURATOR_NAME, new JsonString(FileInputConfigurator.class.getName()));
    
    args.add(Adapter.LOCATION_NAME, new JsonString(u.toString()));
    args.add(Adapter.OUTOPTIONS_NAME, outOptions);
    args.add(Adapter.INOPTIONS_NAME, inOptions);
    
    // create adapter
    OutputAdapter outAdapter = new DefaultHadoopOutputAdapter<JsonHolderDefault, JsonHolderDefault>();
    outAdapter.init(args);
    
    // open adapter
    outAdapter.open();
    
    // get record writer
    ClosableJsonWriter writer = outAdapter.getWriter();
    
    // write items
    for (int i = 0; i < 20; i++) {
        for(JsonValue v : values) {
            writer.write(v);
        }
	}
    
    // close writer
    writer.close();
    
    // close adapter
    outAdapter.close();
    
    MemoryFileSystem.printFsStructure();
    
    MemoryFileSystem fs = new MemoryFileSystem();
    fs.initialize(u, null);
    fs.setRepeat(new Path(u.toString()),100);
    
    /* Test reading */
    // create adapter
    InputAdapter inAdapter = new DefaultHadoopInputAdapter<JsonHolderDefault, JsonHolderDefault>();
    inAdapter.init(args);
    
    // open adapter
    inAdapter.open();
    
    // get record writer
    ClosableJsonIterator reader = inAdapter.iter();
    
    // write items
    int i = 1;
    for(JsonValue v : reader) {
      System.out.println(i++ +": " + v);
    }
    
    // close writer
    reader.close();
    
    // close adapter
    inAdapter.close();
    
  }
  
  public static void writeJSONUsingSequenceFile(Iterable<JsonValue> values, String fileName) throws Exception {
    
    // interrogate the environment
    JobConf conf = new JobConf();
    HadoopSerializationDefault.register(conf);
    Path p = new Path(fileName);
    FileSystem fs = p.getFileSystem(conf);
    
    // get the writer
    Writer writer = SequenceFile.createWriter(fs, conf, p, JsonHolderDefault.class, JsonHolderDefault.class);
    
    // write values
    JsonHolderDefault keyHolder = new JsonHolderDefault(null);
    JsonHolderDefault valueHolder = new JsonHolderDefault();
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
    
    MemoryWriterReaderTest.writeJSONUsingAdapter(v, "fileUsingAdapter");
    //sDirectMemoryWriter.writeJSONUsingSequenceFile(v, "fileUsingSequenceFile");
  }
}