package com.foobar.store;

import java.util.Vector;

import org.apache.hadoop.mapred.SequenceFileInputFormat;

import com.ibm.jaql.io.Adapter;
import com.ibm.jaql.io.ClosableJsonIterator;
import com.ibm.jaql.io.InputAdapter;
import com.ibm.jaql.io.hadoop.DefaultHadoopInputAdapter;
import com.ibm.jaql.io.hadoop.FileInputConfigurator;
import com.ibm.jaql.io.hadoop.HadoopInputAdapter;
import com.ibm.jaql.io.hadoop.JsonHolder;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;

public class DirectSequenceFileReader {
  
  public static ClosableJsonIterator readJSONUsingAdapter(String fileName) throws Exception
  {
    // TODO: API needs to be cleaned up to make this easier to use! In addition, this will produce
    
    // setup adapter options
    BufferedJsonRecord args = new BufferedJsonRecord ();
    BufferedJsonRecord options = new BufferedJsonRecord ();
    options.add(InputAdapter.ADAPTER_NAME, new JsonString(DefaultHadoopInputAdapter.class.getName()));
    options.add(InputAdapter.FORMAT_NAME, new JsonString(SequenceFileInputFormat.class.getName()));
    options.add(HadoopInputAdapter.CONFIGURATOR_NAME, new JsonString(FileInputConfigurator.class.getName()));
    args.add(Adapter.LOCATION_NAME, new JsonString(fileName));
    args.add(Adapter.INOPTIONS_NAME, options);
    
    // create adapter
    InputAdapter adapter = new DefaultHadoopInputAdapter<JsonHolder, JsonHolder>();
    adapter.init(args);
    
    // open adapter
    adapter.open();
    
    // get record reader
    return adapter.iter();
  }
  
  public static final void main(String[] args) throws Exception {
    
    // TODO: fix in writer
    // the nested directory is due to the behavior of DirectSequenceFileWriter
    String fileName = "fileUsingAdapter/fileUsingAdapter";
    
    // sample data (copied from DirectSequenceFileWriter)
    Vector<JsonValue> v = new Vector<JsonValue>();
    BufferedJsonRecord r = new BufferedJsonRecord();
    r.add(new JsonString("a"), new JsonString("sample"));
    r.add(new JsonString("b"), new JsonString("something else"));
    v.add(r);
    r = new BufferedJsonRecord();
    r.add(new JsonString("a"), new JsonLong(123));
    r.add(new JsonString("c"), new JsonString("back to string"));
    v.add(r);
    
    DirectSequenceFileWriter.writeJSONUsingAdapter(v, fileName);
    
    // now read it back in
    ClosableJsonIterator iter = DirectSequenceFileReader.readJSONUsingAdapter(fileName);
    while(iter.moveNext()) {
      JsonValue val = iter.current();
      // so something with val (for now just print it out)
      System.out.println(val.toString());
    }
    iter.close();
  }
}