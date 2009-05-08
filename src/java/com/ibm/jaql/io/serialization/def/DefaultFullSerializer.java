package com.ibm.jaql.io.serialization.def;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.EnumMap;

import com.ibm.jaql.io.serialization.BasicSerializer;
import com.ibm.jaql.io.serialization.FullSerializer;
import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.json.type.JValue;
import com.ibm.jaql.json.type.Item.Encoding;
import com.ibm.jaql.util.BaseUtil;

/** Jaql's default serializer. This serializer is generic; it does not consider/exploit any
 * schema information. */
public final class DefaultFullSerializer extends FullSerializer
{
  final EnumMap<Item.Encoding, BasicSerializer<?>> serializers; 

  // caches
  final JValue[] atoms1 = new JValue[Item.Encoding.LIMIT];
  final JValue[] atoms2 = new JValue[Item.Encoding.LIMIT];

  
  // -- default instance --------------------------------------------------------------------------
  
  private static DefaultFullSerializer defaultInstance = new DefaultFullSerializer();
  public static DefaultFullSerializer getDefaultInstance() {
    if (defaultInstance == null) { 
      // TODO: code block needed; why is defaultInstance not initialized?
      // once reslove, make defaultInstance final      
      defaultInstance = new DefaultFullSerializer();
    }
    return defaultInstance;
  }
  
  
  // -- construction ------------------------------------------------------------------------------

  public DefaultFullSerializer() {
    assert Item.Encoding.LIMIT == 20; // change when adding the encodings
    
    serializers = new EnumMap<Item.Encoding, BasicSerializer<?>>(Item.Encoding.class);
    
    BasicSerializer<JString> jstringSerializer = new JStringSerializer();
    
//    UNKNOWN(0, null, Type.UNKNOWN), // bogus item type used as an indicator
//    UNDEFINED(1, null, null), // reserved for possible inclusion of the undefined value
    serializers.put(Encoding.NULL, new NullSerializer());
    serializers.put(Encoding.ARRAY_SPILLING, new SpillJArraySerializer(this));
    serializers.put(Encoding.ARRAY_FIXED, new FixedJArraySerializer(this));
    serializers.put(Encoding.MEMORY_RECORD, new MemoryJRecordSerializer(
        jstringSerializer, this));
    serializers.put(Encoding.BOOLEAN, new JBoolSerializer());
    serializers.put(Encoding.STRING, jstringSerializer);
    serializers.put(Encoding.BINARY, new JBinarySerializer());
    serializers.put(Encoding.LONG, new JLongSerializer());
    serializers.put(Encoding.DECIMAL, new JDecimalSerializer());
    serializers.put(Encoding.DATE_MSEC, new JDateSerializer());
    serializers.put(Encoding.FUNCTION, new JFunctionSerializer());
    serializers.put(Encoding.SCHEMA, new JSchemaSerializer());
    serializers.put(Encoding.JAVAOBJECT_CLASSNAME, new JJavaObjectSerializer());
    serializers.put(Encoding.REGEX, new JRegexSerializer(jstringSerializer));
    serializers.put(Encoding.SPAN, new JSpanSerializer());
    serializers.put(Encoding.DOUBLE, new JDoubleSerializer());
//    JAVA_RECORD(18, JavaJRecord.class, Type.RECORD), 
//    JAVA_ARRAY(19, JavaJRecord.class, Type.ARRAY);
  }

  
  // -- FullSerializer methods --------------------------------------------------------------------

  @Override
  public JValue read(DataInput in, JValue target) throws IOException
  {
    int encodingId = BaseUtil.readVUInt(in);
    Item.Encoding encoding = Item.Encoding.valueOf(encodingId);
    BasicSerializer<?> serializer = serializers.get(encoding);
    return serializer.read(in, target);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void write(DataOutput out, JValue value) throws IOException
  {
    Item.Encoding encoding;
    if (value == null) {
      encoding = Item.Encoding.NULL;
    } else {
      encoding = value.getEncoding();
    }
    BaseUtil.writeVUInt(out, encoding.id);
    BasicSerializer serializer = serializers.get(encoding);
    assert serializer != null : "No serializer defined for " + encoding;
    serializer.write(out, value);    
  }

  @Override
  public void skip(DataInput in) throws IOException {
    int encodingId = BaseUtil.readVUInt(in);
    Item.Encoding encoding = Item.Encoding.valueOf(encodingId);
    BasicSerializer<?> serializer = serializers.get(encoding);
    serializer.skip(in);
  }

  @SuppressWarnings("unchecked")
  @Override
  public int compare(DataInput in1, DataInput in2) throws IOException {
    // read and compare encodings / types
    int code1 = BaseUtil.readVUInt(in1);
    int code2 = BaseUtil.readVUInt(in2);
    assert code1>0 && code2>0;
    Item.Encoding encoding1 = Item.Encoding.valueOf(code1);
    Item.Encoding encoding2 = Item.Encoding.valueOf(code2);
    if (encoding1 != encoding2) {
      Item.Type type1 = encoding1.getType();
      Item.Type type2 = encoding2.getType();
      int cmp = type1.compareTo(type2);
      if (cmp != 0) return cmp;

      // if same type but different encodings, deserialize
      // TODO: a better way / treat some cases special?
      return compareValuesDeserialized(in1, encoding1, in2, encoding2);
    }
    
    // same encoding
    BasicSerializer s = getSerializer(encoding1);
    return s.compare(in1, in2);
  }
  
  @Override
  public void copy(DataInput in, DataOutput out) throws IOException {
    int encodingId = BaseUtil.readVUInt(in);
    Item.Encoding encoding = Item.Encoding.valueOf(encodingId);
    BasicSerializer<?> serializer = serializers.get(encoding);
    
    BaseUtil.writeVUInt(out, encodingId);
    serializer.copy(in, out);
  }
  
  
  // -- misc --------------------------------------------------------------------------------------

  /** Returns the <code>BasicSerializer</code> used for the given <code>encoding</code>. */
  public BasicSerializer<?> getSerializer(Item.Encoding encoding) {
    BasicSerializer<?> serializer = serializers.get(encoding);
    assert serializer != null : "No serializer defined for " + encoding;
    return serializer;
  }
  

  /** Compares the encoded value from <code>in1</code> with the encoded value from 
   * <code>in2</code> by deserializing. This method is used to compare types of different
   * encodings.   
   * 
   * @param in1 an input stream pointing to a value
   * @param in2 another input stream pointing to another value with the same encoding
   * @return
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  protected int compareValuesDeserialized(DataInput in1, Item.Encoding encoding1, 
      DataInput in2, Item.Encoding encoding2) throws IOException 
  {
    BasicSerializer s1 = getSerializer(encoding1);
    BasicSerializer s2 = getSerializer(encoding2);

    // atoms can be overwritten; they are only used here 
    JValue value1 = atoms1[encoding1.id] = s1.read(in1, atoms1[encoding1.id]);
    JValue value2 = atoms2[encoding2.id] = s2.read(in2, atoms2[encoding2.id]);
    return value1.compareTo(value2);
  }
  
}
