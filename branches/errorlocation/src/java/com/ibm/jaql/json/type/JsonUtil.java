package com.ibm.jaql.json.type;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.Map.Entry;

import com.ibm.jaql.io.serialization.text.TextFullSerializer;
import com.ibm.jaql.io.serialization.text.schema.SchemaTextFullSerializer;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.util.FastPrintBuffer;
import com.ibm.jaql.util.FastPrinter;

/** Utility methods for dealing with {@link JsonValue}s and <code>null</code>s. */
public class JsonUtil
{
  /**
   * Print <code>value</code in (extended) JSON text format. 
   * 
   * @param value a value or <code>null</code>
   * @param out an output stream
   * @throws Exception
   */
  public static void print(FastPrinter out, JsonValue value) throws IOException {
	  TextFullSerializer serializer = getDefaultSerializer();
	  serializer.write(out, value);
	}
    
  /**
   * Print <code>value</code in (extended) JSON text format. 
   * 
   * @param value a value or <code>null</code>
   * @param out an output stream
   * @param emitOrigin emit expression source origins?
   * @throws Exception
   */
  public static void print(FastPrinter out, JsonValue value, boolean emitOrigin) throws IOException {
    TextFullSerializer serializer = getDefaultSerializer(emitOrigin);
    serializer.write(out, value);
  }
  
  /**
   * Print indented <code>value</code in (extended) JSON text format. 
   * 
   * @param value a value or <code>null</code>
   * @param out an output stream
   * @param indent indentation value
   * @throws Exception
   */
  public static void print(FastPrinter out, JsonValue value, int indent) throws IOException {
    TextFullSerializer serializer = getDefaultSerializer();
    serializer.write(out, value, indent);
  }

  public static TextFullSerializer getDefaultSerializer(boolean emitOrigin)
  {
    return TextFullSerializer.getDefault(emitOrigin);
  }
  
  public static TextFullSerializer getDefaultSerializer()
  {
    return TextFullSerializer.getDefault();
  }

  /** The supplied schema must not be null. */
  public static TextFullSerializer getDefaultSerializer(Schema schema)
  {
    if (schema == null) {
      throw new NullPointerException("internal error in schema inference");
    }
    if (schema == SchemaFactory.anySchema()) { // makes a common case more efficient
      return getDefaultSerializer();
    }
    return new SchemaTextFullSerializer(schema);
  }
  
  /**
   * Print <code>value</code>, if non-null, on the stream in (extended) JSON text format using
   * <code>v.print(out)</code>. Otherwise, prints <code>null</code>. 
   * 
   * @param value a value or <code>null</code>
   * @param out an output stream
   * @param indent indentation value
   * @throws Exception
   */
  public static String printToString(JsonValue value) throws IOException
  {
    FastPrintBuffer out = new FastPrintBuffer();
    print(out, value);
    return out.toString();
  }

  /** Handles null (nulls go first) */
  public static int compare(JsonValue v1, JsonValue v2) {
    // handle null
    if (v1 == null)
    {
      return v2 == null ? 0 : -1;
    }
    if (v2 == null)
    {
      return 1;
    }
    
    // compare types
    int cmp = v1.getType().compareTo(v2.getType());
    if (cmp == 0 || (v1.getType().isNumber() && v2.getType().isNumber()))
    {
      return v1.compareTo(v2); 
    }
    else
    {
      return cmp;
    }
  }

  /** Handles null */
  public static boolean equals(JsonValue v1, JsonValue v2) {
    return compare(v1, v2) == 0;
  }

  /** Handles null */
  public static int hashCode(JsonValue v)  {
    if (v == null) {
      return 0;
    } 
    return v.hashCode();
  }

  /** Handles null */
  public static long longHashCode(JsonValue v)  {
    if (v == null) {
      return 0;
    } 
    return v.longHashCode();
  }

  /** Handles null */
  @SuppressWarnings("unchecked")
  public static <T extends JsonValue> T getCopy(T src, JsonValue target) throws Exception
  {
    if (src == null) 
    {
      return null;
    }
    return (T)src.getCopy(target);
  }
  
  /** Handles null */
  @SuppressWarnings("unchecked")
  public static <T extends JsonValue> T getCopyUnchecked(T src, JsonValue target) 
  {
    if (src == null) 
    {
      return null;
    }
    try
    {
      return (T)src.getCopy(target);
    } 
    catch (Exception e)
    {
      throw new UndeclaredThrowableException(e);
    }    
  }

  /** Handles null */
  public static <T extends JsonValue> JsonValue getImmutableCopy(T src) throws Exception
  {
    if (src == null) 
    {
      return null;
    }
    return src.getImmutableCopy();
  }
  
  /** Handles null */
  public static <T extends JsonValue> JsonValue getImmutableCopyUnchecked(T src) 
  {
    if (src == null) 
    {
      return null;
    }
    try
    {
      return src.getImmutableCopy();
    } 
    catch (Exception e)
    {
      throw new UndeclaredThrowableException(e);
    }    
  }
  
  
  // TODO: add ways to copy all fields & a single field into a BufferedJsonRecord
  public static void mergeRecordDeep(BufferedJsonRecord target, JsonRecord source) throws Exception
  {
    for( Entry<JsonString,JsonValue> f: source )
    {
      JsonString key = f.getKey();
      JsonValue value = f.getValue();
      JsonValue oldValue = target.get(key);
      if( value instanceof JsonRecord ) 
      {
        BufferedJsonRecord newTarget;
        if( oldValue instanceof JsonRecord )
        {
          // oldRec MUST be a BufferedJsonRecord 
          newTarget = (BufferedJsonRecord)oldValue;
        }
        else
        {
          newTarget = new BufferedJsonRecord();
          target.set(key, newTarget);
        }
        mergeRecordDeep(newTarget, (JsonRecord)value);
      }
      else if( value == null )
      {
        // TODO: add remove method for records
        // options.remove(key);
        target.set(key, null);
      }
      else
      {
        target.set(key, value.getCopy(oldValue));
      }
    }
  }
  
  /**
   * Convert an array of JsonEnum into an array of JsonString.
   * For an enum, this is called as JsonUtil.jsonStrings(myenum.values());
   */
  public static JsonString[] jsonStrings(JsonEnum[] jenums)
  {
    JsonString[] names = new JsonString[jenums.length];
    for(int i = 0 ; i < jenums.length ; i++)
    {
      names[i] = jenums[i].jsonString();
    }
    return names;
  }

}
