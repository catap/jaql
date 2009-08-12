package com.ibm.jaql.json.type;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.UndeclaredThrowableException;

import com.ibm.jaql.io.serialization.text.TextFullSerializer;

/** Utility methods for dealing with {@link JsonValue}s and <code>null</code>s. */
public class JsonUtil
{
  /**
   * Print <code>value</code>, if non-null, on the stream in (extended) JSON text format using
   * <code>v.print(out)</code>. Otherwise, prints <code>null</code>. 
   * 
   * @param value a value or <code>null</code>
   * @param out an output stream
   * @param indent indentation value
   * @throws Exception
   */
  public static void print(PrintStream out, JsonValue value) throws IOException {
    // TODO: remove?
    TextFullSerializer serializer = TextFullSerializer.getDefault();
    serializer.write(out, value);
  }

  /**
   * Print <code>value</code>, if non-null, on the stream in (extended) JSON text format using
   * <code>v.print(out, indent)</code>. Otherwise, prints <code>null</code>. 
   * 
   * @param value a value or <code>null</code>
   * @param out an output stream
   * @param indent indentation value
   * @throws Exception
   */
  public static void print(PrintStream out, JsonValue value, int indent) throws IOException {
    // TODO: remove?
    TextFullSerializer serializer = TextFullSerializer.getDefault();
    serializer.write(out, value, indent);
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
  public static String printToString(JsonValue value) throws IOException {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(bout);
    print(out, value);
    return bout.toString();
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
}
