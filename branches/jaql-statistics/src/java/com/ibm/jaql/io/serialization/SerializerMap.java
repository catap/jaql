package com.ibm.jaql.io.serialization;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ibm.jaql.json.schema.NonNullSchema;
import com.ibm.jaql.json.schema.NullSchema;
import com.ibm.jaql.json.schema.OrSchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.type.JsonValue;

/** 
 * This is a helper class that can be used to implement a schema-aware {@link FullSerializer}s.
 * It provides a map from a {@link JsonValue} to a <code>SerializerInfo</code> (defined by 
 * subclasses) for that specific value. The serializer info object is picked according to the 
 * schema of the input value.  
 * 
 * In more detail, this class takes as input the common schema of the values of 
 * interest. Then,
 * <ol>
 * <li> If the schema is a {@link NullSchema}, it stores information that null values are matched.
 *      See {@link #matchesNull}. 
 * <li> If the schema is a {@link NonNullSchema}, stores information that nonnull values 
 *      are matched. See {@link #matchesNonNull}.
 * <li> If the schema is not an {@OrSchema}, creates a single serializer info object for the schema 
 *      (using a method defined by subclasses).
 * <li> If the schema is an {@OrSchema}, performs 1-3 for each alternative schema.
 * </ol>
 * The {@link #get(JsonValue)} method then retrieves the right serializer info object for a given 
 * JSON value. 
 * 
 * @param SerializerInfo class that stores information about a serializer
 */
public abstract class SerializerMap<SerializerInfo>
{
  /** The schema for values handled by this map */
  Schema schema;

  /** true if the schema matches "null" */
  boolean matchesNull;
  
  /** true if the schema matches "nonnull" */
  boolean matchesNonNull;
  
  /** true when <code>schema</code> is an instance of an {@link OrSchema} so that there are more 
   * then one case */
  boolean hasAlternatives;
  
  /** Maps subclasses of JsonValue to a list of possible serializers for this subclass */ 
  private Map<Class<? extends JsonValue>, List<SerializerInfo>> serializerMap;
  
  /** A list of serializers (ordered by time of creation) */
  private List<SerializerInfo> serializerList;
  
  
  // -- construction ------------------------------------------------------------------------------
  
  /** Creates a new serializer map for the given schema. Uses the abstract 
   * {@link #makeSerializerInfo(int, Schema)} method to create serializers.
   */
  public SerializerMap(Schema schema)
  {
    this.schema = schema;
    serializerMap = new HashMap<Class<? extends JsonValue>,  List<SerializerInfo>>();
    serializerList = new ArrayList<SerializerInfo>();
    
    // nonnull and null are treated separately
    matchesNonNull = false;
    matchesNull = false;
    
    // expand OrSchema
    if (schema instanceof OrSchema)
    {
      hasAlternatives = true;
      for (Schema s : ((OrSchema)schema).get())
      {
        add(s); // s is not an OrSchema because OrSchemas are not nested
      }
    }
    else 
    {
      hasAlternatives = false;
      add(schema);
    }
  }
  
  /** Adds the given schema to the internal data structures */
  private void add(Schema schema)
  {
    assert !(schema instanceof OrSchema);
    if (schema instanceof NullSchema)
    {
      matchesNull = true;
    } 
    else if (schema instanceof NonNullSchema)
    {
      matchesNonNull = true;
    }
    else
    {
      // add serializer to list
      SerializerInfo entry = makeSerializerInfo(serializerList.size(), schema);
      serializerList.add(entry);
      
      // and to map
      for (Class<? extends JsonValue> clazz : schema.matchedClasses())
      {
        List<SerializerInfo> list = serializerMap.get(clazz);
        if (list == null)
        {
          list = new ArrayList<SerializerInfo>();
        }
        list.add(entry);
        serializerMap.put(clazz, list);
      }
    }    
  }
  

  // -- abstract methods --------------------------------------------------------------------------
  
  /** Create a serializer info object for the given schema.
   *
   * @param pos the position of the schema (increasing from 0,1,2...)
   * @param schema a schema (guaranteeed to be not a {@link NullSchema}, {@link NonNullSchema}, or
   *        {@link OrSchena})  
   */
  public abstract SerializerInfo makeSerializerInfo(int pos, Schema schema);
  
  /** Returns the schema associated with the given serializer info object */
  public abstract Schema schemaOf(SerializerInfo info);
  
  
  // -- getters -----------------------------------------------------------------------------------

  /** true if the schema matches "null" */
  public boolean matchesNull()
  {
    return matchesNull;
  }

  /** true if the schema matches "nonnull" */
  public boolean matchesNonNull()
  {
    return matchesNonNull;
  }

  /** Returns <code>true </code>when schema used to create this map is an instance of an 
   * {@link OrSchema} so that there are more than one possible serializers. If this method 
   * returns <code>false</code>, then input values either exactly one of the methods 
   * {@link #matchesNull} and {@link #matchesNonNull}, or both methods return false and there
   * is a single serializer info object stored in this map. */
  public boolean hasAlternatives()
  {
    return hasAlternatives;
  }
  
  /** Get a serializer info object by position (creation order). */
  public SerializerInfo get(int position)
  {
    return serializerList.get(position);
  }
  
  /** Returns a serializer info object for the given non-null value. It is not guaranteed that the 
   * schema associated with returned serializer info object matches <code>value</code>. It is 
   * guaranteed though that if there is a schema that matches <code>value</code>, it's serializer 
   * info object is returned. */  
  public SerializerInfo get(JsonValue value)
  {
    assert value != null;
    
    // non-null values
    List<SerializerInfo> list = serializerMap.get(value.getClass());
    if (list == null)
    {
      // search for super classes
      Class<?> superClass = value.getClass().getSuperclass();
      while (superClass != null)
      {
        list = serializerMap.get(superClass);
        if (list != null)
        {
          serializerMap.put(value.getClass(), list);
          break;
        }
        superClass = superClass.getSuperclass();
      }
      if (list == null) return null; // failed
    }
    
    int n = list.size();
    // n-1 to avoid expensive matches(...) computation in the common case where there is 
    // just one serializer
    for (int i=0; i<n-1; i++) 
    {
      SerializerInfo s = list.get(i);
      try
      {
        if (schemaOf(s).matches(value)) { 
          return s;
        }
      } catch (Exception e)
      {
      }
    }
    SerializerInfo result = list.get(n-1);
    if (matchesNonNull && !schemaOf(result).matchesUnsafe(value)) // saves matches(...) check when possible 
    {
      return null;
    }
    return result;
  }
  
}
