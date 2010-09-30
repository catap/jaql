package com.ibm.jaql.json.schema;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import static com.ibm.jaql.json.type.JsonType.*;

import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonType;
import com.ibm.jaql.util.Bool3;

/** Useful methods for schema transformation */
public class SchemaTransformation
{
  // -- combination -------------------------------------------------------------------------------
  
  /** Merges its input schemata. The resulting schema will match (1) any value that is matched by 
   * at least one of the provided schemata (2) some other values. The methods tries to both keep 
   * the size of schema description small and to minimize (2).  
   * 
   * See {@link #or(List<Schema>)} for lossless merging.
   */
  public static Schema merge(Schema ... schemata)
  {
    // invalid input
    if (schemata.length == 0)
    {
      throw new IllegalArgumentException("at least one schema has to be provided");
    }
    
    // single input
    if (schemata.length == 1) {
      return schemata[0];
    }
      
    // more than one input
    Schema result = schemata[0];
    for (Schema next : schemata)
    {
      // try a direct merge
      if (result instanceof OrSchema) // try OrSchema merge second
      {
        Schema t = result;
        result = next;
        next = t;
      }
      Schema mergedSchema = result.merge(next);
      if (mergedSchema == null) mergedSchema = next.merge(result);
    
      // if not successful, OR them
      if (mergedSchema == null)
      {
        result = OrSchema.make(result, next);
      }
      else
      {
        result = mergedSchema;    
      }
    }
    return result;
  }

  /** Compacts its input schemata by combining multiple alternatives of the same schema type
   * into a single alternative. This is different to {@link #merge}, which also tries to combine
   * different schema types. */
  public static Schema compact(Schema ... schemata)
  {
    // unnest, sort and remove duplicates
    Schema sin = OrSchema.make(schemata);
    if ( !(sin instanceof OrSchema) )
    {
      return sin;
    }
    Schema[] in = ((OrSchema)sin).schemata; 
    
    List<Schema> out = new LinkedList<Schema>();
    Schema current = in[0];
    for (int i=1; i<in.length; i++)
    {
      Schema next = in[i];
      if (current.getSchemaType() != next.getSchemaType())
      {
        out.add(current);
        current = next;
      }
      else
      {
        Schema merged = current.merge(next);
        
        // merge can return null
        if( merged == null ) 
        {
        	// merge failed so keep current and next separate
        	out.add(current);
        	current = next;
        } 
        else if (merged.getSchemaType() != current.getSchemaType())
        {
          // should not happen if all Schema implementations obey the merge() contract
          throw new IllegalStateException("merge of schema type " + current.getSchemaType() + " failed");
        } else {
        	// merge succeeded so keep the merged result
        	current = merged;
        }
      }
    }
    out.add(current);
    
    return OrSchema.make(out);
  }
  
  
  // -- null values -------------------------------------------------------------------------------
  
  /** Returns a schema that matches the same values as the input schema except <code>null</code>.
   * Returns <code>null</code> when input schema matches <code>null</code> only. */
  public static Schema removeNullability(Schema inSchema)
  {
    Bool3 isNull = inSchema.is(NULL);
    if (isNull.always())
    {
      return null;
    }
    if (isNull.never()) {
      return inSchema;
    }
    
    if (inSchema instanceof OrSchema)
    {
      OrSchema inOrSchema = (OrSchema)inSchema;
      List<Schema> outOrSchema = new LinkedList<Schema>();
      for (Schema schema : inOrSchema.schemata) 
      {
        Schema newSchema = removeNullability(schema);
        if (newSchema != null)
        {
          outOrSchema.add(newSchema);
        }
      }
      if (outOrSchema.size() > 0)
      {
        return OrSchema.make(outOrSchema);
      }
      return null;
    }
    
    // cannot happen
    throw new IllegalArgumentException();
  }

  /** Returns a schema that matches the same values as the input schema as well as 
   * <code>null</code>. */
  public static Schema addNullability(Schema s)
  {
    if (s.is(NULL).maybe())
    {
      return s;
    }
    else
    {
      return SchemaTransformation.merge(s, SchemaFactory.nullSchema());
    }
  }

  
  // -- arrays ------------------------------------------------------------------------------------
  
  /** Returns a common schema of the elements of all arrays matched by the given schema. */
  public static Schema arrayElements(Schema s)
  {
    Schema array = restrictTo(s, JsonType.ARRAY);
    return array != null ? array.elements() : null;
  }

  /** Returns a common schema of the elements of all records matched by the given schema. */
  public static Schema recordElements(Schema s)
  {
    Schema record = restrictTo(s, JsonType.RECORD);
    return record != null ? record.elements() : null;
  }

  /** Shortcut for <code>restrictTo(schema, JsonType.ARRAY)</code> */
  public static Schema restrictToArray(Schema schema)
  {
    return restrictTo(schema, JsonType.ARRAY);
  }

  /** Shortcut for <code>restrictTo(schema, JsonType.ARRAY, JsonType.NULL)</code> */
  public static Schema restrictToArrayOrNull(Schema schema)
  {
    return restrictTo(schema, JsonType.ARRAY, JsonType.NULL);
  }
  
  /** Restrict schema to an array or null, but convert nulls to [] */
  public static Schema restrictToArrayWithNullPromotion(Schema schema)
  {
    if( schema == null )
    {
      return null;
    }
    if( schema.is(JsonType.NULL).never() )
    {
      return restrictToArray(schema);
    }
    Schema empty = SchemaFactory.emptyArraySchema();
    Schema array = SchemaTransformation.restrictToArray(schema);
    if( array == null )
    {
      return empty;
    }
    JsonLong min = array.minElements();
    if( min != null && min.get() == 0 )
    {
      return array;
    }
    return OrSchema.make(array, empty);
  }
  

  /** Shortcut for <code>restrictTo(schema, JsonType.RECORD)</code> */
  public static Schema restrictToRecord(Schema schema)
  {
    return restrictTo(schema, JsonType.RECORD);
  }

  /** Shortcut for <code>restrictTo(schema, JsonType.RECORD, JsonType.NULL)</code> */
  public static Schema restrictToRecordOrNull(Schema schema)
  {
    return restrictTo(schema, JsonType.RECORD, JsonType.NULL);
  }

  /** Shortcut for <code>restrictTo(schema, JsonType.LONG, JsonType.DOUBLE, JsonType.DECFLOAT)</code> */
  public static Schema restrictToNumber(Schema schema)
  {
    return restrictTo(schema, JsonType.LONG, JsonType.DOUBLE, JsonType.DECFLOAT);
  }
  
  /** Shortcut for <code>restrictTo(schema, JsonType.LONG, JsonType.DOUBLE, JsonType.DECFLOAT, JsonType.NULL)</code> */
  public static Schema restrictToNumberOrNull(Schema schema)
  {
    return restrictTo(schema, JsonType.LONG, JsonType.DOUBLE, JsonType.DECFLOAT, JsonType.NULL);
  }
  
  /** As {@link #restrictToNumber(Schema)} but removes modifiers of number schemata. */
  public static Schema restrictToNumberTypes(Schema schema)
  {
    Schema s = restrictToNumberTypesOrNull(schema);
    if (s==null) return null;
    return removeNullability(s);
  }
  
  /** As {@link #restrictToNumberOrNull(Schema)} but removes modifiers of number schemata. */
  public static Schema restrictToNumberTypesOrNull(Schema schema)
  {
    // restrict schema to numeric types
    List<Schema> restricted = new ArrayList<Schema>(4);
    if (schema.is(JsonType.LONG).maybe())
    {
      restricted.add(SchemaFactory.longSchema());
    }
    if (schema.is(JsonType.DOUBLE).maybe())
    {
      restricted.add(SchemaFactory.doubleSchema());
    }
    if (schema.is(JsonType.DECFLOAT).maybe())
    {
      restricted.add(SchemaFactory.decfloatSchema());
    }
    if (schema.is(JsonType.NULL).maybe())
    {
      restricted.add(SchemaFactory.nullSchema());
    }
    if (restricted.size() > 0)
    {
      return OrSchema.make(restricted);
    }
    return null;
  }
  
  /** Returns a schema that matches the values that are (1) matched by the provided <code>schema</code> 
   * and (2) of one of the specified <code>types</code>, or <code>null</code> if no such restriction 
   * exists. For example,
   * <code>restrictTo(schema, SchemaType.ARRAY)</code> will only retain arrays, while
   * <code>restrictTo(schema, SchemaType.ARRAY, SchemaType.NULL)</code> will retain arrays and null. 
   */
  public static Schema restrictTo(Schema schema, JsonType ... types)
  {
    if (schema instanceof NonNullSchema)
    {
      return removeNullability( SchemaFactory.make(types) );
    }
    if (schema instanceof OrSchema)
    {
      // we are given a multiple alternative schemata
      List<Schema> schemata = new ArrayList<Schema>(types.length);
      for (Schema unrestricted : ((OrSchema)schema).get())
      {
        Schema restricted = restrictTo(unrestricted, types);
        if (restricted != null)
        {
          schemata.add(restricted);
        }
      }
      if (schemata.size() == 0)
      {
        return null;
      }
      else
      {
        return OrSchema.make(schemata);
      }      
    }
    else
    {
      // we are given a specific schema (!= nonnull, != or)
      for (JsonType type : types)
      {
        if (schema.is(type).maybe())
        {
          return schema;
        }
      }
      return null;
    }  
  }

  /** Wraps non-null, non-array parts of the provided schema into a single-element array. Array
   * and null elements are retained. */
  public static Schema wrapIntoArrayOrNull(Schema inSchema)
  {
    if (inSchema.is(ARRAY, NULL).always())
    {
      return inSchema;
    }
    if (inSchema.is(ARRAY, NULL).never())
    {
      return new ArraySchema(new Schema[] { inSchema });
    }
  
    // try unnesting
    if (inSchema instanceof OrSchema)
    {
      OrSchema o = (OrSchema)inSchema;
      List<Schema> schemata = new LinkedList<Schema>();
      boolean unresolved = false;
      for (Schema s : o.get())
      {
        if (s.is(ARRAY, NULL).always())
        {
          schemata.add(s);
        }
        else if (s.is(ARRAY, NULL).never())
        {
          schemata.add(new ArraySchema(new Schema[] { s }));
        }
        else
        {
          unresolved = true;
        }
      }
      if (unresolved)
      {
        schemata.add(SchemaFactory.arraySchema());
      }
      return OrSchema.make(schemata);
    }
    
    // otherwise we just know that it will be an array (or null)
    return inSchema.is(NULL).never() ? SchemaFactory.arraySchema() : SchemaFactory.arrayOrNullSchema();
  }

  /** Returns a schema that matches all non-array values matched by <code>inSchema</code> as well
   * as all (first-level) elements of arrays matched by <code>inSchema</code>. Never returns 
   * <code>null</code>. */
  public static Schema expandArrays(Schema inSchema)
  {
    if (inSchema.is(ARRAY).never())
    {
      return inSchema;
    }
  
    if (inSchema.is(ARRAY).always())
    {
      return inSchema.elements();
    }
    
    // try unnesting
    if (inSchema instanceof OrSchema)
    {
      OrSchema o = (OrSchema)inSchema;
      List<Schema> schemata = new LinkedList<Schema>();
      for (Schema s : o.get())
      {
        Schema se = expandArrays(s);
        schemata.add(se);
      }
      return OrSchema.make(schemata);
    }
    
    // fallback
    return SchemaFactory.anySchema();
  }
}
