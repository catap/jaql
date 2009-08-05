package com.ibm.jaql.json.schema;

import java.util.LinkedList;
import java.util.List;

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
        result = OrSchema.or(result, next);
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
    Schema sin = OrSchema.or(schemata);
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
        if (merged.getSchemaType() != current.getSchemaType())
        {
          // should not happen if all Schema implementations obey the merge() contract
          throw new IllegalStateException("merge of schema type " + current.getSchemaType() + " failed");
        }
        current = merged;
      }
    }
    out.add(current);
    
    return OrSchema.or(out);
  }
  
  
  // -- null values -------------------------------------------------------------------------------
  
  /** Returns a schema that matches the same values as the input schema except <code>null</code>.
   * Returns <code>null</code> when input schema matches <code>null</code> only. */
  public static Schema removeNullability(Schema inSchema)
  {
    Bool3 isNull = inSchema.isNull();
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
        return OrSchema.or(outOrSchema);
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
    if (s.isNull().maybe())
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
    Schema array = restrictToArray(s);
    return array != null ? array.elements() : null;
  }
  
  /** Returns a schema that matches all arrays matched by <code>s</code>. Returns <code>null</code> 
   * if <code>s</code> does not match any array. */
  public static Schema restrictToArray(Schema s)
  {
    Schema result = restrictToArrayOrNull(s);
    return result!=null ? removeNullability(result) : null;
  }
  
  /** Returns a schema that matches the arrays as well as the <code>null</code> value matched 
   * by <code>s</code>. Returns <code>null</code> if <code>s</code> does not match any array or 
   * <code>null</code>. */
  public static Schema restrictToArrayOrNull(Schema s)
  {
    if (s instanceof NonNullSchema)
    {
      return SchemaFactory.arraySchema();
    }
    if (s instanceof ArraySchema || s instanceof NullSchema)
    {
      return s;
    }
    if (s instanceof OrSchema)
    {
      List<Schema> alternatives = new LinkedList<Schema>();
      for (Schema os : ((OrSchema) s).schemata)
      {
        Schema ns = restrictToArrayOrNull(os);
        if (ns != null) alternatives.add(ns);
      }
      if (alternatives.size() == 0) return null;
      return OrSchema.or(alternatives);
    }
    return null;
  }

  /** Wraps non-null, non-array parts of the provided schema into a single-element array. Array
   * and null elements are retained. */
  public static Schema wrapIntoArrayOrNull(Schema inSchema)
  {
    if (inSchema.isArrayOrNull().always())
    {
      return inSchema;
    }
    if (inSchema.isArrayOrNull().never())
    {
      return new ArraySchema(inSchema);
    }
  
    // try unnesting
    if (inSchema instanceof OrSchema)
    {
      OrSchema o = (OrSchema)inSchema;
      List<Schema> schemata = new LinkedList<Schema>();
      boolean unresolved = false;
      for (Schema s : o.getInternal())
      {
        if (s.isArrayOrNull().always())
        {
          schemata.add(s);
        }
        else if (s.isArrayOrNull().never())
        {
          schemata.add(new ArraySchema(s));
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
      return OrSchema.or(schemata);
    }
    
    // otherwise we just know that it will be an array (or null)
    return inSchema.isNull().never() ? SchemaFactory.arraySchema() : SchemaFactory.arrayOrNullSchema();
  }

  /** Returns a schema that matches all non-array values matched by <code>inSchema</code> as well
   * as all (first-level) elements of arrays matched by <code>inSchema</code>. Never returns 
   * <code>null</code>. */
  public static Schema expandArrays(Schema inSchema)
  {
    if (inSchema.isArray().never())
    {
      return inSchema;
    }
  
    if (inSchema.isArray().always())
    {
      return inSchema.elements();
    }
    
    // try unnesting
    if (inSchema instanceof OrSchema)
    {
      OrSchema o = (OrSchema)inSchema;
      List<Schema> schemata = new LinkedList<Schema>();
      for (Schema s : o.getInternal())
      {
        Schema se = expandArrays(s);
        schemata.add(se);
      }
      return OrSchema.or(schemata);
    }
    
    // fallback
    return SchemaFactory.anySchema();
  }
}
