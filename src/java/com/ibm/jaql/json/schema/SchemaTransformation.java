package com.ibm.jaql.json.schema;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.ibm.jaql.util.Bool3;

/** Useful methods for schema transformation */
public class SchemaTransformation
{
  // -- combination -------------------------------------------------------------------------------
  
  /** Merges schemata <code>s1</code> and <code>s2</code>. The resulting schema will match
   * (1) any value that is matched by <code>s1</code> and/or <code>s2</code> and (2) some 
   * other values. The methods tries to both keep the size of schema description small and to
   * minimize (2).  
   * 
   * Commutative: merge(s1,s2)==merge(s2,s1)
   * 
   * See {@link #or(List<Schema>)} for lossless merging.
   */
  public static Schema merge(Schema s1, Schema s2)
  {
    // try a direct merge
    if (s1 instanceof OrSchema) // try OrSchema merge second
    {
      Schema t = s1;
      s1 = s2;
      s2 = t;
    }
    Schema mergedSchema = s1.merge(s2);
    if (mergedSchema == null) mergedSchema = s2.merge(s1);
    
    // if not successful, OR them
    if (mergedSchema == null)
    {
      return new OrSchema(s1, s2);
    }
    else
    {
      return mergedSchema;    
    }
  }
  
  /** Combines schemata <code>s1</code> and <code>s2</code>. The resulting schema will match
   * precisely the values matched by <code>s1</code> and/or <code>s2</code>. 
   * 
   * See {@link #merge(Schema, Schema)} for lossy merging. */
  public static Schema or(Schema s1, Schema s2)
  {
    List<Schema> unnestedSchemata = new LinkedList<Schema>();
    unnestedSchemata.addAll(unnestOrs(s1));
    unnestedSchemata.addAll(unnestOrs(s2));
    return new OrSchema(unnestedSchemata);
  }

  /** Combines schemata <code>s1</code>, <code>s2</code> and <code>s3</code>. The resulting schema 
   * will match precisely the values matched by <code>s1</code>, <code>s2</code>, and/or 
   * <code>s3</code>. 
   * 
   * See {@link #merge(Schema, Schema)} for lossy merging. */
  public static Schema or(Schema s1, Schema s2, Schema s3)
  {
    List<Schema> unnestedSchemata = new LinkedList<Schema>();
    unnestedSchemata.addAll(unnestOrs(s1));
    unnestedSchemata.addAll(unnestOrs(s2));
    unnestedSchemata.addAll(unnestOrs(s3));
    return new OrSchema(unnestedSchemata);
  }

  /** Combines schemata the provided schemata. The resulting schema 
   * will match precisely the values matched by any of the provided schemata. 
   * 
   * See {@link #merge(Schema, Schema)} for lossy merging. */
  public static Schema or(List<Schema> schemata)
  {
    if (schemata.size() == 1)
    {
      return schemata.get(0);
    }
    else
    {
      List<Schema> unnestedSchemata = new LinkedList<Schema>();
      for (Schema s : schemata)
      {
        unnestedSchemata.addAll(unnestOrs(s));
      }
      return new OrSchema(unnestedSchemata);
    }
  }
  
  /** Combines schemata the provided schemata. The resulting schema 
   * will match precisely the values matched by any of the provided schemata. 
   * 
   * See {@link #merge(Schema, Schema)} for lossy merging. */
  public static Schema or(Schema[] schemata)
  {
    if (schemata.length == 1)
    {
      return schemata[0];
    }
    else
    {
      List<Schema> unnestedSchemata = new LinkedList<Schema>();
      for (Schema s : schemata)
      {
        unnestedSchemata.addAll(unnestOrs(s));
      }
      return new OrSchema(unnestedSchemata);
    }
  }
  
  // helper that recursively unnests OrSchema
  private static List<Schema> unnestOrs(Schema schema)
  {
    List<Schema> result;
    if (schema instanceof OrSchema)
    {
      result = new LinkedList<Schema>();
      for (Schema s : ((OrSchema)schema).schemata)
      {
        result.addAll(unnestOrs(s));
      }
    }
    else
    {
      result = new ArrayList<Schema>(1);
      result.add(schema);
    }
    return result;
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
        return or(outOrSchema);
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
    if (s instanceof AnyNonNullSchema)
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
      return new OrSchema(alternatives);
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
      return or(schemata);
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
      return or(schemata);
    }
    
    // fallback
    return SchemaFactory.anyOrNullSchema();
  }
}
