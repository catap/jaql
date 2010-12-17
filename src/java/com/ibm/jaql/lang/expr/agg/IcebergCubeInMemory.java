/*
 * Copyright (C) IBM Corp. 2010.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.ibm.jaql.lang.expr.agg;

import java.util.Arrays;
import java.util.Comparator;

import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonUtil;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.SpilledJsonArray;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.BuiltInFunctionDescriptor;
import com.ibm.jaql.lang.expr.function.Function;
import com.ibm.jaql.lang.expr.function.JsonValueParameter;
import com.ibm.jaql.lang.expr.function.JsonValueParameters;

/**
 *  
 *
 */
public class IcebergCubeInMemory extends Expr
{
  private final static int ARG_INPUT = 0;
  private final static int ARG_COLUMNS = 1;
  private final static int ARG_PER_GROUP_FN = 2;
  private final static int ARG_MIN_SUPPORT = 3;

  public static class Descriptor implements BuiltInFunctionDescriptor 
  {
    private Schema schema = new ArraySchema(null, SchemaFactory.arraySchema());
    private JsonValueParameters parameters;

    public Descriptor() {
      parameters = new JsonValueParameters(new JsonValueParameter[] {
          new JsonValueParameter("input", SchemaFactory.arrayOrNullSchema()),
          new JsonValueParameter("columns", new ArraySchema(null, SchemaFactory.stringSchema())),
          new JsonValueParameter("perGroupFn", SchemaFactory.functionSchema()),
          new JsonValueParameter("minSupport", SchemaFactory.numericSchema(), JsonLong.ONE)
      });
    }

    @Override
    public Expr construct(Expr[] positionalArgs)
    {
      return new IcebergCubeInMemory(positionalArgs);
    }

    @Override
    public Class<? extends Expr> getImplementingClass() 
    {
      return IcebergCubeInMemory.class;
    }

    @Override
    public String getName()
    {
      return "icebergCubeInMemory";
    }

    @Override
    public JsonValueParameters getParameters()
    {
      return parameters;
    }

    @Override
    public Schema getSchema()
    {
      return schema;
    }
  }

  
  public IcebergCubeInMemory(Expr... inputs)
  {
    super(inputs);
  }

  @Override
  public Schema getSchema()
  {
    return SchemaFactory.arraySchema(); // TODO: refine
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  @Override
  protected JsonValue evalRaw(final Context context) throws Exception
  {
    JsonArray colNamesArray = (JsonArray)exprs[ARG_COLUMNS].eval(context);
    colNames = new JsonString[(int)colNamesArray.count()];
    colNamesArray.getAll(colNames);
    perGroupFn = (Function)exprs[ARG_PER_GROUP_FN].eval(context);
    minSupport = ((JsonNumber)exprs[ARG_MIN_SUPPORT].eval(context)).longValue();
    
    fieldComparators = new FieldComparator[colNames.length];
    for(int i = 0 ; i < colNames.length ; i++)
    {
      fieldComparators[i] = new FieldComparator(colNames[i]);
    }

    loadData();
    
    key = new BufferedJsonRecord();
    tmpData = new JsonRecord[size];
    group = new BufferedJsonArray();
    result = new SpilledJsonArray();

    if( size >= minSupport )
    {
      aggregate(0,size);
      long colset = BitSet64.range(0, colNames.length);
      for( int c = 0 ; c < colNames.length ; c++ ) 
      {
        process(0,size,colset);
        colset = BitSet64.clear(colset,c);
      }
    }
    
    // TODO: unwind recursion to make streaming?
    // TODO: make API that gets a json writer? 
    //    then we can either use queues to make streaming or write result to output file.
    //    default write(writer) is writer.write(this.eval(ctx)) or writer.write(this.iter(ctx))
    return result; 
  }

  JsonString[] colNames;
  FieldComparator[] fieldComparators;
  BufferedJsonRecord key;
  JsonRecord[] data;
  int size;
  long minSupport;
  SpilledJsonArray result;
  BufferedJsonArray group;
  JsonRecord[] tmpData;
  Function perGroupFn;
  Context context;
  
  protected void loadData() throws Exception
  {
    data = new JsonRecord[8192];
    int i = 0;
    final JsonIterator iter = exprs[ARG_INPUT].iter(context);
    for( JsonValue val: iter )
    {
      if( i >= data.length )
      {
        JsonRecord[] tmp = new JsonRecord[i * 2];
        System.arraycopy(data, 0, tmp, 0, data.length);
      }
      data[i++] = JsonUtil.getCopy((JsonRecord)val, null);
    }
    size = i;
  }
  
  protected void process(
      int start, 
      int end, 
      long colset)
    throws Exception
  {
    if( colset == 0 )
    {
      return;
    }
    
    int col = BitSet64.nextSetBit(colset, 0);
    colset = BitSet64.clear(colset, col);
    JsonString colName = colNames[col];
    int colIndex = key.add(colName, null);    
    
    Comparator<JsonRecord> cmp = fieldComparators[col];
    Arrays.sort(data, start, end, cmp);
    
    // Find the group breaks
    for( int i = start + 1 ; i <= end ; i++ )
    {
      if( i == end || 
          cmp.compare(data[start], data[i]) != 0 )
      {
        if( i - start >= minSupport )
        {
          key.set(colIndex, data[start].get(colName));
          aggregate(start,i);
          // recurse
          long colset2 = colset;
          for( int c = BitSet64.nextSetBit(colset,col+1) ; 
               c >= 0 ; 
               c = BitSet64.nextSetBit(colset,c+1) )
          {
            process(start,i,colset2);
            colset2 = BitSet64.clear(colset2,c);
          }
        }
        start = i;
      }
    }
    
    key.remove(colIndex);
  }

  protected void aggregate(int start, int end) throws Exception
  {
    int n = end - start;
    System.arraycopy(data, start, tmpData, 0, n);
    group.set(tmpData, n); // TODO: add start index to BufferedJsonArray to avoid copy
    perGroupFn.setArguments(group, key);
    JsonIterator iter = perGroupFn.iter(context);
    result.addCopyAll(iter);
  }
  
  public static class FieldComparator implements Comparator<JsonRecord>
  {
    protected JsonString fieldName;
    
    public FieldComparator(JsonString fieldName)
    {
      this.fieldName = fieldName;
    }
    
    @Override
    public int compare(JsonRecord x, JsonRecord y)
    {
      if( x == null )
      {
        if( y == null )
        {
          return 0;
        }
        return -1;
      }
      else if( y == null )
      {
        return 1; 
      }
      else
      {
        JsonValue vx = x.get(fieldName);
        JsonValue vy = y.get(fieldName);
        return JsonUtil.compare(vx, vy);
      }
    }
  }
  
  public static class BitSet64 // TODO: move
  {
    /** Returns the number of true bits in set. */
    public static int cardinality(long set)
    {
      return Long.bitCount(set);
    }
    
    /** return the set of bits from fromIndex(inclusive) to toIndex(exclusive). */
    public static long range(int fromIndex, int toIndex)
    {
      int n = toIndex - fromIndex; 
      if( n <= 0 )
      {
        return 0;
      } 
      else if( n >= 64 )
      {
        return -1;
      }
      else
      {
        return ((1L << n) - 1) << fromIndex;
      }
    }
    
    /** Returns the value of the bit with the specified index. */
    public static boolean get(long set, int bitIndex)
    {
      return (set & (1L << bitIndex)) != 0;
    }
    
    /** Returns a new set composed of bits from set from fromIndex(inclusive) to toIndex(exclusive). */
    public static long get(long set, int fromIndex, int toIndex)
    {
      return set & range(fromIndex,toIndex);
    }

    /** Returns the index of the first bit that is set to false that occurs on or after the specified starting index.*/
    public static int nextClearBit(long set, int fromIndex)
    {
      int i = Long.numberOfTrailingZeros(~(set & range(fromIndex,64)));
      return i >= 64 ? -1 : i;
    }
    
    /** Returns the index of the first bit that is set to true that occurs on or after the specified starting index. */
    public static int nextSetBit(long set, int fromIndex)
    {
      int i = Long.numberOfTrailingZeros(set & range(fromIndex,64));
      return i >= 64 ? -1 : i;
    }

    /** Sets the bit at the specified index to true. */
    public static long set(long set, int bitIndex)
    {
      return set | (1L << bitIndex);
    }
    
    /** Sets the bit at the specified index to the specified value. */
    public static long set(long set, int bitIndex, boolean value)
    {
      return value ? set(set, bitIndex) : clear(set, bitIndex);
    }
    
    /** Sets the bits from the specified fromIndex(inclusive) to the specified toIndex(exclusive) to true. */
    public static long set(long set, int fromIndex, int toIndex)
    {
      return set | range(fromIndex, toIndex);
    }
    
    /** Sets the bits from the specified fromIndex(inclusive) to the specified toIndex(exclusive) to the specified value. */
    public static long set(long set, int fromIndex, int toIndex, boolean value)
    {
      return value ? set(set, fromIndex, toIndex) : clear(set, fromIndex, toIndex);
    }
    
    /** Sets the bit specified by the index to false. */
    public static long clear(long set, int bitIndex)
    {
      return set & ~(1L << bitIndex); 
    }
    
    /** Sets the bits from the specified fromIndex(inclusive) to the specified toIndex(exclusive) to false. */
    public static long clear(long set, int fromIndex, int toIndex)
    {
      return set & ~range(fromIndex, toIndex);
    }
    
    /* Sets the bit at the specified index to to the complement of its current value. */
    public static long flip(long set, int bitIndex)
    {
      return set ^ (1 << bitIndex);
    }
    
    /** Sets each bit from the specified fromIndex(inclusive) to the specified toIndex(exclusive) to the complement of its current value. */
    public static long flip(long set, int fromIndex, int toIndex)
    {
      return set ^ range(fromIndex,toIndex);
    }
    
    /** Returns true if the set1 and set2 have any true bits in common. */
    public static boolean intersects(long set1, long set2)
    {
      return (set1 & set2) != 0;
    }    

    /** Performs a logical AND set1 and set2 */
    public static long and(long set1, long set2)
    {
      return set1 & set2;
    }
    
    /** Clears all of the bits in set1 whose corresponding bit is set in the specified set2. */
    public static long andNot(long set1, long set2)
    {
      return set1 & ~set2;
    }
    
    /** Performs a logical OR of this bit set with the bit set argument. */
    public static long or(long set1, long set2)
    {
      return set1 | set2;
    }
    /** Performs a logical XOR of this bit set with the bit set argument. */
    public static long xor(long set1, long set2)
    {
      return set1 ^ set2;
    }
  }
}
