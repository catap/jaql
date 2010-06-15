/*
 * Copyright (C) IBM Corp. 2008.
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
package com.ibm.jaql.lang.util;

import java.util.Enumeration;
import java.util.Hashtable;
import java.lang.Math;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonBinary;
import com.ibm.jaql.json.type.JsonDouble;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

/**
 * This class creates a BloomFilter over a given set of values.
 */
public class BloomFilterFn extends IterExpr
{
	private final double FILTER_FALSE_POS = 0.02;                    /* the false positive probability */
	private final int BYTE_SIZE = 0x08;                              /* eight bits per byte */
	
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par22
  {
    public Descriptor()
    {
      super("bloomFilter", BloomFilterFn.class);
    }
  }
  
  /**
   * @param exprs
   */
  public BloomFilterFn(Expr[] exprs)
  {
    super(exprs);
  }

  @Override
  public Schema getSchema()
  {
    return SchemaFactory.arraySchema();
  }
  
  /**
   * Computes the appropriate length (in bytes) of the bloom filter given the number of distinct values.
   * It is based on the following equation:  len_in_bits = -(distinct_count * Ln(false_positive_probability)/ (Ln2)^2)  
   */
  private int bloom_filter_size(int distinct_count) 
  {
	  int len = (int) -((distinct_count * Math.log(FILTER_FALSE_POS)) / (Math.pow(Math.log(2),2)));
	  len = (int) (len / BYTE_SIZE);
	  assert(len > 0);
	  return len;
  }

  /**
   * Create a bloom filter over the given set of values. 
   * If 'distinct_cutoff' is null, then create the bloom filter regardless of the count of the distinct values.
   * If 'distinct_cutoff' is not null, then create the bloom filter only if the ratio between the distinct values to the total values is less than distinct_cutoff.
   */
  private BufferedJsonArray create_bloomFilter(JsonArray data, JsonValue distinct_cutoff) throws Exception
  {
	  BufferedJsonArray result = new BufferedJsonArray();
	  
	  //Get the distinct values.
	  Hashtable<String, JsonValue> unique_vals = new Hashtable<String, JsonValue>();
	  for (int i = 0; i < data.count(); i++)
	  {		  
		  JsonValue val = data.get(i);
		  unique_vals.put(val.toString(), val);
	  }

	  //If the distinct ratio is larger than the distinct_cutoff, then do not create a bloom filter.
	  if (distinct_cutoff != null) 
	  {
		  double distinct_ratio = (double)unique_vals.size()/data.count();
		  if ((new JsonDouble(distinct_ratio)).compareTo(distinct_cutoff) > 0)
			  return result;
	  }
  
	  //Loop over the distinct values and create a bloom filter
	  Enumeration<JsonValue> e = unique_vals.elements();
	  BloomFilter bf = new BloomFilter(bloom_filter_size(unique_vals.size()));
	  while (e.hasMoreElements())
		  bf.addValueToSignature(e.nextElement());
	  
	  JsonBinary filter_signature = new JsonBinary(bf.getBloomSignature());
	  result.addCopy(filter_signature);
	  return result;
  }

  /*
  private String hex_code(String hex_str) 
  {
	  String output = hex_str.replace("hex(", "").replace(")", "").replace("\\", "").replace("'", "");
	  return output;
  }

  private void readFn(JsonArray data) throws Exception
  {
	  String keys[] = {"Scholastic", "Mohamed", "Yassin", "Grosset", "Eltabakh", "soft", "hard", "Deathly Hallows", "Chamber of Secrets", "Sorcerers Stone", "Monster Blood IV"};
	  JsonString s = new JsonString("code");
	  for (int i = 0; i < data.count(); i++)
	  {
		  JsonString vv = (JsonString)((JsonRecord)data.get(i)).get(s);
		  JsonBinary v = new JsonBinary(hex_code(vv.toString()));
		  BloomFilter bf = new BloomFilter(v.getCopy());

		  System.out.println("Bloom Filter: " + v.getCopy());
		  for (int j = 0; j < keys.length; j++)
		  {
			  if (bf.containsValue(keys[j]))
				  System.out.println("Key: " + keys[j] + "......Found");
			  else
				  System.out.println("Key: " + keys[j] + "......Not Found");				  
		  }
	  }
  }
   */

/**
   * @param 
   * 	exprs[0] (data): array of values on which a bloom filter is created.
   * 	exprs[1] (distinct_cutoff --Can be null): If not null, a bloom filter is created only if the ratio between the distinct values to the total values is less than or equal to 'distinct_cutoff'. 
   *
   * @return Byte array representing the bloom filter
   * 
   * See "src/test/com/ibm/jaql/DataStatistics.txt" for examples.
   */

  @Override
  public JsonIterator iter(final Context context) throws Exception
  {
    JsonArray data = (JsonArray) exprs[0].eval(context);
    JsonValue distinct_cutoff = (JsonValue) exprs[1].eval(context);

    if (data.isEmpty())
    	return JsonIterator.EMPTY; 

    BufferedJsonArray result = create_bloomFilter(data, distinct_cutoff);
    return result.iter();
  }
}
