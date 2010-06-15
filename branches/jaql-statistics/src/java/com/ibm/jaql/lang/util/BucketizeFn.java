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

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonDecimal;
import com.ibm.jaql.json.type.JsonDouble;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.core.MathExpr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.lang.expr.function.Function;
import com.ibm.jaql.lang.expr.function.JaqlFunction;

/**
 * Given a sorted array of json values, divide these values into buckets.
 */
public class BucketizeFn extends IterExpr
{
  private final JsonString MIN_ATTR = new JsonString("Min");
  private final JsonString MAX_ATTR = new JsonString("Max");
  private final JsonLong  MIN_DIFF = new JsonLong(0);
  private final JsonDouble STEP_SIZE_ONE = new JsonDouble(1.0);
  private final double  MARGINE_FACTOR = 2.5;
  
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par33
  {
    public Descriptor()
    {
      super("bucketize", BucketizeFn.class);
    }
  }
  
  /**
   * @param exprs
   */
  public BucketizeFn(Expr[] exprs)
  {
    super(exprs);
  }

  @Override
  public Schema getSchema()
  {
    return SchemaFactory.arraySchema();
  }

  /**
   * A default comparison function that computes the difference (distance) between v1 and v2.
   * If the values are numbers, then the standard MINUS operator is used.
   * If the values are not numbers, then we assume they are the same (for now).
   */
  private JsonValue default_diff_func(JsonValue v1, JsonValue v2)
  {  
	  if ((v1 instanceof JsonLong) || (v1 instanceof JsonDouble) || (v1 instanceof JsonDecimal))
		  return MathExpr.eval(v1, v2, MathExpr.MINUS);
	  else
		  return MIN_DIFF;
  }

  
  /**
   * Computes a reasonable threshold for the max step-size given the data values.
   */
  private JsonValue compute_threshold(JsonArray data) throws Exception 
  {
	  long cnt = data.count();
	  JsonValue min = data.get(0);
	  JsonValue max = data.get(cnt-1);
	  if (((min instanceof JsonLong) || (min instanceof JsonDouble) || (min instanceof JsonDecimal)) &&
	  		((max instanceof JsonLong) || (max instanceof JsonDouble) || (max instanceof JsonDecimal)))
	  {
		  JsonValue diff = MathExpr.eval(max, min, MathExpr.MINUS);
		  JsonValue step = MathExpr.eval(diff, new JsonDouble((cnt/MARGINE_FACTOR)), MathExpr.DIVIDE);
		  return step;
	  }
	  else
		  return STEP_SIZE_ONE;
  }
  
  
  /**
   * Compare the values and create the buckets.
   */
  private BufferedJsonArray create_buckets(JsonArray data, JsonValue stepThreshold, Function user_fun) throws Exception
  {
	  BufferedJsonArray result = new BufferedJsonArray();
	  BufferedJsonRecord r = new BufferedJsonRecord();
	  JsonValue step_threshold = stepThreshold;
	  JsonValue start, end;

	  //If the user-supplied step threshold is null, then figure out a reasonable value.
	  if (step_threshold == null)
		  step_threshold = compute_threshold(data);

	  start = data.get(0);
	  end = data.get(0); 
	  for (int i = 1; i < data.count(); i++)
	  {
		  JsonValue diff;
		  if (user_fun != null)
		  {
			  user_fun.setArguments(data.get(i), end);
			  diff = (user_fun.eval(null));
		  }
		  else
			  diff = default_diff_func(data.get(i), end);
		  
		  if (diff.compareTo(step_threshold) <= 0)
		  {
			  //Extend the current range.
			  end = data.get(i);
			  continue;
		  }
		  
		  //create a bucket for the current range, and start a new range
		  r.clear();
		  r.add(MIN_ATTR, start);
		  r.add(MAX_ATTR, end);
		  result.addCopy(r);
		  start = data.get(i);
		  end = data.get(i);
	  }
	  
	  //create the last bucket.
	  r.clear();
	  r.add(MIN_ATTR, start);
	  r.add(MAX_ATTR, end);
	  result.addCopy(r);	  
	  return result;
  }

/**
   * @param
   * 	exprs[0] (data): is a SORTED array of json values.
   * 	exprs[1] (step_threshold--Can be null): Specifies the maximum step between two consecutive values beyond which a new bucket is created. If 'null' is provided, the threshold is computed automatically.
   *    exprs[2] (jaql_udf--Can be null): is a jaql function that implements the comparison function between to values in the input data array. If 'null' is provided, a default function (based on the data type) is used.
   * 	
   * @return
   * 	Array of json records, each record represents one bucket. The record format is:
   * 	{
   * 		Min: <bucket min. value>
   * 		Max: <bucket max. value>
   * 	}  
   * See "src/test/com/ibm/jaql/DataStatistics.txt" for examples.
   */
  @Override
  public JsonIterator iter(final Context context) throws Exception
  {
    JsonArray data = (JsonArray) exprs[0].eval(context);
    JsonValue step_threshold = (JsonValue) exprs[1].eval(context);
    Function user_fun = (Function) exprs[2].eval(context);
    
    if ((data == null) || data.isEmpty())
      return JsonIterator.EMPTY; 

    BufferedJsonArray result = create_buckets(data, step_threshold, user_fun);
    return result.iter();
  }
}
