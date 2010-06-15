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
package com.ibm.jaql.lang.expr.record;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.BufferedJsonArray;             
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonType;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.JaqlQuery;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.core.MathExpr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.lang.expr.record.FieldPath.Type;

/**
 * @Returns For each input record, this class returns an array of records, each record is in the form of:
 * {
 * 		AttrName: <root-to-leaf path name>,
 * 		AttrValue: <path value (post-processed)>
 * 		AttrType: <Data type of the AttrValue>
 * }
 */
public class PathValues extends IterExpr
{
	public static final String ATTR_FIELD_NAME = "AttrName"; 
	public static final String ATTR_VALUE_NAME = "AttrValue";
	public static final String ATTR_TYPE_NAME = "AttrType";
	public static final String ATTR_ORDER_NAME = "SortingInfo";
	public static final String ATTR_COUNT_NAME = "CountValues";
	public static final String ATTR_DISTINCT_NAME = "CountDistinct";
	public static final String ATTR_FILTER_NAME = "BloomFilter";

	private final String VAL_NOT_APPLICABLE = "N/A";
	private final JsonString JSON_ATTR_FIELD_NAME = new JsonString(ATTR_FIELD_NAME);
	private final JsonString JSON_ATTR_VALUE_NAME = new JsonString(ATTR_VALUE_NAME);
	private final JsonString JSON_ATTR_TYPE_NAME = new JsonString(ATTR_TYPE_NAME);
	private final JsonString JSON_VAL_NOT_APPLICABLE = new JsonString(VAL_NOT_APPLICABLE);

	private BufferedJsonArray result = new BufferedJsonArray();

	public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11
	{
		public Descriptor()
		{
			super("pathValues", PathValues.class);
		}
	}

  /**
   * pathValues(rec) paths(exprs[0])
   * 
   * @param exprs
   */
  public PathValues(Expr[] exprs)
  {
    super(exprs);
  }

  @Override
  public Schema getSchema()
  {
    return SchemaFactory.arraySchema();
  }

  /**
   * Adds a record to the output array.
   */
  private void add_to_output(JsonString field_name, JsonType field_type, JsonValue val) throws Exception
  {
	  BufferedJsonRecord s = new BufferedJsonRecord();
	  
	  //numeric datatypes should be promoted to a common type
	  String type_str = "";
	  if (MathExpr.promote(field_type, JsonType.DECFLOAT) != null)
		  type_str = (JsonType.DECFLOAT).toString();
	  else
		  type_str = field_type.toString();
 
	  s.add(JSON_ATTR_FIELD_NAME, (JsonValue)field_name);
	  s.add(JSON_ATTR_VALUE_NAME, val);
	  s.add(JSON_ATTR_TYPE_NAME, (JsonValue)(new JsonString(type_str)));
	  result.addCopy(s);	
  }
  
  /**
   * apply a post-processing phase over the value 'v' and then insert it in the output.
   */
  private boolean post_processing(JsonValue v) throws Exception
  {
	  BufferedJsonRecord rec = (BufferedJsonRecord)v;
	  JsonString attr_name = (JsonString)rec.get(JSON_ATTR_FIELD_NAME);
	  JsonValue attr_value = rec.get(JSON_ATTR_VALUE_NAME);
	  if (attr_value == null)
		  return true;
	  
	  FieldPath fp = FieldPath.fromJsonString(attr_name);
	  if ((fp.getLeafType() != Type.ATOM) && (fp.getLeafType() != Type.ATOM_ARRAY))
	  {
		  add_to_output(attr_name, attr_value.getType(), JSON_VAL_NOT_APPLICABLE);
		  return true;
	  }
	  
	  if (!(attr_value instanceof JsonArray))
	  {
		  add_to_output(attr_name, attr_value.getType(), attr_value);
	  }
	  else
	  {
		  JsonArray r = (JsonArray)attr_value;
		  for (int i = 0;  i < r.count(); i++)
		  {
			  JsonValue element = r.get(i);
			  if (element == null)
				  continue;
			  add_to_output(attr_name, element.getType(), element);
		  }
	  }
	  return true;
  }

  
  /**
   * Given a record 'data' and a set of root-to-leaf paths, compute the value of each paths. 
   */
  public boolean path_values(JsonRecord data, JsonArray paths) throws Exception
  {
	  String field_name = "", query_str = "";
	  String attr = "", output = "";		
	  int len = (int) paths.count();
	
	  query_str = "$rec = [" +  data.toString() + "];";
	  for (int i = 0; i < len; i++)
	  {
		  field_name = paths.get(i).toString();
		  FieldPath fp = FieldPath.fromString(field_name);
		  
		  String attr_var = "$r" +  String.valueOf(i);  
		  attr = attr_var + " = $rec " + fp.toJsonTransformExpr();
		  query_str = query_str + "\n" + attr;
		  if (output.length() > 0)
			  output = output + ", ";
		  output = output + " {" + ATTR_FIELD_NAME + ":'" + field_name + "', " + ATTR_VALUE_NAME + ":" + attr_var +  "}";
	  }
	  query_str = query_str + "\n" + "$output = [" + output + "];";
	  query_str = query_str + "\n" + "$output;";	    
	  //System.out.println(query_str);
	        
	  //Run the query and get the values
	  JaqlQuery jaql = new JaqlQuery();
	  jaql.setQueryString(query_str);
	  JsonIterator it = jaql.iterate(); 
	  while(it.moveNext())
		  post_processing(it.current());

	  jaql.close();
	  return true;
  }

  /**
   * Returns an array of records, each record is in the form of:
   * {
   * 	AttrName: <root-to-leaf path name>,
   * 	AttrValue: <path value (post-processed)>
   * 	AttrType: <Data type of the AttrValue>
   * }
   * 
   * See "src/test/com/ibm/jaql/DataStatistics.txt" for examples.
   */
  public JsonIterator iter(final Context context) throws Exception
  {
	  result.clear();
	  final JsonRecord rec = (JsonRecord) exprs[0].eval(context);
	  if (rec == null) 
	  {
		  return JsonIterator.EMPTY; 
	  }

	  //Get the root-to-leaf paths
	  BufferedJsonArray paths = JsonRecord.paths(rec.iteratorSorted());
	  
	  //Get the value of each root-to-leaf path. This function updates 'result' array.
	  path_values(rec, paths);
	  return (JsonIterator)(result.iterator());
  }
}
