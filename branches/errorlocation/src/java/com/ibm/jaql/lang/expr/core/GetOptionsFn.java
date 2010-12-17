package com.ibm.jaql.lang.expr.core;

import java.util.Map;

import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

/**
 * @jaqlDescription Return Jaql's options as a record
 * Usage:
 * 
 * { *: any, *} getOptions();
 * 
 * Jaql maintains globally accessible options, e.g., name-value pairs.
 * These options are represented as a record; the getOptions function
 * returns these options. Note that if you set the field "conf" with
 * a record, those options are overlaid onto Hadoop's JobConf when a 
 * MapReduce job is run. Using getOptions and setOptions, one can 
 * override settings in the default JobConf.
 * 
 * @jaqlExample getOptions();
 * {
 *   "conf": {
 *     "io.sort.factor": 20
 *   }
 * }
 */
public class GetOptionsFn extends Expr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par00
  {
    public Descriptor()
    {
      super("getOptions", GetOptionsFn.class);
    }
  }

  public GetOptionsFn(Expr... exprs)
  {
    super(exprs);
  }
  
  public GetOptionsFn()
  {
    super();
  }

  @Override
  public Map<ExprProperty, Boolean> getProperties()
  {
    Map<ExprProperty, Boolean> result = super.getProperties();
    result.put(ExprProperty.READS_EXTERNAL_DATA, true);
    return result;
  }

  @Override
  protected JsonRecord evalRaw(Context context) throws Exception
  {
    return context.getOptions();
  }
}
