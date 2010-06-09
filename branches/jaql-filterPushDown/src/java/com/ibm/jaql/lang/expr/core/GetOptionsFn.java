package com.ibm.jaql.lang.expr.core;

import java.util.Map;

import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

/** Return the system options record. */
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
  public JsonRecord eval(Context context) throws Exception
  {
    return context.getOptions();
  }
}
