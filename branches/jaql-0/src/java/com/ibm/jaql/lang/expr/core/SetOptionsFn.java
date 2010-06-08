package com.ibm.jaql.lang.expr.core;

import java.util.Map;

import com.ibm.jaql.json.type.JsonBool;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

/** Set system options. */
public class SetOptionsFn extends Expr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11
  {
    public Descriptor()
    {
      super("setOptions", SetOptionsFn.class);
    }
  }

  public SetOptionsFn(Expr... exprs)
  {
    super(exprs);
  }
  
  @Override
  public Map<ExprProperty, Boolean> getProperties()
  {
    Map<ExprProperty, Boolean> result = super.getProperties();
    result.put(ExprProperty.HAS_SIDE_EFFECTS, true);
    return result;
  }

  @Override
  public JsonValue eval(Context context) throws Exception
  {
    JsonRecord rec = (JsonRecord)exprs[0].eval(context);
    context.setOptions(rec);
    return JsonBool.TRUE;
  }

}
