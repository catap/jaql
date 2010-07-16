package com.ibm.jaql.lang.expr.string;

import java.io.StringReader;

import com.ibm.jaql.json.parser.JsonParser;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

public class JsonFn extends Expr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11
  {
    public Descriptor()
    {
      super("json", JsonFn.class);
    }
  }
  
  public JsonFn(Expr ... exprs)
  {
    super(exprs);
  }

  @Override
  public JsonValue eval(Context context) throws Exception
  {
    JsonString in = (JsonString)exprs[0].eval(context);
    if (in == null) return in;
    JsonParser parser = new JsonParser(new StringReader(in.toString()));
    return parser.JsonVal();
  }

  @Override
  public JsonIterator iter(Context context) throws Exception
  {
    JsonString in = (JsonString)exprs[0].eval(context);
    if (in == null) return JsonIterator.NULL;
    JsonParser parser = new JsonParser(new StringReader(in.toString()));
    return parser.arrayIterator();
  }
}
