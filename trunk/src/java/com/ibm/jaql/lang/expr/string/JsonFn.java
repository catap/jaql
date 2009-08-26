package com.ibm.jaql.lang.expr.string;

import java.io.StringReader;

import com.ibm.jaql.json.parser.JsonParser;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

@JaqlFn(fnName = "json", minArgs=1, maxArgs=1)
public class JsonFn extends Expr
{
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
}
