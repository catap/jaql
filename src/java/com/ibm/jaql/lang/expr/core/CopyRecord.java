package com.ibm.jaql.lang.expr.core;

import java.io.PrintStream;
import java.util.HashSet;
import java.util.Map.Entry;

import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.metadata.MappingTable;
import com.ibm.jaql.util.Bool3;


// $.*
public class CopyRecord extends FieldExpr
{

  /**
   * 
   * @param exprs
   */
  public CopyRecord(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * 
   * @param recExpr
   * @param nameExpr
   */
  public CopyRecord(Expr recExpr)
  {
    super(recExpr);
  }

  @Override
  public Bool3 evaluatesChildOnce(int i)
  {
    return Bool3.TRUE;
  }
  
  /**
   * 
   */
  @Override
  public void decompile(PrintStream exprText, HashSet<Var> capturedVars)
      throws Exception
  {
    exprText.print("(");
    exprs[0].decompile(exprText, capturedVars);
    exprText.print(").*");
  }

  /**
   * 
   */
  @Override
  public void eval(Context context, BufferedJsonRecord outrec) throws Exception
  {
    JsonRecord inrec = (JsonRecord) exprs[0].eval(context);
    if (inrec != null)
    {
      for (Entry<JsonString, JsonValue> e : inrec)
      {
        outrec.add(e.getKey(), e.getValue());
      }
    }
  }

  /**
   * 
   */
  @Override
  public Bool3 staticNameMatches(JsonString name)
  {
    return Bool3.UNKNOWN;
  }
}
