package com.ibm.jaql.lang.expr.core;

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
import com.ibm.jaql.util.FastPrinter;


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

  public Expr recExpr()
  {
    return exprs[0];
  }
  
  /**
   * 
   */
  @Override
  protected void decompileRaw(FastPrinter exprText, HashSet<Var> capturedVars, boolean emitLocation)
      throws Exception
  {
    exprText.print("(");
    exprs[0].decompile(exprText, capturedVars,emitLocation);
    exprText.print(").*");
  }

  /**
   * Return the mapping table.
   */
  @Override
  public MappingTable getMappingTable()
  {
	  MappingTable mt = new MappingTable();

	  //If the input is a record expression then we can return its mapping, otherwise the mapping is lost.
	  if (exprs[0] instanceof RecordExpr)
		  return exprs[0].getMappingTable();
	  else
		  return mt;
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
