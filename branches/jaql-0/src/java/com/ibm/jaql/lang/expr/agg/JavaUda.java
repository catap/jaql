package com.ibm.jaql.lang.expr.agg;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.type.JsonValue;

/** 
 * Interface for user-defined aggregate functions. 
 * 
 * Implementations are provided with values to aggregate one at a time. Information about the 
 * seen values (as required to compute the aggregate value) can be hold in an arbitrary data 
 * structures, but the UDa provides methods to extract and use a JSON representation of this 
 * partial result.  
 */ 
public interface JavaUda
{
  /** Initialize (or reset) the partial results used by this UDA. 
   * 
   * The provided arguments do not contain the data to aggregate but additional arguments to 
   * parameterize the aggregate function, if needed.
   * 
   * @param args arguments for parameterizing the aggregate
   */
  public void init(JsonValue ... args);

  /** Accumulate the given value, i.e., merge it with the current partial result. The
   * value is fragile, i.e., must not be buffered by an implementation. */ 
  public void accumulate(JsonValue value);

  /** Produce a JSON representation of the partial result. */ 
  public JsonValue getPartial();
  
  /** Returns the schema of the value returned by {@link #getPartial()}. */
  public Schema getPartialSchema();
  
  /** Merge the specified partial result with the partial result hold by this aggregate. 
   * <code>partial</code> is fragile, i.e., must not be buffered by an implementation. */
  public void combine(JsonValue partial);
  
  /** Return the final aggregate value. */
  public JsonValue getFinal();
  
  /** Returns the schema of the value returned by {@link #getFinal()}. */
  public Schema getFinalSchema();
}
