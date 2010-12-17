package com.ibm.jaql.lang.expr.core;

import java.util.EnumMap;
import java.util.Map;

/** Properties of expressions. Also contains some factory methods. */
public enum ExprProperty 
{
  ALLOW_COMPILE_TIME_COMPUTATION(Distribution.ALL_DESCENDANTS), // in practice only done when the 4 below are false    
  HAS_SIDE_EFFECTS(Distribution.AT_LEAST_ONE_DESCENDANT),
  IS_NONDETERMINISTIC(Distribution.AT_LEAST_ONE_DESCENDANT),
  READS_EXTERNAL_DATA(Distribution.AT_LEAST_ONE_DESCENDANT),
  HAS_CAPTURES(Distribution.AT_LEAST_ONE_DESCENDANT);
  
  public enum Distribution
  {
    ALL_DESCENDANTS,
    AT_LEAST_ONE_DESCENDANT,
    SHALLOW
  }
  private Distribution distribution;
  
  private ExprProperty(Distribution distribution)
  {
    this.distribution = distribution;
  }
  
  /** Describes how this property spreads over the expression tree. <code>true<code> means that
   * a property hold for a subtree if it holds for the root and all descendants. <code>false<code> 
   * means that a property is true for a subtree if it holds for the root or at least one of its 
   * descendants (or both). */
  public Distribution getDistribution()
  {
    return distribution;
  }
  
  public static Map<ExprProperty, Boolean> createSafeDefaults()
  {
    return new EnumMap<ExprProperty, Boolean>(ExprProperty.class); // all unknown
  }
  
  public static Map<ExprProperty, Boolean> createUnsafeDefaults()
  {
    // DO NOT CHANGE
    Map<ExprProperty, Boolean> result = new EnumMap<ExprProperty, Boolean>(ExprProperty.class);
    // ALLOW_COMPILE_TIME_COMPUTATION not set; we want to make this explicit where desired
    result.put(HAS_SIDE_EFFECTS, false);
    result.put(IS_NONDETERMINISTIC, false);
    result.put(READS_EXTERNAL_DATA, false);
    result.put(HAS_CAPTURES, false);
    return result;
  }
}
