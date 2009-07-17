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
package com.ibm.jaql.lang.rewrite;

import java.util.ArrayList;

import com.ibm.jaql.lang.core.Env;
import com.ibm.jaql.lang.core.VarMap;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.top.AssignExpr;
import com.ibm.jaql.lang.expr.top.QueryExpr;
import com.ibm.jaql.lang.walk.ExprFlow;
import com.ibm.jaql.lang.walk.ExprWalker;
import com.ibm.jaql.lang.walk.OneExprWalker;
import com.ibm.jaql.lang.walk.PostOrderExprWalker;

/**
 * 
 */
public class RewriteEngine
{
  protected int            phaseId     = 0;
  protected RewritePhase[] phases      = new RewritePhase[9];
  protected boolean        traceFire   = false;
  protected boolean        explainFire = false;                    // traceFire must true for this to matter
  protected long           counter     = 0;

  // These are for use by rewrites.
  public Env               env;
  public ExprWalker        walker      = new PostOrderExprWalker();
  public ExprFlow          flow        = new ExprFlow();
  public VarMap            varMap      = new VarMap();
  public ArrayList<Expr>   exprList    = new ArrayList<Expr>();

  /**
   * 
   */
  public RewriteEngine()
  {
    // TODO: add command line way to do this, add way to do remotely
    String onFire = System.getProperty("jaql.rewrite.onFire");
    if (onFire != null)
    {
      if ("trace".equals(onFire))
      {
        traceFire = true;
      }
      else if ("explain".equals(onFire))
      {
        traceFire = explainFire = true;
      }
    }

    ExprWalker postOrderWalker = new PostOrderExprWalker();
    ExprWalker rootWalker = new OneExprWalker();

    RewritePhase basicPhase = new RewritePhase(this, postOrderWalker, 10000);
    RewritePhase phase = phases[phaseId] = basicPhase;
    new LetInline(phase);
    new DoMerge(phase);
    // new DechainFor(phase);
    new FunctionInline(phase);
    new TrivialForElimination(phase);
    new TrivialTransformElimination(phase);
    new TransformMerge(phase);
    new ForToLet(phase);
    new AsArrayElimination(phase);
    // new GlobalInline(phase);
    new DoInlinePragma(phase);
    new ConstArrayAccess(phase);
    new ConstFieldAccess(phase);
    new ForInSimpleIf(phase);
    new SimplifyFirstNonNull(phase);
    new TrivialCombineElmination(phase);
    new CombineInputSimplification(phase);
    new DoConstPragma(phase);
    new PathArrayToFor(phase);
    new PathIndexToFn(phase);
    new ToArrayElimination(phase);
    new EmptyOnNullElimination(phase);
    new InjectAggregate(phase);
    new UnnestFor(phase);
    new WriteAssignment(phase);
    //    new ConstArray(phase);
    //    new ConstRecord(phase);

    phase = phases[++phaseId] = new RewritePhase(this, postOrderWalker, 10000);
    // new GroupToMapReduce(phase);
    new JoinToCogroup(phase);
    // new CogroupToMapReduce(phase);
    // new ForToMapReduce(phase);

    phase = phases[++phaseId] = new RewritePhase(this, postOrderWalker, 0);
    new ConstEval(phase); // TODO: run bottom-up/post-order
    // new ConstFunction(phase);

    phase = phases[++phaseId] = new RewritePhase(this, postOrderWalker, 10000);
    new StrengthReduction(phase);
    
    phase = phases[++phaseId] = new RewritePhase(this, rootWalker, 1);
    new ToMapReduce(phase);

    phase = phases[++phaseId] = new RewritePhase(this, postOrderWalker, 10000);
    new GroupElimination(phase);
    new PerPartitionElimination(phase);
    
    phases[++phaseId] = basicPhase;
    
    phase = phases[++phaseId] = new RewritePhase(this, postOrderWalker, 0);
    new StrengthReduction(phase);


    // This phase is REQUIRED to run to completion if the expr has been
    // modified in a way that a variable could be hidden (ie, just about any rewrite)
    // FIXME: Make sure this phase is run to completion. Probably when we wrap up the parse & rewrite into one class. 
    phase = phases[++phaseId] = new RewritePhase(this, postOrderWalker,
        10000000);
    new RebindVars(phase);
  }

  /**
   * @param env
   * @param query
   * @throws Exception
   */
  public Expr run(Env env, Expr query) throws Exception
  {
//    if (1==1) return query;
    
    // We don't rewrite def expressions until they are actually evaluated.
    if (query instanceof AssignExpr)
    {
      return query;
    }
    Expr dummy = new QueryExpr(query);
    this.env = env;
    counter = 0;
    for (RewritePhase phase : phases)
    {
      phase.run(dummy);
    }
    return dummy.child(0);
  }

  /**
   * @return a unique number for this run of the engine
   */
  public long counter()
  {
    long n = counter;
    counter++;
    return n;
  }
}
