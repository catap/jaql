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
import com.ibm.jaql.lang.expr.agg.Aggregate;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.top.AssignExpr;
import com.ibm.jaql.lang.walk.ExprFlow;
import com.ibm.jaql.lang.walk.ExprWalker;
import com.ibm.jaql.lang.walk.PostOrderExprWalker;

/**
 * 
 */
public class RewriteEngine
{
  protected int            phaseId     = 0;
  protected RewritePhase[] phases      = new RewritePhase[6];
  protected boolean        traceFire   = false;
  protected boolean        explainFire = false;                    // traceFire must true for this to matter
  protected long           counter     = 0;

  // These are work areas for use by rewrites.
  public Env               env;
  public ExprWalker        walker      = new PostOrderExprWalker();
  public ExprFlow          flow        = new ExprFlow();
  public VarMap            varMap      = new VarMap();
  public ArrayList<Expr>   exprList    = new ArrayList<Expr>();
  public ArrayList<Aggregate> aggList  = new ArrayList<Aggregate>();

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
    // ExprWalker rootWalker = new OneExprWalker();

    RewritePhase basicPhase = new RewritePhase(this, postOrderWalker, 10000);
    RewritePhase phase = phases[phaseId] = basicPhase;
    new LetInline(phase);
    new DoMerge(phase);
    new DoPullup(phase);
    new FunctionInline(phase);
    new TrivialForElimination(phase);
    new TrivialTransformElimination(phase);
    new AsArrayElimination(phase);
    new DoConstPragma(phase);
    new UnrollTransformLoop(phase);
    new SimplifyRecord(phase);
    new CheapConstEval(phase);
    new ConstIfElimination(phase);
    new FilterPredicateSimplification(phase);
    new FilterPushDown(phase);
    new FilterMerge(phase);
    new OuterJoinToInner(phase);
    
    phase = phases[++phaseId] = new RewritePhase(this, postOrderWalker, 10000);
    new LetInline(phase);
    new DoMerge(phase);
    new DoPullup(phase);
    // new DechainFor(phase);
    new FunctionInline(phase);
    new DaisyChainInline(phase);
    new TrivialForElimination(phase);
    new TrivialTransformElimination(phase);
    new TransformMerge(phase);
    new ForToLet(phase);
    new AsArrayElimination(phase);
    // new GlobalInline(phase);
    new DoInlinePragma(phase);
    new ConstArrayAccess(phase); // FIXME: merge with RewriteFirstPathStep, remove fn
    new ConstFieldAccess(phase); // FIXME: merge with RewriteFirstPathStep, remove fn
    new ForInSimpleIf(phase);
    new SimplifyFirstNonNull(phase);
    new TrivialCombineElimination(phase);
    new CombineInputSimplification(phase);
    new DoConstPragma(phase);
    new RewriteFirstPathStep(phase);
    new PathArrayToFor(phase); // FIXME: merge with RewriteFirstPathStep
    new PathIndexToFn(phase); // FIXME: merge with RewriteFirstPathStep, remove PathIndexToFn + fn, remove ConstFieldAccess, et al
    new ToArrayElimination(phase);
    new EmptyOnNullElimination(phase);
    new InjectAggregate(phase);
    new UnrollForLoop(phase);
    new UnrollTransformLoop(phase);
    new SimplifyUnion(phase);
    new VarProjection(phase);
    new SimplifyRecord(phase);
    new UnnestFor(phase);
    new WriteAssignment(phase);
    new TypeCheckSimplification(phase);
    // new ConstEval(phase); // TODO: do we need full ConstEval? Should it be in this or another phase?  Can it be made quicker?
    new CheapConstEval(phase);
    new ConstIfElimination(phase);
    //    new StrengthReduction(phase);
    //    new ConstArray(phase);
    //    new ConstRecord(phase);

    phase = phases[++phaseId] = new RewritePhase(this, postOrderWalker, 10000);
    // new GroupToMapReduce(phase);
    new JoinToCogroup(phase);
    // new CogroupToMapReduce(phase);
    // new ForToMapReduce(phase);

//    // TODO: put ConstEval in basicPhase? it is somewhat expensive because it 
//    // tested on every expr and the test can walk a lot of the tree...
//    phase = phases[++phaseId] = new RewritePhase(this, postOrderWalker, 1000);
//    new ConstEval(phase); // TODO: run bottom-up/post-order
//    new LetInline(phase); // ConstEval opens more LetInline chances, which opens more ConstEval   
//    //new ConstFunction(phase);

    // phase = phases[++phaseId] = new RewritePhase(this, rootWalker, 1);
    phase = phases[++phaseId] = new RewritePhase(this, postOrderWalker, 1000);
    new ToMapReduce(phase);
    new WriteAssignment(phase);
    new LetInline(phase);
    new DoMerge(phase);
    new DoPullup(phase);

    phase = phases[++phaseId] = new RewritePhase(this, postOrderWalker, 10000);
    new GroupElimination(phase);
    new PerPartitionElimination(phase);
    new PragmaElimination(phase);
    
    phases[++phaseId] = phases[1];
  }

  /**
   * @param env
   * @param query
   * @throws Exception
   */
  public Expr run(Expr query) throws Exception
  {
//    if (1==1) return query;
    
    // We don't rewrite def expressions until they are actually evaluated.
    // FIXME: rewrites of MaterializeExpr inlines functions; disable those inlines
//    if (query instanceof AssignExpr || query instanceof MaterializeExpr)
    if (query instanceof AssignExpr )
    {
      return query;
    }
    if (query.getEnvExpr() == null)
    {
      throw new IllegalArgumentException("expression tree does not have an EnvExpr");
    }
    this.env = query.getEnvExpr().getEnv();
    if (env == null)
    {
      throw new IllegalArgumentException("expression tree does not have an environment");
    }
    counter = 0;
    for (RewritePhase phase : phases)
    {
      phase.run(query);
    }
    return query;
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
