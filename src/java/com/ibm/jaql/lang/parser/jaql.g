/*
 * Copyright (C) IBM Corp. 2009.
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
 
header {
package com.ibm.jaql.lang.parser;

import java.util.*;

import com.ibm.jaql.lang.core.*;
import com.ibm.jaql.lang.expr.core.*;
import com.ibm.jaql.lang.expr.top.*;
import com.ibm.jaql.lang.expr.path.*;

import com.ibm.jaql.lang.expr.io.*;
import com.ibm.jaql.lang.expr.udf.*;
import com.ibm.jaql.lang.expr.record.IsdefinedExpr;
import com.ibm.jaql.lang.expr.array.ExistsFn;
import com.ibm.jaql.lang.expr.nil.IsnullExpr;
import com.ibm.jaql.lang.expr.agg.Aggregate;

import com.ibm.jaql.json.type.*;
import com.ibm.jaql.json.schema.*;
import com.ibm.jaql.json.util.*;
import com.ibm.jaql.lang.registry.*;

import com.ibm.jaql.util.*;

}

options {
  language="Java";
}



class JaqlParser extends Parser;
options {
    k = 1;                           // number of tokens to lookahead
	exportVocab=Jaql;               // Call its vocabulary "Jaql"
    defaultErrorHandler = false;     // Don't generate parser error handlers
}

{
	public boolean done = false;
	public Env env = new Env();
    // static abstract class ExpandMapper { public abstract Expr remap(Expr ctx); } 

    public void oops(String msg) throws RecognitionException, TokenStreamException
    { 
      throw new RecognitionException(msg, getFilename(), LT(1).getColumn(), LT(1).getLine()); 
    }
}

parse returns [Expr r=null]
    : r=stmtOrFn
    | EOF { done = true; }
	;
	

stmtOrFn returns [Expr r=null]
	: r=stmt (SEMI|EOF)
	| SEMI
	| r=functionDef
	;
	
stmt returns [Expr r=null]
    { String v; }
    : r=topAssign
    | "explain" r=topAssign  { r = new ExplainExpr(r); }
    | "materialize" v=var    { r = new MaterializeExpr(env.inscope(v)); }
    | "quit"                 { done = true; }
    ;

//assign2 returns [Expr r=null]
//   // { String v; }
//    : r=pipe  // TODO: keep this?
//        {
//          if( r instanceof BindingExpr )
//          {
//            BindingExpr b = (BindingExpr)r; 
//            r = new AssignExpr( env.sessionEnv().scopeGlobal(b.var.name),  b.inExpr());
//          }

//          BindingExpr b = (BindingExpr)r; 
//          if( "$".equals(b.var.name) )
//          {
//          	r = b.inExpr();
//          }
//          else
//          {
//          	r = new AssignExpr( env.sessionEnv().scopeGlobal(b.var.name),  b.inExpr());
//          }
//        }
//      ( "=>" v=var { r = new AssignExpr( env.sessionEnv().scopeGlobal(v), r); } // TODO: var.type = pipe, do-block scope
//      )?
//    ;

block returns [Expr r=null] 
    { ArrayList<Expr> es=null; }
    : r=optAssign  //TODO: ("=>" var ";" block)? ???
      ("," { es = new ArrayList<Expr>(); es.add(r); }
         r=optAssign       { es.add(r); }
        ("," r=optAssign   { es.add(r); } )*
        { 
        	for(Expr e: es)
        	{
        		if( e instanceof BindingExpr )
        		{
        			env.unscope(((BindingExpr)e).var);
        		}
        	}
        	r = new DoExpr(es);
        	// r = new ParallelDoExpr(es); // TODO: parallel
        }
      )?
    ;


pipe returns [Expr r=null]
    { String n = "$"; } // TODO: should var source define name? eg, $x -> filter $x.y > 3
    : r=expr ( "->" r=op[r] )*
    | r=pipeFn
    ;
    
  
subpipe[Var inVar] returns [Expr r=null]
    {
    	inVar.hidden = true;
//        r=new PipeInput(new VarExpr(inVar));
        r= new VarExpr(inVar);
    }
    : ( "->" r=op[r] )+
    ;

pipeFn returns [Expr r=null]
    {
        Var v = env.makeVar("$");
    }
    : r=subpipe[v]
    {
        ArrayList<Var> p = new ArrayList<Var>();
        p.add(v);
        r = new DefineFunctionExpr(null, p, r); // TODO: use a diff class
    }
    ;

    
vpipe returns [BindingExpr r=null]
    { String v; Expr e=null; }
    : v=var ( /*empty*/   { e = new VarExpr(env.inscope(v)); }
            | "in" e=expr )
    { r = new BindingExpr(BindingExpr.Type.IN, env.scope(v), null, e); }
    ;

// A pipe must end with a binding, with the name still in scope
//vpipe returns [BindingExpr r=null]
//    : v=var ( /*empty*/   { e = new VarExpr(env.inscope(v)); }
//      {
//      	if( !( e instanceof BindingExpr ) )
//      	{
//      	  throw new NoViableAltException(LT(1), getFilename()); // binding required in this context
//      	}
//      	r = (BindingExpr)e;
//      }
//    ;


//vpipe returns [BindingExpr r=null]
//    { String v=null; Expr e; }
//    : v=var ( /*empty*/       { e=makeVarExpr(v); }
//            | "in" e=inPipe )
//    {
//      Var var = env.scope(v);
//      r = new BindingExpr(BindingExpr.Type.IN, var, null, e);
//    }
//    ;
    
//subPipe[Var v] returns [Expr r=null]
//    { String vn = "$"; r = new PipeInput(new VarExpr(v)); }
//    : ("->" r=op[vn, r] ("as" vn=var)?)+ 
//    ;

//outPipe[Var v] returns [Expr r=null]
//    { String vn = "$"; r = new VarExpr(v); }
//    : "->" (r=op[r] "->")* r=sink[r]
//    : ("->" r=op[vn,r] )+
//    ;



//fnOp returns [Expr r=null]
//    { String s=null; ArrayList<Expr> args; }  
//    : s=name "(" args=exprList ")" 
//      { r = FunctionLib.lookup(env, s, args);  }
//    ;

aggregate[Expr in] returns [Expr r=null]
    { BindingExpr b; }
    : ("aggregate" | "agg") b=each[in] r=expr
       { r = AggregateExpr.make(env, b.var, b.inExpr(), r); } // TODO: take binding!
    ;

group returns [Expr r=null]
	{ 
	  BindingExpr in;
	  BindingExpr by = null;
	  Expr c=null;
	  String v = "$";
	  ArrayList<Var> as = new ArrayList<Var>();
	}
	: "group" ("each" v=var "in")? 
	    { 
	      in=new BindingExpr(BindingExpr.Type.IN, env.makeVar(v), null, Expr.NO_EXPRS); 
	    }
	  by=groupIn[in,by,as] ( "," by=groupIn[in,by,as] )*
		{ if( by.var != Var.unused ) env.scope(by.var); } 
	  ( "using" c=comparator { oops("comparators on group by NYI"); } )?
        {
          for( Var av: as )
          {
            env.scope(av);
          }
	    }
	  r=groupReturn
	    {
	      if( by.var != Var.unused ) env.unscope(by.var);
	      for( Var av: as )
          {
            env.scope(av);
          }
          r = new GroupByExpr(in, by, as, c, r); // .expand(env);
	    }
	;

groupIn[BindingExpr in, BindingExpr prevBy, ArrayList<Var> asVars] returns [BindingExpr by]
    { Expr e; String v=null; }
	: e=expr { env.scope(in.var); } by=groupBy[prevBy] ( "as" v=var )?    
	    {
          if( v == null )
          {
            if( e instanceof VarExpr )
            {
              v = ((VarExpr)e).var().name;
            }
            else
            {
              oops("\"as\" variable required when grouping expression is not a simple variable");
            }
          }
          for( Var as: asVars ) 
          {
            if( as.name.equals(v) )
            {
              oops("duplicate group \"as\" variable: "+v);
            }
          }
	      in.addChild(e); 
          env.unscope(in.var); // TODO: unscope or hide?
          asVars.add(env.makeVar(v));
        }
	;

groupBy[BindingExpr by] returns [BindingExpr b=null]
    { String v = null; Expr e=null; }
    : ( "by" (v=avar "=")? e=expr )?
    {
      if( e == null )
      {
        e = new ConstExpr(Item.nil);
      }
      if( by == null )
      {
        Var var;
        if( v == null )
        {
            var = Var.unused;
        }
        else
        {
            var = env.makeVar(v);
        }
        b = new BindingExpr(BindingExpr.Type.EQ, var, null, e);
      }
      else if( v == null || (by.var != Var.unused && by.var.name.equals(v)) )
      {
        by.addChild(e);
        b = by;
      }
      else
      {
        oops("all group by variables must have the same name:" +by.var.name+" != "+v);
      }
    }
    ;

groupReturn returns [Expr r=null]
    : "into" r=expr    { r = new ArrayExpr(r); }
    | "expand" r=expr
    //| r=aggregate[new VarExpr(pipeVar)] 
    //    { if(numInputs != 1) throw new RuntimeException("cannot use aggregate with cogroup"); } 
    ;

//group returns [Expr r=null]
//    {
//      ArrayList<BindingExpr> inputs = new ArrayList<BindingExpr>();
//      ArrayList<Expr> keys = new ArrayList<Expr>();
//      ArrayList<Expr> intos = new ArrayList<Expr>();
//      inputs.add(null); // for the by expressions
//    }
//    : "group" "(" groupIn[inputs, keys, intos] ( "," groupIn[inputs, keys, intos] )* ")" 
//      {
//        int n = keys.size();
//      	Var keyVar = env.makeVar("$key");
//        Expr[] args = new Expr[n];
//        inputs.set(0, new BindingExpr(BindingExpr.Type.EQ, keyVar, null, keys.toArray(args)));
//        args = new Expr[n+1];
//        args[0] = new ProjPattern(new VarExpr(keyVar),null,true);
//        for(int i = 0 ; i < n ; i++)
//        {
//          Var aggVar = env.makeVar("$av");
//          Var inVar = inputs.get(i+1).var;
//          Expr e = intos.get(i);
//          e.replaceVar(inVar, aggVar);
//          e = AggregateExpr.make(env, aggVar, new VarExpr(inVar), e);
//          e = new IndexExpr(e,0);
//          e = new ProjPattern(e,null,true); // TODO: eliminate record assumption
//          args[i+1] = e;
//        }
//        r = new RecordExpr(args);
//        r = new ArrayExpr(r); 
//        r = new GroupByExpr(inputs, r); // .expand(env);
//      }
//    ;
//
//groupIn[ArrayList<BindingExpr> inputs, ArrayList<Expr> keys, ArrayList<Expr> intos]
//    { Expr f=null; String vn="$"; Var v; Expr e; Expr key=null; Expr r=null; }
//    // TODO: verify that all keys and into are compatible (all recs/arrays)
//    // TODO: verify that all keys have same fields??
//    // TODO: make comparators another class of special functions (ignored outside of sorting/grouping)?
//    : (f=simpleField)?
//      (vn=var ( /*empty*/      { e = new VarExpr(env.inscope(vn)); }
//              | "in" e=expr))  { v=env.scope(vn); } 
//      ("by" key=cmp)?
//      ("into" r=expr)?
//    {
//       env.unscope(v);
//       if( key == null )
//       {
//         key = new ConstExpr(Item.nil);
//       }
//       if( r == null )
//       {
//       	 r = new ArrayAgg(new VarExpr(v));
//       	 if( f == null )
//       	 {
//       	   r = new ArrayExpr(r);
//       	 }
//       	 else
//         {
//           r = new RecordExpr(new NameValueBinding(f, r, false));
//         }
//       }
//       else if( f != null )
//       {
//       	 // TODO: just ignore this?
//       	 throw new RecognitionException("warning: field label cannot be used with 'into' clause: "+f,
//       	    getFilename(), LT(1).getColumn(), LT(1).getLine());
//       }
//       
//       BindingExpr in = new BindingExpr(BindingExpr.Type.IN, v, null, e);
//       inputs.add(in);
//       keys.add(key);
//       intos.add(r);
//     }
//   ;


groupPipe[Expr in] returns [Expr r=null]
    { 
      BindingExpr b; BindingExpr by=null; Expr key=null;  Expr c; 
      String v="$"; Var asVar; 
    }
    : "group" b=each[in] 
      by=groupBy[null]       { env.unscope(b.var); if( by.var != Var.unused ) env.scope(by.var); }
      ( "as" v=var )?        { asVar=env.scope(v); }
      ( "using" c=comparator { oops("comparators on group by NYI"); } )?
      r=groupReturn
        {
          if( by.var != Var.unused ) env.unscope(by.var);
          env.unscope(asVar);
          r = new GroupByExpr(b,by,asVar,null,r);
        }
    ;

comparator returns [Expr r=null]
    { String s; Expr e; }
    : r=cmpArrayFn["$"]
    //  | "default"      { r = new DefaultComparatorExpr(); }
    | (s=name|s=str)     { oops("named comparators NYI: "+s); } // r=new InvokeBuiltinCmp(ctx, e);
    | r=cmpFnExpr
    | r=varExpr
    | r=parenExpr 
    ;

cmpExpr returns [Expr r=null]
    { String v; Var var; }
    : "cmp" ( "(" v=var ")" r=cmpArrayFn[v]
            | r=cmpArray )
    ;

cmpFnExpr returns [Expr r=null]
    { String v; Var var; }
    : "cmp" "(" v=var ")" r=cmpArrayFn[v]
    ;

cmpArrayFn[String vn] returns [Expr r=null]
    { Var var=env.scope(vn); }
    : r=cmpArray
    {
      env.unscope(var);
      r = new DefineFunctionExpr(null, new Var[]{var}, r); // TODO: DefineCmpFn()? Add Cmp type?
    }
    ;

cmpArray returns [CmpArray r=null]
    { Var var; ArrayList<CmpSpec> keys = new ArrayList<CmpSpec>(); }
    : "[" ( cmpSpec[keys] ("," (cmpSpec[keys])? )* )? "]"
    {
      r = new CmpArray(keys);
    }
    ;
cmpSpec[ArrayList<CmpSpec> keys]
    { Expr e; Expr c=null; CmpSpec.Order o = CmpSpec.Order.ASC; }
    : e=expr 
      ( "using" c=comparator { oops("nested comparators NYI"); } )? 
      ( "asc" | "desc" { o = CmpSpec.Order.DESC; } )?
    {
      keys.add( new CmpSpec(e, o) );
    }
    ;
    

//defBinding[String name] returns [BindingExpr b]
//    { Expr e; }
//    : e=pipe
//    {
//    	if( e instanceof BindingExpr )
//    	{
//    		b=(BindingExpr)e;
//    	}
//    	else
//    	{
//    		b = new BindingExpr(BindingExpr.Type.IN, env.scope(name), null, e);
//    	} 
//    }
//    ;
    
//groupBy[BindingExpr by] returns [BindingExpr b=null]
//    { String n = "$key"; Expr e=null; }
//    : ("by" b=defBinding[n])? // TODO: start at simple expr
////    : ("by" (bv=avar "=")? e=expr)?
//    {
//      if( b == null )
//      {
//        b = new BindingExpr(BindingExpr.Type.IN, env.scope(n), null, new ConstExpr(Item.nil));
//      }
//      if( by == null )
//      {
//        by = b;
//      }
//      else if( by.var.name.equals(b.var.name) )
//      {
//        by.addChild(b.inExpr());
//      }
//      else
//      {
//        throw new RuntimeException("all group by variables must have the same name:" +by.var.name+" != "+b.var.name);
//      }
////      if( e == null )
////      {
////          e = new ConstExpr(Item.nil);
////      }
//      b.type = BindingExpr.Type.EQ;
//      by.addChild(b);
//    }
//    ;
 

//groupReturn[ArrayList<BindingExpr> in] returns [Expr r=null]
//    : "into" r=expr // { r = new ArrayExpr(r); } 
//    : "into" r=subpipe[null] // "into" r=expr { r = new ArrayExpr(r); } 
//    | /* empty */
//        {
//          Expr[] fields = new Expr[in.size()];
//          for(int i = 0 ; i < in.size() ; i++ )
//          {
//            Var var = i == 0 ? in.get(i).var : in.get(i).var2;
//            env.unscope(var);
//            fields[i] = new NameValueBinding(var, false);
//          }
//          r = new ArrayExpr(new RecordExpr(fields));
//        }
//    ;

//partition[Expr in] returns [Expr r=null]
//    { BindingExpr b; Var by = null; Expr e = null; String bn = "$key"; }
//    : "partition" b=each[in]
//          "by" ( "(" ( bn=avar "=" )? e=expr ")" // TODO: allow a list and comparators
//               | "default" { e = new DefaultExpr(); } ) // use input/arbitrary partitioning 
//                           { by = env.scope(bn); }
//          "into" "(" r=subpipe[b.var] ")"
//          // | r=aggregate[in.var,new PipeInput(new VarExpr(in.var))]
//          // | r=window[in.var,new PipeInput(new VarExpr(in.var))] )
//      { 
//        env.unscope(by);
//        env.unscope(b.var);
//        r = new PartitionExpr(b.var,b.inExpr(),by,e,r); 
//      }
//    ;


//window[Expr in] returns [Expr r=null]
//    { BindingExpr b; Expr s = null; Expr e=null; String w = "$window"; }
//    //: "tumble" "window" (w=var) windowStart (windowEnd)? r=aggregate[v,new PipeInput(new VarExpr(v))]
//    //| "slide" "window" (w=var) windowStart windowEnd r=aggregate[v,new PipeInput(new VarExpr(v))]
//    : "shift" b=each[in] { b.var.hidden=true; } 
//               s=expr ( "before" e=expr "after" 
//                       | "after"  { e=s; s=null; } )
//             { b.var.hidden=false; } 
//       { env.unscope(b.var); r = new ShiftExpr(v,input,s,e); }
//    ;
    
//windowStart
//    { Expr e; }
//    : "start" windowVars "when" e=expr
//    ;
//
//windowEnd
//    { Expr e; }
//    : "end" windowVars "when" e=expr
//    ;
//
//windowVars
//    { 
//    	String c, a, p, n; 
//    	Var v;
//    }
//    : (c=var) // ("at" a=var)? ("previous" p=var)? ("next" n=var)?
//    ;


join returns [Expr r=null]
    { 
      ArrayList<BindingExpr> in = new ArrayList<BindingExpr>();
      HashMap<String,Var> keys = new HashMap<String,Var>();
      Expr p; 
      BindingExpr b;
    }
    : "join" b=joinIn     { in.add(b); b.var.hidden=true; }
            ("," b=joinIn { in.add(b); b.var.hidden=true; } )+  
      {
        for( BindingExpr b2: in )
        {
          b2.var.hidden = false;
        }
      }
      "where" p=expr
      ( "into" r=expr     { r = new ArrayExpr(r); }
      | "expand" r=expr )
      {
        for( BindingExpr b2: in )
        {
          env.unscope(b2.var);
        }
        r = new MultiJoinExpr(in, p, r).expand(env);  
      }
    ;

joinIn returns [BindingExpr b=null]
    { boolean p = false; }
    : ("preserve" { p = true; })? b=vpipe
      {
        b.preserve = p;
      }
    ;

equijoin returns [Expr r=null]
    { 
        ArrayList<BindingExpr> in = new ArrayList<BindingExpr>(); 
        ArrayList<Expr> on = new ArrayList<Expr>(); 
        Expr c=null; 
    }
    : "equijoin" ejoinIn[in,on] ( "," ejoinIn[in,on] )+ 
      ( "using" c=comparator { oops("comparators on joins are NYI"); } )?
      {
      	for( BindingExpr b: in )
        {
          b.var.hidden = false;
        }
      }
      ( "into" r=expr     { r = new ArrayExpr(r); }
      | "expand" r=expr )
    {
      r = new JoinExpr(in,on,r); // TODO: add comparator
      for( BindingExpr b: in )
      {
      	env.unscope(b.var);
      }
    }
    ;

ejoinIn[ArrayList<BindingExpr> in, ArrayList<Expr> on]
    { Expr e; BindingExpr b; }
    : b=joinIn       { in.add(b); } 
      "on" e=expr    { on.add(e); b.var.hidden=true; }
    ;


//join returns [Expr r=null]
//    { 
//      ArrayList<BindingExpr> in = new ArrayList<BindingExpr>();
//      HashMap<String,Var> keys = new HashMap<String,Var>(); 
//    }
//    : "join" joinIn[keys,in] ("," joinIn[keys,in])+
//        {
//          for(int i = 0 ; i < in.size() ; i++ )
//          {
//            Var var = in.get(i).var;
//            var.hidden = false;
//          }
//        }
//      ( "into" r=expr { r = new ArrayExpr(r); } 
//      | "expand" r=expr //TODO: support join...expand? 
//      )
//      { r = new MultiJoinExpr(in, r).expand(env);  }
//    ;
//
//joinIn[HashMap<String,Var> keys, ArrayList<BindingExpr> bindings]
//	{ 
//		boolean p = false; 
//		BindingExpr b; 
//		ArrayList<BindingExpr> on = new ArrayList<BindingExpr>(); 
//	}
//	: ( "preserve" { p = true; })? b=vpipe "on" "(" joinOn[keys,on] ("," joinOn[keys,on])* ")"  // TODO: define $ = b.var during on
//		{
//	  	  b.var.hidden = true;
//	      b.preserve = p;
//	      b.addChildren(on);
//	      bindings.add( b );
//	    }
//	;
//
//joinOn[HashMap<String,Var> keys, ArrayList<BindingExpr> on]
//    { String k = "$__key__"; Expr e; } // TODO: define $ has current input so $ or $input can be used
//    : (k=avar "=")? e=expr 
//      {
//      	Var var = keys.get(k);
//      	if( var == null )
//      	{
//    	  var = env.scope(k);
//          var.hidden = true;
//          keys.put(k,var);
//      	}
//      	on.add(new BindingExpr(BindingExpr.Type.EQ, var, null, e)); 
//      }
//    ;
	
//joinReturn[ArrayList<BindingExpr> in] returns [Expr r=null]
//    : "into" r=expr { r = new ArrayExpr(r); } 
  //  | /* empty */
//        {
//          Expr[] fields = new Expr[in.size()];
//          for(int i = 0 ; i < in.size() ; i++ )
//          {
//            Var var = in.get(i).var;
//            env.unscope(var);
//            fields[i] = new NameValueBinding(var, false);
//          }
//          r = new ArrayExpr(new RecordExpr(fields));
//        }
//    ;

	
// Introduce uncorrelated expand operator?
// 
//cross returns [Expr r=null]
//    {  BindingExpr b; ArrayList<BindingExpr> in = new ArrayList<BindingExpr>();  }
//    : "cross" "(" b=vpipe   { in.add(b); } 
//         ("," b=vpipe   { in.add(b); } )* ")"
//      {
//        Expr[] fields = new Expr[in.size()];
//        for(int i = 0 ; i < in.size() ; i++ )
//        {
//          Var var = in.get(i).var;
//          env.unscope(var);
//          fields[i] = new NameValueBinding(var, false);
//        }
//        r = new ArrayExpr(new RecordExpr(fields));
//        r = new MultiForExpr(in, null, r).expand(env);
//      }
//    ;


// TODO: keep or remove taggedMerge?
//taggedMerge returns [Expr r=null]
//    { ArrayList<BindingExpr> bs = new ArrayList<BindingExpr>(); }
//    : "taggedMerge" "(" taggedInput[bs] ( "," taggedInput[bs] )* ")" 
//      { r = new TaggedMergeExpr(bs); }
//    ;
//    
//taggedInput[ArrayList<BindingExpr> bs]
//    { BindingExpr b; String v; Expr e; }
//    : b=vpipe 
//      ( "with" v=avar "=" e=expr  // TODO: define $ as alias to pipe name here, and on join, copartition, etc 
//        { b.var2 = env.makeVar(v); b.addChild(e); }
//      )?
//      { env.unscope(b.var); bs.add(b); }
//    ;


//tee[Expr in] returns [Expr r=null]
//    { 
//      ArrayList<Expr> es = new ArrayList<Expr>();
//      Var var = env.makeVar("$tee");
//      es.add( new BindingExpr(BindingExpr.Type.IN, var, null, in) );
//    }
//    : "tee" "("      r=subpipe[var] { es.add(r); }  
//                ("," r=subpipe[var] { es.add(r); } )*
//            ")"
//      {
//     	r = new TeeExpr(es);
//      }
//    ;

split[Expr in] returns [Expr r=null]
    { 
      ArrayList<Expr> es = new ArrayList<Expr>();
      Var tmpVar = env.makeVar("$split");
      Expr p; Expr e;
      BindingExpr b;
    }
//    : "split" b=each[in] "("  { es.add( b ); } 
//         ( "if"  p=expr e=subpipe[b.var]  { es.add(new IfExpr(p,e)); } )*
//         ( "else" e=subpipe[b.var]        { es.add(new IfExpr(new ConstExpr(JBool.trueItem), e) ); } )?
//       ")"
    : "split" b=each[in]     { es.add( b ); } 
         ( "if"              { b.var.hidden=false; } 
              "(" p=expr ")" { b.var.hidden=true; }
                e=expr       { es.add(new IfExpr(p,e)); } )*
         ( "else"            { b.var.hidden=true; } 
                e=expr       { es.add(new IfExpr(new ConstExpr(JBool.trueItem), e) ); } )?
      {
        r = new SplitExpr(es);
        env.unscope(b.var);
      }
    ;


op[Expr in] returns [Expr r=null]
	{ BindingExpr b=null; } 
	: "filter" b=each[in] r=expr     { r = new FilterExpr(b, r);    env.unscope(b.var); }
    | "transform" b=each[in] r=expr  { r = new TransformExpr(b, r); env.unscope(b.var); }
    | "expand"  b=each[in] ( r=expr
                           | /*empty*/ { r = new VarExpr(b.var); } )
                                     { r = new ForExpr(b, r);       env.unscope(b.var); }
    | r=groupPipe[in]
    | r=sort[in]
    | r=top[in]
    //| r=tee[in]
    | r=split[in]
    | r=aggregate[in]
    | r=call[in]
    // | r=partition[in]
    // TODO: add rename path
    // | r=window[in]
	;

each[Expr in] returns [BindingExpr b=null]
    { String v = "$"; }
    : ( "each" v=var )?
    { b = new BindingExpr(BindingExpr.Type.IN, env.scope(v), null, in); }
    ;

//assignOrCall[Expr in] returns [Expr r]
call[Expr in] returns [Expr r=null]
    { String n; ArrayList<Expr> args; }
    : n=var ( // /*empty*/          { r = new AssignExpr( env.sessionEnv().scopeGlobal(n), in); }
              // |
              args=fnArgs        { args.add(0, in); r = new FunctionCallExpr(new VarExpr(env.inscope(n)), args); }
               ( args=fnArgs     { r = new FunctionCallExpr(r, args); } )*
            )
    | n=name args=fnArgs         { args.add(0, in); r = FunctionLib.lookup(env, n, args); }
               ( args=fnArgs     { r = new FunctionCallExpr(r, args); }    )*
    ;

//bindOrCall[BindingExpr in] returns [Expr r]
//    { String n; ArrayList<Expr> args; }
//    : n=var ( /*empty*/          { r = new BindingExpr(BindingExpr.Type.IN, env.scope(n), null, in.inExpr()); }
//            | args=fnArgs        { r = new DoExpr(in, new FunctionCallExpr(new VarExpr(env.inscope(n)), args)); }
//               ( args=fnArgs     { r = new FunctionCallExpr(r, args); } )*
//            )
//    | n=name args=fnArgs         { r = new DoExpr(in, FunctionLib.lookup(env, n, args)); }
//               ( args=fnArgs     { r = new FunctionCallExpr(r, args); }    )*
//    ;
    

// TODO: we might have to merge sink and op and make a semantic check for sinks to handle ops that can also be sinks, eg fnCall, select 
//sink[Expr input] returns [Expr r=null]
//    { String v; }
////    : "write" v=var     { r = new StWriteExpr(new VarExpr(env.inscope(v)), input); }
//    : "write" r=expr     { r = new StWriteExpr(r, input); }
//    // | "return" // TODO: keep this?
//   // | v=var             { r = new AssignExpr( env.sessionEnv().scopeGlobal(v), input); } // TODO: var.type = pipe, do-block scope 
//    // | "discard"           { r = new EatExpr(input); } // TODO: discard
//    // | fnCall
//    ;
    

top[Expr in] returns [Expr r=null]
    { Expr n; Expr by=null; }
    : "top" n=expr (by=sortCmp)?
      {
      	// TODO: add heap-based top operator
      	if( by != null )
      	{
          in = new SortExpr(in, by);
      	}
      	r = new PathExpr(in, new PathArrayHead(new MathExpr(MathExpr.MINUS, n, new ConstExpr(JLong.ONE_ITEM))));
      }
    ;

// TODO: we need temp analysis to break undirected cycles in the flow!!!!
// eg, tee |- $X
//         |- $Y -|
//     cross $X,$Y  // either $X or $Y must be temped
//     merge $X,$Y  // neither needs to be temped if merge is willing to read from either ready input; otherwise, one needs to be temped.  

// TODO: cross operator
//
// TODO: add co-partition; eliminate group by?
// TODO: co-partition produces one tagged, merged stream? or multiple streams?
// TODO: if multiple streams, can easily merge, cross.  need co-aggregate?

//copartition returns [Expr r=null]
//    { 
//      ArrayList<BindingExpr> inputs = new ArrayList<BindingExpr>();
//      Var byVar = null;
//      Var partVar = env.makeVar("$"); // TODO: one part variable or multiple??
//    }
//    : "copartition" copartitionIn[inputs] ("," copartitionIn[inputs])*
//         {
//           byVar = inputs.get(0).var2;
//           byVar.hidden = false;
//           for( BindingExpr b: inputs )
//           {
//          	 // TODO: should copart introduce multiple streams?? 
//          	 // b.var.hidden = false;
//           }
//         }
//      "|-" r=opPipe[partVar] "-|" // r=srcPipe "-|" 
//         {
//           env.unscope( byVar );
//           for( BindingExpr b: inputs )
//           {
//           	 env.unscope(b.var);
//           }
//           r = PartitionExpr.makeCopartition(env, inputs, partVar, r);
//         }
//    ;
//
//copartitionIn[ArrayList<BindingExpr> inputs]
//    { 
//        BindingExpr in;
//        String bn = null; 
//        Expr by;
//    }
//    : in=vpipe "by" (bn=avar "=")? by=expr // TODO: should copartition support "by default"?
//        {  // TODO: define $ = in.var during by
//          Var byVar;
//          if( inputs.size() == 0 )
//          {
//            if( bn == null )
//            {
//              bn = "$key"; // TODO: this should be a constant somewhere
//            }
//            byVar = env.scope(bn);
//            byVar.hidden = true;
//          }
//          else 
//          {
//          	byVar = inputs.get(0).var2;
//          	if( bn != null && ! byVar.name().equals(bn) )
//          	{
//          	  throw new RuntimeException("must use same name for all key expressions: "+bn+" != "+byVar.name());
//          	}
//          }
//          in.var.hidden = true;
//          in.var2 = byVar;
//          in.addChild( by );
//          inputs.add( in );
//        }
//    ;

    
//coaggregate returns [Expr r=null]
//    { 
//      BindingExpr b;
//      ArrayList<BindingExpr> bs = new ArrayList<BindingExpr>(); 
//    }
//    : "coaggregate" b=vpipe {bs.add(b);} ("," b=vpipe {bs.add(b);})* r=expr
//       { r = CoaggregateExpr.make(env, bs, r); }
//    ;


unroll returns [Expr r=null]
    { ArrayList<Expr> args = new ArrayList<Expr>(); }
    : "unroll" r=fnCall  { args.add(r); }
          ( r=estep      { args.add(r); } )+
          // TODO: as name
      {
        r = new UnrollExpr(args);
      }
    ;


estep returns [Expr r = null] // TOD: Unify step expressions
    : r=projName       { r = new UnrollField(r); }
    | "[" r=expr "]"   { r = new UnrollIndex(r); }
    // TODO: add [*], .*
    ;
	

assign returns [Expr r=null]
	{ String v; }
    : v=avar "=" r=rvalue  { r = new AssignExpr(env.sessionEnv().scopeGlobal(v), r); } // TODO: var.type = non-pipe, do-block scope
    | r=pipe "=>" v=var    { r = new AssignExpr(env.sessionEnv().scopeGlobal(v), r); } // TODO: var.type = non-pipe, do-block scope
	;

optAssign returns [Expr r=null]
    { String v; }
    : v=avar "=" r=rvalue  { r = new BindingExpr(BindingExpr.Type.EQ, env.scope(v), null, r); } 
    | r=pipe 
         ( "=>" v=var      { r = new BindingExpr(BindingExpr.Type.EQ, env.scope(v), null, r); } )?  // { r = new AssignExpr(env.sessionEnv().scopeGlobal(v), r); } )?
    ;

// Same as optAssign but creates global variables on assigment, and inlines referenced globals
topAssign returns [Expr r=null]
    { String v; }
    : ( v=avar "=" r=rvalue  { r = new AssignExpr( env.sessionEnv().scopeGlobal(v), r); } // TODO: expr name should reflect global var
      | r=pipe
         ( /*empty*/         { r = env.importGlobals(r); } 
         | "=>" v=var        { r = new AssignExpr( env.sessionEnv().scopeGlobal(v), r); } )
      )
    ;

rvalue returns [Expr r = null]
    : r=pipe
//    | r=collection
    // | r=function
    | r=extern
    ;
	
//collection returns [Expr r = null]
//	: "collection" r=expr { r = new FileExpr(r); } // TODO: rename to CollectionExpr
//	// TODO: this expression is evaluated immediately and replaced with a constant; file is registered, dups detected, type found. read and write get constant.
//	;

extern returns [Expr r = null]
    { String lang; }
    : "extern" lang=name "fn" r=expr
      { r = new ExternFunctionExpr(lang, r); }
    ;
      
function returns [Expr r = null]
    { ArrayList<Var> p; }
    : "fn" p=params r=expr
    { 
      r = new DefineFunctionExpr(null, p, r);
      for( Var v: p )
      {
        env.unscope(v);
      }
    }
    ;


params returns [ArrayList<Var> p = new ArrayList<Var>()]
    { String v; }
    : "(" ( v=var          { p.add(env.scope(v)); }
             ( "," v=var   { p.add(env.scope(v)); } )*
          )?
      ")"
    ;

//exceptions
//	: "exceptions" "to" str ("limit" expr)?
//	;

//returnStmt returns [Expr r=null]
//	: "return" ( r=topExpr )? // ("," var)* )?
//	;
	
functionDef returns [Expr r=null]
	{ String lang; String s; String body; Expr e; }
	// : "function" name "(" (var ("," var)*)? ")" block
	: "script" lang=name body=str
	  {
	  	r = new ScriptBlock(lang, body);
	  }
	| "import" lang=name s=name e=expr (SEMI|EOF)
	  {
	  	if( ! "java".equals(lang.toLowerCase()) ) oops("only java functions supported right now");
	  	r = new RegisterFunctionExpr(new ConstExpr(new JString(s)), e);
	  }
	;
	
forExpr returns [Expr r = null]
    { 
    	ArrayList<BindingExpr> bs = new ArrayList<BindingExpr>();
    }
    : "for" "(" forDef[bs] ("," forDef[bs])* ")" r=expr
//    : "for" forDef[bs] ("," forDef[bs])* 
//        ( "into" r=expr    { r = new ArrayExpr(r); }
//        | "expand" r=expr )
    {
      for(int i = 0 ; i < bs.size() ; i++ )
      {
        BindingExpr e = bs.get(i);
        env.unscope(e.var);
      }
      MultiForExpr f = new MultiForExpr(bs, null, r /*new ArrayExpr(r)*/); // TODO: eleminate WHERE, array return, make native multifor
      r = f.expand(env);
    }
    ;


forDef[ArrayList<BindingExpr> bindings]
    { String v; Expr e; String v2 = null; BindingExpr.Type t = null; BindingExpr b; }
//    : b=vpipe
//    {
//    	bindings.add(b);
//    }
    : v=var ( /*( "at" v2=var )?*/  "in" e=pipe { t = BindingExpr.Type.IN; }
            // | ":" v2=var "in" e=pipe        { t = BindingExpr.Type.INREC; }
            // | "="  e=expr                { t = BindingExpr.Type.EQ; }
            )
    { 
      Var var = env.scope(v);
//      Var var2 = null;
//      if( v2 != null )
//      {
//        var2 = env.scope(v2); 
//      }
      bindings.add( new BindingExpr(t, var, null, e) );
    }
    ;


ifExpr returns [Expr r=null]
	{ Expr p=null; Expr s=null; }
	: "if" "(" p=expr ")" r=expr 
	  ( options {greedy=true;} : 
        "else" s=expr )?
	{
		r = new IfExpr(p, r, s);
	}
	;

//combineExpr returns [CombineExpr r = null]
//    { String v1, v2; Var var1 = null; Var var2 = null; Expr in; Expr use; }
//    : 
//      "combine" v1=var "," v2=var "in" in=pipe
//         { 
//           var1 = env.scope(v1);
//           var2 = env.scope(v2);
//         }
//      "using" use=expr
//         {
//           env.unscope(var1);
//           env.unscope(var2);
//           r = new CombineExpr(var1, var2, in, use);
//         }
//    ;


sort[Expr in] returns [Expr r=null]
    : "sort" r=sortCmp
      { r = new SortExpr(in, r); }
    ;

sortCmp returns [Expr r=null]
    { String v="$"; }
    : ("each" v=var)? "by" r=cmpArrayFn[v]
    | "using" r=comparator
    ;
    
//sortSpec returns [ArrayList<OrderExpr> by = new ArrayList<OrderExpr>()]
//    : "(" (sortStep[by] ("," sortStep[by])*)? ")"
//    //| "[" (sortStep[by] ("," sortStep[by])*)? "]"
//    //| "{" (sortField[by] ("," sortField[by])*)? "}"
//    ;
//
//sortStep[ArrayList<OrderExpr> by]
//    { Expr e; OrderExpr.Order c; }
//    : e=expr c=cmpSpec    { by.add( new OrderExpr(e,c) ); }
//    ;
    
record returns [RecordExpr r = null]
	{ ArrayList<FieldExpr> args = new ArrayList<FieldExpr>();
	  FieldExpr f; }
	: "{" ( f=field  { args.add(f); } )? ( "," ( f=field  { args.add(f); } )? )*  "}"
	{ r = new RecordExpr(args.toArray(new Expr[args.size()])); }
    ;

field returns [FieldExpr f=null]  // TODO: lexer ID "(" => FN_NAME | keyword ?
	{ Expr e = null; Expr v=null; boolean required = true; }
    : e=fname ( "?" { required = false; } )?  ":" v=expr  
      { 
      	f = new NameValueBinding(e, v, required); 
      }
	| e=path ( DOT_STAR    
	           { 
	           	 f = new CopyRecord(e);
	           }
	         | ( "?" { required = false; } )?
	           ( ":" v=expr )?
	           {
	           	 if( v != null )
	           	 {
	           	    f = new NameValueBinding(e, v, required); 
	           	 }
                 else if( e instanceof VarExpr )
                 {
                   f = new NameValueBinding( ((VarExpr)e).var(), required );
                 }
                 else if( e instanceof PathExpr )
                 {
                   // TODO: { $r.x } becomes { $r.{.x} }.  keep it that way or leave it as { $r.x }??
                   PathStep ret = ((PathExpr)e).getReturn();
                   Expr step = ret.parent();
                   if( step instanceof PathFieldValue )
                   {
                   	 step.replaceInParent(ret);
                   	 f = new CopyField(e, step.child(0), required ? CopyField.When.DEFINED : CopyField.When.NONNULL );
                   }
                   else
                   {
                     // ((PathExpr)e).forceRecord();
                     f = new CopyRecord(e);
                   }
                 }
                 else
                 {
                   oops("field name required, or use (expr).* to copy records");
                 }
	           }
	         )
	;    


expr returns [Expr r]
    : r=group
    | r=join
    | r=equijoin
    | r=forExpr
    // | r=taggedMerge
    | r=ifExpr
    | r=unroll
    // | r=combineExpr
    | r=function
    | r=orExpr
    ;


orExpr returns [Expr r]
	{ Expr s; }
	: r=andExpr ( "or" s=andExpr { r = new OrExpr(r,s); } )*
	;

andExpr returns [Expr r]
	{ Expr s; }
	: r=notExpr ( "and" s=notExpr { r = new AndExpr(r,s); } )*
	;

notExpr returns [Expr r]
	: "not" r=notExpr  { r = new NotExpr(r); }
	| r=kwTest
	;

kwTest returns [Expr r]
    : r=inExpr
    | r=isnullExpr
    | r=isdefinedExpr
    | r=existexpr
    ;

isnullExpr returns [Expr r]
    : "isnull" r=inExpr
    { r = new IsnullExpr(r); }
    ;

existexpr returns [Expr r]
    : "exists" r=inExpr
    { r = new ExistsFn(r); }
    ;


isdefinedExpr returns [Expr r]
    { Expr n; }
    : "isdefined" r=fnCall n=projName // TODO: this should be a path expression
    { r = new IsdefinedExpr(r,n); }
    ;

    
inExpr returns [Expr r = null]
	{ Expr s; }
	: r=compare ( "in" s=compare  { r = new InExpr(r,s); } )?
	;

compare returns [Expr r = null]
    { int c; Expr s; Expr t; }
    : r=instanceOfExpr 
          ( c=compareOp  s=instanceOfExpr  { r = new CompareExpr(c,r,s); }
               ( c=compareOp       { s = s.clone(new VarMap(env)); }  // TODO: introduce a variable?
                 t=instanceOfExpr  { r = new AndExpr( r, new CompareExpr(c,s,t) ); s=t; } 
               )*
          )? 
    ;

compareOp returns [int r = -1]
    : "==" { r = CompareExpr.EQ; }
    | "<"  { r = CompareExpr.LT; }
    | ">"  { r = CompareExpr.GT; }
    | "!=" { r = CompareExpr.NE; }
    | "<=" { r = CompareExpr.LE; }
    | ">=" { r = CompareExpr.GE; }
    ;

instanceOfExpr returns [Expr r = null]
    { Expr s; }
    : r=addExpr 
    ( "instanceof" s=addExpr { r = new InstanceOfExpr(r,s); } )? 
    ;

addExpr returns [Expr r = null]
	{ Expr s; int op; }
    : r=multExpr ( op=addOp s=multExpr  { r = new MathExpr(op,r,s); } )*
    ;

addOp returns [int op=0]
	: "+" { op=MathExpr.PLUS; }
	| "-" { op=MathExpr.MINUS; }
	;

multExpr returns [Expr r = null]
	{ Expr s; int op; }
    : r=unaryAdd ( op=multOp s=unaryAdd  { r = new MathExpr(op,r,s); } )*
    ;

multOp returns [int op=0]
	: "*" { op=MathExpr.MULTIPLY; }
	| "/" { op=MathExpr.DIVIDE; }
	;

// TODO: there is a bug handling negative numbers minLong (= -maxLong -1) doesn't parse
// TODO: there is a bug parsing large integers that can fit into a decimal
unaryAdd returns [Expr r = null]
	: "-" r=typeExpr { r = MathExpr.negate(r); } 
	| ("+")? r=typeExpr	
	;
	
typeExpr returns [Expr r = null]
    { Schema s; }
    : "type" s=type   { r = new ConstExpr(new JSchema(s)); }
    | r=path
    ;	

path returns [Expr r=null]
    { PathStep s=null; PathStep in=null; }
    : r=fnCall 
       ( s=step[in = new PathExpr(r)] 
           ( s=step[s] )*
         { r = in; }
       )?
    ;

step[PathStep ctx] returns [PathStep r = null]
    { Expr e; Expr f; ArrayList<PathStep> pn; }
    : ( e=projName                   { r = new PathFieldValue(e); }
      | "{" pn=projNames "}"         { r = new PathRecord(pn); } // TODO: all steps after names: {.x.y, .z[2]}
      | "!" "{" pn=projNotNames "}"  { r = new PathNotRecord(pn); }
      | "[" ( e=expr
                ( /*empty*/       { r = new PathIndex(e); }
                | ":" ( f=expr    { r = new PathArraySlice(e,f); }
                      | "*"       { r = new PathArrayTail(e); }
              ))
            | "*" ( /*empty*/     { r = new PathArrayAll(); }
                  | ":" ( e=expr  { r = new PathArrayHead(e); }
                        | "*"     { r = new PathArrayAll(); }
                ))
          ) // ( "*" { ((PathArray)r).setExpand(true); } )? // TODO: add shorthand to expand?
        "]"
      )
      {
      	ctx.setNext(r);
      }
    ;

projNames returns [ArrayList<PathStep> names = new ArrayList<PathStep>()]
    : ( wildProjName[names] ("," wildProjName[names])* )?
    ;

wildProjName[ArrayList<PathStep> names] // TODO: eliminte .prefix* code
    { Expr e; PathStep s=null; }
    : ( e=projName 
        ( /*empty*/   { s = new PathOneField(e); } // TODO: ? indicator to eliminate nulls
        // | "*"         { s = new PathPrefixFields(e); }
        )
      | "*"           { s = new PathAllFields(); }
      )               {  names.add(s);  }
      ( s=step[s] )*
    ;


projNotNames returns [ArrayList<PathStep> names = new ArrayList<PathStep>()]
    : ( wildProjNotName[names] ("," wildProjNotName[names])* )?
    ;

wildProjNotName[ArrayList<PathStep> names]
    { Expr e; }
    : e=projName 
        ( /*empty*/ { names.add(new PathOneField(e)); }
        // | "*"       { names.add(new PathPrefixFields(e)); }
        )
    | "*"           { names.add(new PathAllFields()); }
    ;
    
projName returns [Expr r=null]
    : r=dotName
    | DOT r=basic
    ;

fnCall returns [Expr r=null]
    { String s; ArrayList<Expr> args; }
    : ( r=basic 
      | r=builtinCall )
	     ( args=fnArgs    { r = new FunctionCallExpr(r, args); } )*
	;

builtinCall returns [Expr r=null]
    { String s; ArrayList<Expr> args; }
    : s=name args=fnArgs
    { 
      r = FunctionLib.lookup(env, s, args);
    }
    ;
    
basic returns [Expr r=null]
    : r=constant
    | r=record
    | r=array
    | r=varExpr
    | r=cmpExpr
    | r=parenExpr
    ;
	
parenExpr returns [Expr r=null]
//    : "(" r=letExpr ")"
    : "(" r=block ")"
	;

//letExpr returns [Expr r=null]
//	{ String v; Var var=null; Expr s; }
//	: v=avar "=" s=expr ";"
//	  {
//	  	var = env.scope(v); 
//	  } 
//	  r=letExpr
//	  {
//	  	env.unscope(var);
//	  	r = new LetExpr(var, s, r);
//	  }
//	| r=expr
//	;

//nameUse returns [Expr r=null] // TODO: eliminate this if var keeps $
//	: r=fnCall
//	: r=varExpr 
//    ;
    
//nameUse2[BindingExpr b] returns [Expr r=null]
//    { String v; }
//    : r=fnCall    { r = new DoExpr(b, r); }
//    | v=var       { r = new BindingExpr(BindingExpr.Type.IN, env.scope(v), null, b.inExpr()); } // { r = new AssignExpr( env.sessionEnv().scopeGlobal(v), input); } // TODO: var.type = pipe, do-block scope
//    ;
    
//fnCall returns [Expr r=null]
//	{ String s; ArrayList<Expr> args; }
//    : s=name "(" args=exprList ")" { r = FunctionLib.lookup(env, s, args); }
//    | s=var "(" args=exprList ")"  { r = new FunctionCallExpr(new VarExpr(env.inscope(s)), args); }
//	;	

//fnCall returns [Expr r=null]
//    { String s; Expr e; ArrayList<Expr> args; }
//    :// s=name "(" args=exprList ")" { r = FunctionLib.lookup(env, s, args); }
//     e=basic "(" args=exprList ")"  { r = new FunctionCallExpr(e, args); }
//    ;   

varExpr returns [Expr r = null]
    { String v; }
    : v=var 
    { r = new VarExpr( env.inscope(v) ); }
    ;


array returns [Expr r=null]
	{ ArrayList<Expr> a; }
	: "[" a=exprList2 "]"
	{ r = new ArrayExpr(a); }
	;


fnArgs returns [ArrayList<Expr> r = new ArrayList<Expr>()]
    : "(" r=exprList ")"
    { 
    	for(Expr e: r)
    	{
    		if( e instanceof AssignExpr )
    		{
    			oops("Call by name NYI");
    		}
    	}
    }
    
    ;

exprList returns [ArrayList<Expr> r = new ArrayList<Expr>()]
	{ Expr e; }
	: ( e=pipe 	    { r.add(e); }
	    ("," e=pipe    { r.add(e); } )*
	  )?
	;

// An exprList that allows ignores empty exprs
exprList2 returns [ArrayList<Expr> r = new ArrayList<Expr>()]
    { Expr e; }
    : ( e=pipe  { r.add(e); } )? ("," ( e=pipe  { r.add(e); } )? )*
    ;

constant returns [Expr r=null]
	{ String s; }
    : s=str		 { r = new ConstExpr(new JString(s)); }
    | i:INT      { r = new ConstExpr(new JLong(i.getText())); }
    | n:DEC      { r = new ConstExpr(new JDecimal(n.getText())); }
    | d:DOUBLE   { r = new ConstExpr(new JDouble(d.getText())); }
    | h:HEXSTR   { r = new ConstExpr(new JBinary(h.getText())); }
    | t:DATETIME { r = new ConstExpr(new JDate(t.getText())); }
    | r=boolLit
    | r=nullExpr
    ;

boolLit returns [Expr r=null]
    : "true"     { r = new ConstExpr(JBool.trueItem); }
    | "false"    { r = new ConstExpr(JBool.falseItem); }
    ;
    
nullExpr returns [Expr r=null]
    : "null"     { r = new ConstExpr(Item.nil); }
    ;
    
str returns [String r=null]
	: s:STR 			{ r = s.getText(); }
	| h:HERE_STRING     { r = h.getText(); }
	| b:BLOCK_STRING    { r = b.getText(); }
	;

	
avar returns [String r=null]: n:AVAR { r = n.getText(); }; //  TODO: move to ID
var returns [String r=null]: n:VAR { r = n.getText(); }; // TODO: move to ID
name returns [String r=null]: n:ID { r = n.getText(); };
dotName returns [Expr r = null]
    : i:DOT_ID     { r = new ConstExpr(new JString(i.getText())); }
    ;

fname returns [Expr r=null] 
    : i:FNAME     { r = new ConstExpr(new JString(i.getText())); }
    ;

simpleField returns [Expr f=null]
    : ( f=fname | "(" f=pipe ")" ) ":"
    ;

type returns [Schema s = null]
    { Schema s2 = null; SchemaOr os = null; }
    : s = typeTerm 
    ( "|"              { s2 = s; s = os = new SchemaOr(); os.addSchema(s2); }
      s2 = typeTerm    { os.addSchema(s2); } 
    )*
    ;
    
typeTerm returns [Schema s = null]
    : "*"           { s = new SchemaAny(); }
    | s=oneType
       ( "?"        { s = new SchemaOr(s, new SchemaAtom("null")); } 
       )?
    ;

oneType returns [Schema s = null]
    : s=atomType
    | s=arrayType
    | s=recordType
    ;

atomType returns [SchemaAtom s = null]
    : i:ID    { s = new SchemaAtom(i.getText()); }
    | "null"  { s = new SchemaAtom("null"); }
    | "type"  { s = new SchemaAtom("type"); }
    ;

arrayType returns [SchemaArray s = new SchemaArray()]
    { Schema head = null; Schema p; Schema q; }
    : "["
        ( p=type          { head = p; }
          ( "," q=type    { p = p.nextSchema = q; }
          )*
          arrayRepeat[head,s]
        )?
      "]"
    ;

arrayRepeat[Schema typeList, SchemaArray s]
    { long lo = 0; long hi = 0; }
    : /*empty*/           { s.noRepeat(typeList); }
    | ( "*"                 { lo = 0; hi = SchemaArray.UNLIMITED; }
      | "+"                 { lo = 1; hi = SchemaArray.UNLIMITED; }
      | "<" 
          ( "*"             { lo = 0; hi = SchemaArray.UNLIMITED; }
          | i1:INT          { lo = Long.parseLong(i1.getText()); }
            ( /*empty*/     { hi = lo; }
            | "," ( "*"     { hi = SchemaArray.UNLIMITED; }
                  | i2:INT  { hi = Long.parseLong(i2.getText()); }
                  )
            )
          )
        ">" 
      ) { s.setRepeat(typeList, lo, hi); }
    ;

recordType returns [SchemaRecord s = new SchemaRecord()]
    { SchemaField f; }
    : "{"
        ( f=fieldType        { s.addField(f); }
          ( "," f=fieldType  { s.addField(f); }
          )*
        )?
      "}"
    ;

fieldType returns [SchemaField f = new SchemaField()]
    { String n; Schema t; }
    : (  n=constFieldName   { f.name = new JString(n); }
         ( "*"              { f.wildcard = true; }
         | "?"              { f.optional = true; }
         )?
      | "*"               { f.name = new JString(""); 
                            f.wildcard = true;     }
      )
      ":" t=type        { f.schema = t; }
    ;

constFieldName returns [String s=null]
    : i:FNAME   { s = i.getText(); }
    | s=str
    ;

class JaqlLexer extends Lexer;

options {
  charVocabulary = '\3'..'\377'; // all characters except special ANTLR ones
  testLiterals=false;    // don't automatically test for literals
  k=3;                   // lookahead // TODO: try to reduce to 2
}

{
	private int indent;
	private int blockIndent;
	private String blockTag;
}

protected DIGIT
  : '0'..'9'
  ;

protected HEX
  : '0'..'9'|'a'..'f'|'A'..'F'
  ;

protected LETTER
  : 'a'..'z'|'A'..'Z'
  ;

WS
	: ( ' ' 
      | '\t'
      | '\f'
      | ( options { generateAmbigWarnings=false; }
        : "\r\n"
        | '\r'
        | '\n'
        ) { newline(); }
      )+
		{ $setType(Token.SKIP); }
	;

COMMENT
	: "//" (~('\n'|'\r'))* 
		{$setType(Token.SKIP);}
    ;
    
protected NL
	options { generateAmbigWarnings=false; }
  	: "\r\n" | '\r' | '\n' { newline(); }
  	;
  	
ML_COMMENT
	:	"/*"
		( ~('*'|'\n'|'\r')
        | '*' ~('/')
        | NL
		)*
		"*/"
		{$setType(Token.SKIP);}
	;


	
protected BLOCK_LINE1
	: (' '! (~('\n'|'\r'))*)? NL
	{
		for( indent = 1; indent < blockIndent && LA(1) == ' ' ; indent++)
		{
			_saveIndex=text.length();
			match(' ');
			text.setLength(_saveIndex);
		}
	}
	;
	

//protected BLOCK_STRING1 // options { generateAmbigWarnings=false; }
//	{ blockIndent = 0; }
//	: "|"! (' '! | '\t'!)* NL! (' '! {blockIndent++;})+ (~(' '|'\t'|'\n'|'\r'))* NL (BLOCK_LINE)*
//	{ System.out.println("here: ["+new String(text.getBuffer(), _begin, text.length()-_begin)+"]"); }
//	;
//
//protected BLOCK_LINE
//	: '|'! (' '! (~('\n'|'\r'))*)? NL
//	;
//
//BLOCK_STRING
//	: (BLOCK_LINE (' '! | '\t'!)* )+
//	;

protected HERE_TAG
	{ int start = text.length(); }
	: (' '|'\t')*           { text.setLength(start); } // ignore leading whitespace 
	  ~(' '|'\t'|'\n'|'\r'|'\f') (~('\n'|'\r'))*
	  { 
	  	blockTag = new String(text.getBuffer(), start, text.length());
	  	text.setLength(start);
	  }
	;

protected HERE_LINE
	{ 
		boolean firstLine = text.length() == 0; 
	}
	: NL 
	{
		newline();
		if( firstLine ) // don't put the first newline in the token
		{
			text.setLength(0);
		}
		int start = text.length();
		int i;
		char c;
		int n = blockTag.length();
		boolean done = false;
		for( i = 0 ; i < n && (c = LA(1)) == blockTag.charAt(i) ; i++ )
		{
			match(c);
		}
		if( i == n )
		{
			// We matched the tag; look for whitespace then newline
			while( (c = LA(1)) == ' ' || c == '\t' )
			{
				match(c);
			}
			if( (c = LA(1)) == '\n' || c == '\r' )
			{
				// We found the end; consume until we find a non-newline to signal end of string
				while( (c = LA(1)) == '\n' || c == '\r' )
				{
					match(c);
				}
				// Erase the tag
				text.setLength(start);
				done = true;
			}
		}
		if( !done )
		{
			while( (c = LA(1)) != '\n' && c != '\r' )
			{
				match(c);
			}
		}
	}
	;

protected HERE_END
	{ int start = text.length(); }
	: (~('\r'|'\n'))
	{ text.setLength(start); } 
	;

HERE_STRING
	: '<'! '<'! HERE_TAG (HERE_LINE)+ HERE_END
	;

protected VAR1
	: '$' ('_'|LETTER|DIGIT)*
    ;

VAR
	options { ignore=WS; }
    : (VAR1 '=' ('='|'>')) => VAR1
    | (VAR1 '=') => VAR1     { $setType(AVAR); }
	| VAR1 
    ;

SYM
    options { testLiterals=true; }
    : '(' | ')' | '[' | ']' | ',' 
    | '|' | '&'
    | '}' 
    | '=' ('=' | '>')? | ('<' | '>' ) ('=')? | "!" ("=")?
    | '/' | '*' | '+' | '-' ('>' | '|')?
    | ':' (':'|'=')? | '?'
    ;

SEMI
    options { testLiterals=true; }
    : ';'
    ;
    
SYM2
    options { testLiterals=true; }
    : '{' (('=' | (('<' | '>') ('=')?) | "!=") '}')?
    ;


// STRCHAR doesn't allow single OR double quotes.
protected STRCHAR
     : (~( '\'' | '"' | '\\' | '\r' | '\n' ))
     | ( options { generateAmbigWarnings=false; }
       : "\r\n"
       | '\r'
       | '\n'
       ) { newline(); $setText("\n"); }
     | '\\' ( '\''  { $setText("\'"); }
            | '\"'  { $setText("\""); }
            | '\\'  { $setText("\\"); }
            | '/'   { $setText("/"); }
            | 'b'   { $setText("\b"); }
            | 'f'   { $setText("\f"); }
            | 'n'   { $setText("\n"); }
            | 'r'   { $setText("\r"); }
            | 't'   { $setText("\t"); }
            | ('x'|'X') x1:HEX x2:HEX
                { byte b = BaseUtil.parseHexByte(x1.getText().charAt(0),
                	                             x2.getText().charAt(0));
                  String s = Character.toString((char)b);
                  $setText( s ); }
            | ('u'|'U') u1:HEX u2:HEX u3:HEX u4:HEX  
                { char c = BaseUtil.parseUnicode(u1.getText().charAt(0),
                	                             u2.getText().charAt(0),
                	                             u3.getText().charAt(0),
                	                             u4.getText().charAt(0));
                  String s = Character.toString(c);
                  $setText( s ); }
	        )
     ;

STR
    : '\"'! ( STRCHAR | '\'' )* '\"'!
    | '\''! ( STRCHAR | '\"' )* '\''!
    ;

// TODO: this is going away
HEXSTR
    : ('x'!|'X'!) '\"'! (~('\"'))* '\"'!
    | ('x'!|'X'!) '\''! (~('\''))* '\''!
    ;
    
//protected REGEX_CHAR
//     : (~( '/' | '\\' ))
//     | '\\' ( '/'  { System.out.println("hi"); $setText("/"); }
//            | .
//	        )
//    ;
//
//REGEX
//	: '/' ( REGEX_CHAR )* '/' ('g'|'i'|'m')*
//	;

protected INT
	: (DIGIT)+
	;
	
protected DEC
    : INT ( ( '.' INT ( ('e'|'E') ('+' | '-')? INT )? )
          | ( ('e'|'E') ('+' | '-')? INT ) )
    | '.' INT ( ('e'|'E') ('+' | '-')? INT )?
// TODO: get these as keywords/identifiers
//   | "Infinity"
//   | "NaN"
//   | "sNaN"
//   | "qNaN"
    ;
    
DOTTY
    options { testLiterals=false; }
 	: (DEC) => 
 	     DEC   		
 	       ( /*empty*/       {$setType(DEC);} // TODO: flag to control default decimal/double
 	       | "m"!            {$setType(DEC);}
 	       | "d"!            {$setType(DOUBLE);}
 	       )
    |  INT
        ( /*empty*/    		 {$setType(INT);} // TODO: flag to control default decimal/double
        | "m"!               {$setType(INT);}
        | "d"!               {$setType(DOUBLE);}
        )
    | '0'('x'|'X')(HEX)+     {$setType(INT);}
    | (DOT_ID) => DOT_ID     {$setType(DOT_ID);}
    | (DOT_STAR) => DOT_STAR {$setType(DOT_STAR); }
    | '.'                    {$setType(DOT);}
    ;

protected IDWORD
    : ('@'|'_'|LETTER) ('@'|'_'|'#'|LETTER|DIGIT)*
    ;

ID
	options { ignore=WS; }
    : (IDWORD ('?')? ':') => IDWORD {$setType(FNAME);}
    | (IDWORD '*') => IDWORD
	// | (IDWORD '=') => IDWORD {$setType(AVAR);}
	| IDWORD { _ttype = testLiteralsTable(_ttype); }
	;

protected DOT_ID
	: '.'! IDWORD
	;

protected DOT_STAR
    : '.' '*'
    ;
	    
DATETIME
    : ('d'!|'D'!) '\"'! (~('\"'))* '\"'!
    | ('d'!|'D'!) '\''! (~('\''))* '\''!
    ;
    
