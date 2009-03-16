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
 
header {
package com.ibm.jaql.lang.parser;

import java.util.*;

import com.ibm.jaql.lang.core.*;
import com.ibm.jaql.lang.expr.core.*;
import com.ibm.jaql.lang.expr.top.*;

import com.ibm.jaql.json.type.*;
import com.ibm.jaql.json.schema.*;
import com.ibm.jaql.json.util.*;

import com.ibm.jaql.util.*;

}

options {
  language="Java";
}



class JaqlParser extends Parser;
options {
    k = 1;                          // number of tokens to lookahead
	exportVocab=Jaql;               // Call its vocabulary "Jaql"
    defaultErrorHandler = false;     // Don't generate parser error handlers
}
{
  public Env env = new Env();
  public boolean done;
}

query returns [Expr r = null]
	{ done = false; DefineFunctionExpr f; }
	: "declare" f=functionDef[true]
    | r=stmt (SEMI | EOF)
    | SEMI
    | EOF						{ done=true; }
    ;
    
stmt returns [Expr r = null]
    { String v; }
    : r=assignment
	| r=expr              { r = new QueryExpr(env.importGlobals(r)); }
    | "explain" r=expr    { r = new ExplainExpr(env.importGlobals(r)); }
	| "materialize" v=var { r = new MaterializeExpr(env.sessionEnv().inscope(v)); }
	| "quit"              { done=true; }
	;

assignment returns [Expr r = null]
	{ String v; }
	: v=avar "=" r=expr
	  { r = new AssignExpr(v, r); }
	;
	
// TODO: should functions be in the session state?
// TODO: how to catalog functions in the db?
// TODO: need to scope/unscope declared functions
// TODO: is declare needed?  can we declare other things (vars)?

expr returns [Expr r = null]
    : r=kwExpr
    | r=orExpr
	;
	
kwExpr returns [Expr r = null]
    : r=forExpr
	| r=letExpr
    | r=ifExpr
	| r=joinExpr
	| r=groupExpr
    | r=sortExpr
	| r=combineExpr
	| r=reduceExpr
	| r=functionDef[false]
//    | r=unnestExpr
//    | "some"  bs=inBindingList "satisfies" p=expr {
//        { bs[-1].setWhere(p); foreach b in bs: bs[-1].setWhere();  r=None }
//    | "every" bs=inBindingList "satisfies" p=expr { r=None }
	;
	
functionDef[boolean declaring] returns [DefineFunctionExpr fn = null]
	{ String i = null; ArrayList<Var> p; Var fnVar = null; Expr body; }
    : "fn" ( i=id         { fnVar = env.scope(i); } )? 
       p=params 
       body=expr        
    { 
	  fn = new DefineFunctionExpr(fnVar, p, body);
      if( fnVar != null && !declaring )
      {
        env.unscope(fnVar);
      }
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

recordExpr returns [RecordExpr r = null]
	{ ArrayList<FieldExpr> args = new ArrayList<FieldExpr>();
	  FieldExpr f; }
	: "{" ( f=fieldExpr          { args.add(f); }
	        ( "," ( f=fieldExpr  { args.add(f); }
	              )? 
	        )*
	      )? 
	  "}"
	{ r = new RecordExpr(args.toArray(new Expr[args.size()])); }
    ;

fieldExpr returns [FieldExpr f]
	{ Expr n = null; String i; VarExpr v; }
	: i=id f=fieldValue[new ConstExpr(new JString(i))]
    | n=literalExpr ( f=fieldValue[n] 
                    | f=projPattern[n] )
    | v=varExpr ( ("?" ":") => f=fieldValue[v] 
                | f=projPattern[v] 
                | f=varField[v] )
	;
    
fieldValue[Expr name] returns [FieldExpr f = null]
	{ Expr v; boolean required = true; }
	: ("?" { required = false; } )?
      ":" v=expr
    { f = new NameValueBinding(name, v, required); }
    ;

projPattern[Expr ctx] returns [FieldExpr f = null]
	{ Expr n = null; boolean wild = false; }
    : ( n=dotId        ( "*" { wild = true; } )?
      | "." ( n=basic  ( "*" { wild = true; } )?
            | "*" { wild = true; }
            ) 
      )
      { f = new ProjPattern(ctx,n,wild); }
    ;
    
varField[VarExpr ve] returns [FieldExpr f = null]
	{ boolean required = true; }
	: ("?" { required = false; } )?
	{ 
		String name = ve.var().name().substring(1);
        f = new NameValueBinding(name,ve,required);
    }
    ;

dotId returns [Expr r = null]
    : i:DOT_ID     { r = new ConstExpr(new JString(i.getText())); }
    ;
    
dotName returns [Expr r = null]
    : r=dotId
    | "." r=basic 
    ;
    
fieldName returns [Expr r = null]
	{ String i; }
	: r=basic
	| i=id		{ r = new ConstExpr(new JString(i)); }
	;

constFieldName returns [String name = null]
	: name=id
	| s:STR		{ name=s.getText(); }
	;

orExpr returns [Expr r = null]
	{ Expr s; }
	: r=andExpr ( "or" s=andExpr { r = new OrExpr(r,s); } )*
	;

andExpr returns [Expr r = null]
	{ Expr s; }
	: r=notExpr ( "and" s=notExpr { r = new AndExpr(r,s); } )*
	;

notExpr returns [Expr r = null]
	: "not" r=notExpr { r = new NotExpr(r); }
	| r=inExpr
	;

inExpr returns [Expr r = null]
	{ Expr s; }
	: r=compare ( "in" s=compare  { r = new InExpr(r,s); } )?
	;

compare returns [Expr r = null]
    { int c; Expr r2; }
    : r=instanceOfExpr ( c=compareOp  r2=instanceOfExpr { r = new CompareExpr(c,r,r2); } )?
    ;

compareOp returns [int r = -1]
    : "==" { r = CompareExpr.EQ; }
    | "<"  { r = CompareExpr.LT; }
    | ">"  { r = CompareExpr.GT; }
    | "!=" { r = CompareExpr.NE; }
    | "<=" { r = CompareExpr.LE; }
    | ">=" { r = CompareExpr.GE; }
    ;
	
// FIXME: make "type blah" a basic type when Schema is an atom.
instanceOfExpr returns [Expr r = null]
	{ Expr s; }
	: r=toExpr 
	( "instanceof" s=toExpr { r = new InstanceOfExpr(r,s); } )?
	;

toExpr returns [Expr r = null]
	{ Expr s; }
    : r=addExpr ( "to" s=addExpr  { r = new RangeExpr(r,s); } )?
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
	: "-" r=access { r = MathExpr.negate(r); } 
	| ("+")? r=access	
	;

access returns [Expr r = null]
    { String i; ArrayList<Expr> args; }
    : ( r=construct
      | i=id "(" args=exprList ")"  { r = FunctionLib.lookup(env, i, args); }
      )
      r=step[r]
    ;

step[Expr ctx] returns [Expr r = null]
    { Var var = null; ArrayList<Expr> args; boolean addArray = true; }
    : /* empty */                { r = ctx; }
    | ( ( r=dotName              { r = new FieldValueExpr(ctx, r); }
        | "(" args=exprList ")"  { r = new FunctionCallExpr(ctx, args); } 
        | "[" 
            ( r=expr "]"         { r = new IndexExpr(ctx, r); }
            | "*" ( "*"          { addArray = false; }
                  )?
                  "]"            { var = env.makeVar("$star"); r = new VarExpr(var); }
            )
        )
        r=step[r]
      )
      { 
      	if( var != null ) 
      	{
      	  if( addArray )
      	  {
      	  	r = new ArrayExpr(r); 
      	  }
      	  r = new ForExpr(var, ctx, r); 
      	}
      }
    ;

    
construct returns [Expr r = null]
    : "true"     { r = new ConstExpr(JBool.trueItem); }
    | "false"    { r = new ConstExpr(JBool.falseItem); }
    | "null"     { r = new ConstExpr(Item.nil); }
    | r=typeExpr
    | r=arrayExpr
    | r=recordExpr
    //| r=functionDef[false]
    | r=basic
    ;

basic returns [Expr r = null]
	: r=varExpr
	| r=literalExpr
	;

literalExpr returns [Expr r = null]
    : s:STR      { r = new ConstExpr(new JString(s.getText())); }
    | i:INT      { r = new ConstExpr(new JLong(i.getText())); }
    | n:DEC      { r = new ConstExpr(new JDecimal(n.getText())); }
    | d:DOUBLE   { r = new ConstExpr(new JDouble(d.getText())); }
    | h:HEXSTR   { r = new ConstExpr(new JBinary(h.getText())); }
    | t:DATETIME { r = new ConstExpr(new JDate(t.getText())); }
    // | x:REGEX    { r = new ConstExpr(RegexItem.parse(x.getText())); }
    // | "(" r=expr ")"
    | r=parenExpr 
    ;
    
parenExpr returns [Expr r = null]
	{ ArrayList<BindingExpr> b = new ArrayList<BindingExpr>(); }
 	: "(" ( letDef[b] "," )*  r=expr ")"
 	{
 		if( ! b.isEmpty() )
 		{
	      for(int i = 0 ; i < b.size() ; i++ )
    	  {
      		BindingExpr e = b.get(i);
      		env.unscope(e.var);
	  	  }
      	  r = new LetExpr(b, r);
 		}
 	}
    ;


varExpr returns [VarExpr r = null]
    { String v; }
    : v=var 
    { 
      Var var = env.inscope(v);
      r = new VarExpr(var);
    }
    ;

arrayExpr returns [Expr r = null]
    { ArrayList<Expr> es; }
    : "[" es=exprList "]"    { r = new ArrayExpr(es); }
    ;

exprList returns [ArrayList<Expr> es = new ArrayList<Expr>()]
    { Expr e; }
    : (",")* ( e=expr { es.add(e); }
        ( (",") (e=expr  { es.add(e); })? )*
      )?
    ;
    

forExpr returns [Expr r = null]
	{ ArrayList<BindingExpr> b = new ArrayList<BindingExpr>(); }
    : "for" "(" forDef[b] ("," forDef[b])* ")" r=expr
    {
      for(int i = 0 ; i < b.size() ; i++ )
      {
      	BindingExpr e = b.get(i);
      	env.unscope(e.var);
	  }
      MultiForExpr f = new MultiForExpr(b, null, r); // TODO: eleminate WHERE
      r = f.expand(env);
    }
    ;

// forExprOld returns [Expr r = null]
// 	{ Expr p = null; Expr d; ArrayList<BindingExpr> b = new ArrayList<BindingExpr>(); }
//     : "for" forDef[b] ("," forDef[b])*
//       ( "where" p=expr )?
//       ( "return" d=expr	  { d = new ArrayExpr(d); }
//       | "collect" d=expr )
//     {
//       for(int i = 0 ; i < b.size() ; i++ )
//       {
//       	BindingExpr e = b.get(i);
//       	env.unscope(e.var);
// 	  }
//       MultiForExpr f = new MultiForExpr(b, p, d);
//       r = f.expand(env);
//     }
//     ;


forDef[ArrayList<BindingExpr> bindings]
	{ String v; Expr e; String v2 = null; BindingExpr.Type t = null; }
	: v=var ( ( "at" v2=var )?	"in" e=expr { t = BindingExpr.Type.IN; }
			| ":" v2=var "in" e=expr 		{ t = BindingExpr.Type.INREC; }
			// | "="  e=expr   	            { t = BindingExpr.Type.EQ; }
			)
	{ 
	  Var var = env.scope(v);
	  Var var2 = null;
	  if( v2 != null )
	  {
	  	var2 = env.scope(v2); 
	  }
	  bindings.add( new BindingExpr(t, var, var2, e) );
	}
	;

letExpr returns [Expr r = null]
	{ ArrayList<BindingExpr> b = new ArrayList<BindingExpr>(); }
    : "let" "(" letDef[b] ("," letDef[b])* ")" r=expr
    {
      for(int i = 0 ; i < b.size() ; i++ )
      {
      	BindingExpr e = b.get(i);
      	env.unscope(e.var);
	  }
      r = new LetExpr(b, r);
    }
    ;

// letExprOld returns [Expr r = null]
// 	{ ArrayList<BindingExpr> b = new ArrayList<BindingExpr>(); }
//     : "let" letDef[b] ("," letDef[b])*
//       "return" r=expr
//     {
//       for(int i = 0 ; i < b.size() ; i++ )
//       {
//       	BindingExpr e = b.get(i);
//       	env.unscope(e.var);
// 	  }
//       r = new LetExpr(b, r);
//     }
//     ;

letDef[ArrayList<BindingExpr> bindings]
	{ String v; Expr e; }
	: v=avar "="  e=expr
	{ 
	  Var var = env.scope(v);
	  bindings.add( new BindingExpr(BindingExpr.Type.EQ, var, null, e) );
	}
	;

ifExpr returns [Expr r = null]
    { Expr p; Expr t; Expr f = null; }
    : "if" "(" p=expr ")" t=expr
      ( options {greedy=true;} : 
        "else" f=expr )? 
      { r = new IfExpr(p,t,f); }
    ;

// ifExprOld returns [Expr r = null]
//     { Expr p; Expr t; Expr f = null; }
//     : "if" p=expr "then" t=expr
//       ( options {greedy=true;} : "else" f=expr )? 
//       { r = new IfExpr(p,t,f); }
//     ;

//unnestExpr returns [Expr r]
//    : "unnest" r=expr // TODO: eliminate unnest altogether
//      {
//		  r = new UnnestExpr(r);
//		}
//	  }
//    ;

//unnestExpr returns [Expr r]
//    : "unnest" r=expr // TODO: eliminate unnest altogether
//      {
      	// Var v = env.makeVar(Var.autoForName);
      	// r = new ForExpr(v, r, new VarExpr(v)); // TODO: no longer supports non-arrays??
//        // Some expressions can push unnest inside them for efficiency
//        if( r instanceof ForExpr )
//        {
//		  ((ForExpr)r).setUnnest(true);
//        }
//        //else if( r instanceof ArrayExpr )
//        //{
//		//  ((ArrayExpr)r).setUnnest(true);
//        //}
//		else
//		{      	
//		  r = new UnnestExpr(r);
//		}
//	  }
//    ;


// We might add ANY/ALL to join.  Right now the meaning is ALL non-optional
// bindings must be non-empty.  The alternative is that ANY non-optional binding
// must exist.  ANY can be achieved with the current syntax using multiple binary joins.
joinExpr returns [JoinExpr r = null]
	{ Expr e; ArrayList<BindingExpr> bs = new ArrayList<BindingExpr>(); }
	: "join" "(" joinDef[bs] "," (joinDef[bs]) ("," joinDef[bs])* ")"
	  {
	  	for(int i = 0 ; i < bs.size() ; i++ )
	  	{
	  	  BindingExpr b = bs.get(i);
	  	  b.var.hidden = false;
	  	}
	  }
	  e=expr
	  {
  	    for(int i = 0 ; i < bs.size() ; i++ )
	    {
	      BindingExpr b = bs.get(i);
	  	  env.unscope(b.var);
	    }
        r = new JoinExpr(bs, e);
	  }
	;

// We might add ANY/ALL to join.  Right now the meaning is ALL non-optional
// bindings must be non-empty.  The alternative is that ANY non-optional binding
// must exist.  ANY can be achieved with the current syntax using multiple binary joins.
// joinExprOld returns [JoinExpr r = null]
// 	{ Expr e; ArrayList<BindingExpr> bs = new ArrayList<BindingExpr>(); }
// 	: "join" joinDef[bs] "," (joinDef[bs]) ("," joinDef[bs])*
// 	  {
// 	  	for(int i = 0 ; i < bs.size() ; i++ )
// 	  	{
// 	  	  BindingExpr b = bs.get(i);
// 	  	  b.var.hidden = false;
// 	  	}
// 	  }
// 	  "return" e=expr
// 	{
//   	  for(int i = 0 ; i < bs.size() ; i++ )
// 	  {
// 	    BindingExpr b = bs.get(i);
// 	  	env.unscope(b.var);
// 	  }
//       r = new JoinExpr(bs, e);
// 	}
// 	;

joinDef[ArrayList<BindingExpr> bindings]
	{ String v1; Expr e1; Expr e2; Var var1 = null; 
      boolean opt = false; }
	: ( "optional"   { opt = true; } )?
	  v1=var "in" e1=expr 		{ var1 = env.scope(v1); }
	  "on" e2=expr
	{
	  var1.hidden = true;
	  BindingExpr b = new BindingExpr(BindingExpr.Type.IN, var1, null, e1, e2);
	  b.optional = opt;
	  bindings.add( b );
	}
	;

groupExpr returns [GroupByExpr r = null]
	{ Expr e; ArrayList<BindingExpr> bs = new ArrayList<BindingExpr>(); }
	: "group" "(" groupDef[bs] ("," (groupDef[bs])?)* ")"
	  {
	  	bs.get(0).var.hidden = false;
	  	for(int i = 1 ; i < bs.size() ; i++ )
	  	{
	  	  BindingExpr b = bs.get(i);
	  	  b.var2.hidden = false;
	  	}
	  }
	  e=expr
	  {
        env.unscope(bs.get(0).var);
  	    for(int i = 1 ; i < bs.size() ; i++ )
	    {
          BindingExpr b = bs.get(i);
	  	  env.unscope(b.var2);
	    }
        r = new GroupByExpr(bs, e);
	  }
	;

// groupExprOld returns [GroupByExpr r = null]
// 	{ Expr e; ArrayList<BindingExpr> bs = new ArrayList<BindingExpr>(); }
// 	: "group" groupDef[bs] ("," (groupDef[bs])?)*
// 	  {
// 	  	bs.get(0).var.hidden = false;
// 	  	for(int i = 1 ; i < bs.size() ; i++ )
// 	  	{
// 	  	  BindingExpr b = bs.get(i);
// 	  	  b.var2.hidden = false;
// 	  	}
// 	  }
// 	  ( "return"  e=expr   { e = new ArrayExpr(e); }
// 	  | "collect" e=expr )
// 	{
//       env.unscope(bs.get(0).var);
//   	  for(int i = 1 ; i < bs.size() ; i++ )
// 	  {
// 	    BindingExpr b = bs.get(i);
// 	  	env.unscope(b.var2);
// 	  }
//       r = new GroupByExpr(bs, e);
// 	}
// 	;

// TODO: the groups should be able to be sorted at the same time, so asc/desc/unordered and multi-column...??
groupDef[ArrayList<BindingExpr> bindings]
	{ String v1, v2, v3; Expr e1, e2; Var inVar = null; }
	: v1=var "in" e1=expr 		{ inVar = env.scope(v1); }
	  "by" v2=avar "=" e2=expr
	  "into" v3=var
	{
      env.unscope(inVar);
	  if( bindings.size() == 0 )
	  {
	    Var byVar = env.scope(v2);
	    bindings.add( new BindingExpr(BindingExpr.Type.EQ, byVar, null, e2) );
	    byVar.hidden = true;
	  }
	  else
	  {
	  	BindingExpr byBinding = bindings.get(0);
	  	if( ! byBinding.var.name.equals(v2) )
	  	{
	  	  throw new RuntimeException("all by expressions must use the same variable: " +
                                     v2 + " != " + byBinding.var.name );
	  	}
	    bindings.get(0).addChild( e2 );
	  }
	  Var intoVar = env.scope(v3);
	  bindings.add( new BindingExpr(BindingExpr.Type.IN, inVar, intoVar, e1) );
	  intoVar.hidden = true;
	}
	;
	
combineExpr returns [Expr r = null]
	{ String v1, v2; Var var1 = null; Var var2 = null; Expr in; Expr use; }
	: "combine" "(" v1=var "," v2=var "in" in=expr ")"
         { 
	       var1 = env.scope(v1);
	       var2 = env.scope(v2);
	     }
	  use=expr
	     {
	       env.unscope(var1);
	       env.unscope(var2);
    	   r = new CombineExpr(var1, var2, in, use);
	     }
//	  ( options {greedy=true;} : "when" "empty" empty=expr ) ?
//	  { 
//	    r = new CombineExpr(var1, var2, in, use, empty);
//	  }
	;
	
reduceExpr returns [ReduceExpr r = null]
	{ 
	  String v; Expr inExpr; Expr ret; Var inVar = null; int n = 0;
	  BindingExpr a;
	  ArrayList<BindingExpr> aggs = new ArrayList<BindingExpr>(); 
	}
 	: "reduce" "(" v=var "in" inExpr=expr
 	     { 
 	     	inVar = env.scope(v);
 	     } 
 	  "into" a=agg     { aggs.add(a); }
 	  ( "," a=agg      { aggs.add(a); } )*
 	     {
 	     	env.unscope(inVar);
 	     	n = aggs.size();
 	     	for(int i = 0 ; i < n ; i++)
 	     	{
 	     		aggs.get(i).var.hidden = false;
 	     	}
 	     }
 	  ")" ret=expr
 	      {
 	     	for(int i = 0 ; i < n ; i++)
 	     	{
	 	        env.unscope( aggs.get(i).var );
 	     	}
 	     	r = new ReduceExpr(inVar, inExpr, aggs, ret);
 	     }
 	;

agg returns [BindingExpr b=null]
 	{ String v; Var var=null; String i; Expr e; }
 	: v=avar "=" e=expr
      {  
		var = env.scope(v);
		var.hidden = true;
		b = new BindingExpr(BindingExpr.Type.AGGFN, var, null, e);
	  }
	;
   
// agg returns [BindingExpr b=null]
// 	{ String v; Var var=null; String i; Expr e; }
// 	: v=var "=" 
//    	{  
//			var = env.scope(v);
//			var.hidden = true;
//		}
// 	  ( i=id "(" e=expr ")"
// 		{  
//			e = FunctionLib.lookup(env, i, e);
//			b = new BindingExpr(BindingExpr.Type.AGGFN, var, null, e);
//		}
//	  | e=combineExpr
// 		{  
//			b = new BindingExpr(BindingExpr.Type.AGGFN, var, null, e);
//		}
//	  )
//	;
   
sortExpr returns [SortExpr s = null]
	{ 
	  String v;
	  Expr e;
	  Var var = null;
	  BindingExpr b = null;
	  ArrayList<OrderExpr> by = new ArrayList<OrderExpr>();
	}
	: "sort" "(" v=var "in" e=expr	
	  { 
	  	var = env.scope(v);
	  	b = new BindingExpr(BindingExpr.Type.IN, var, null, e);
	  }
	  "by" sortSpec[by] ("," sortSpec[by])* ")"
	  { 
	  	env.unscope(var);
	  	s = new SortExpr(b, by);
	  }
	;
	

// sortExprOld returns [SortExpr s = null]
// 	{ 
// 	  // TODO: i don't like the () around the by list. They were needed to avoid an ambiguity. 
// 	  String v;
// 	  Expr e;
// 	  Var var = null;
// 	  BindingExpr b = null;
// 	  ArrayList<OrderExpr> by = new ArrayList<OrderExpr>();
// 	}
// 	: "sort" v=var "in" e=expr	
// 	  { 
// 	  	var = env.scope(v);
// 	  	b = new BindingExpr(BindingExpr.Type.IN, var, null, e);
// 	  }
// 	  "by" "(" sortSpec[by] ("," sortSpec[by])* ")"
// 	  { 
// 	  	env.unscope(var);
// 	  	s = new SortExpr(b, by);
// 	  }
// 	;
	

sortSpec[ArrayList<OrderExpr> by]
	{ Expr e; OrderExpr.Order order = OrderExpr.Order.ASC; }
	: e=expr ( options {greedy=true;}
			 : "asc" 
	         | "desc"  { order = OrderExpr.Order.DESC; }
	         )?
	  { by.add( new OrderExpr(e, order) ); }
	;
	

var returns [String s = null]
    : v:VAR  { s = v.getText(); }
    ;

avar returns [String s = null]
    : v:AVAR  { s = v.getText(); }
    ;

id returns [String s = null]
    : i:ID   { s = i.getText(); }
    ;

typeExpr returns [Expr r = null]
	{ Schema s; }
	: "type" s=type   { r = new ConstExpr(new JSchema(s)); }
	;

type returns [Schema s = null]
	{ Schema s2 = null; SchemaOr os = null; }
	: s = typeTerm 
	( "|"              { s2 = s; s = os = new SchemaOr(); os.addSchema(s2); }
	  s2 = typeTerm    { os.addSchema(s2); } 
	)*
	;
	
typeTerm returns [Schema s = null]
	: "*"			{ s = new SchemaAny(); }
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
        ( p=type		  { head = p; }
	      ( "," q=type	  { p = p.nextSchema = q; }
	      )*
	      arrayRepeat[head,s]
	    )?
	  "]"
	;

arrayRepeat[Schema typeList, SchemaArray s]
	{ long lo = 0; long hi = 0; }
	: /**/				  
	  { s.noRepeat(typeList); }
	| "<" 
	    ( "*"			  { lo = 0; hi = SchemaArray.UNLIMITED; }
	    | i1:INT          { lo = Long.parseLong(i1.getText()); }
		  ( /**/          { hi = lo; }
		  | "," ( "*"	  { hi = SchemaArray.UNLIMITED; }
		        | i2:INT  { hi = Long.parseLong(i2.getText()); }
		        )
		  )
		)
	  ">"
	  { s.setRepeat(typeList, lo, hi); }
	;

recordType returns [SchemaRecord s = new SchemaRecord()]
	{ SchemaField f; }
	: "{"
	    ( f=fieldType		 { s.addField(f); }
	      ( "," f=fieldType	 { s.addField(f); }
	      )*
	    )?
	  "}"
	;

fieldType returns [SchemaField f = new SchemaField()]
    { String n; Schema t; }
	: ( n=constFieldName    { f.name = new JString(n); }
         ( "*"				{ f.wildcard = true; }
	     | "?"				{ f.optional = true; }
	     )?
	  | "*"               { f.name = new JString(""); 
	                        f.wildcard = true;     }
	  )
	  ":" t=type		{ f.schema = t; }
	;
	

class JaqlLexer extends Lexer;

options {
  charVocabulary = '\3'..'\377'; // all characters except special ANTLR ones
  testLiterals=false;    // don't automatically test for literals
  k=3;                   // lookahead // TODO: try to reduce to 2
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
    
ML_COMMENT
	:	"/*"
		( ~('*'|'\n'|'\r')
        | '*' ~('/')
        | ( options { generateAmbigWarnings=false; }
          : "\r\n"
          | '\r'
          | '\n'
          ) { newline(); }
		)*
		"*/"
		{$setType(Token.SKIP);}
	;

protected VAR1
	: '$' ('_'|LETTER|DIGIT)*
    ;

VAR
	options { ignore=WS; }
	: (VAR1 '=' ('='|'>')) => VAR1
	| (VAR1 '=') => VAR1 {$setType(AVAR);}
	| VAR1 
    ;

SYM
    options { testLiterals=true; }
    : '(' | ')' | '[' | ']' | ',' | '|'
    | '}' 
    | '=' ('=')? | ('<' | '>' ) ('=')? | "!="
    | '/' | '*' | '+' | '-'
    | ':' (':'|'=')? | '?'
    ;

SYM2
    options { testLiterals=true; }
    : '{' (('=' | (('<' | '>') ('=')?) | "!=") '}')?
    ;

SEMI: ';' ;

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
    options { testLiterals=true; }
 	: (DEC) => 
 	     DEC   		
 	       ( /*empty*/      {$setType(DEC);} // TODO: flag to control default decimal/double
 	       | "m"!           {$setType(DEC);}
 	       | "d"!           {$setType(DOUBLE);}
 	       )
    |  INT
        ( /*empty*/    		{$setType(INT);} // TODO: flag to control default decimal/double
        | "m"!              {$setType(INT);}
        | "d"!              {$setType(DOUBLE);}
        )
    | '0'('x'|'X')(HEX)+	{$setType(INT);}
    | '.'
    | (DOT_ID) => DOT_ID    {$setType(DOT_ID);}
    ;

protected IDWORD
    : ('@'|'_'|LETTER) ('@'|'_'|'#'|LETTER|DIGIT)*
    ;

ID
	options { ignore=WS; }
	: (IDWORD ('*'|'?')? ':') => IDWORD
	| IDWORD { _ttype = testLiteralsTable(_ttype); }
	;

protected DOT_ID
	: '.'! IDWORD
	;
	    
//DOT_OR_DEC
//    options { testLiterals=true; }
//	: '.' (
//	        INT ( ('e'|'E') ('+' | '-')? INT )? { $setType(DEC); }
//	      )?
//   ;
		      
DATETIME
    : ('d'!|'D'!) '\"'! (~('\"'))* '\"'!
    | ('d'!|'D'!) '\''! (~('\''))* '\''!
    ;
    
