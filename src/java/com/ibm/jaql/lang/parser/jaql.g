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
import com.ibm.jaql.lang.expr.function.*;
import com.ibm.jaql.lang.expr.top.*;
import com.ibm.jaql.lang.expr.path.*;
import com.ibm.jaql.lang.expr.schema.*;

import com.ibm.jaql.lang.expr.record.IsdefinedExpr;
import com.ibm.jaql.lang.expr.nil.IsnullExpr;
import com.ibm.jaql.lang.expr.agg.Aggregate;
import com.ibm.jaql.lang.expr.agg.AlgebraicAggregate;

import com.ibm.jaql.json.type.*;
import com.ibm.jaql.json.schema.*;

import com.ibm.jaql.util.BaseUtil;
}

options {
  language="Java";
}


class JaqlParser extends Parser;
options {
    k = 1;                           // number of tokens to lookahead
    exportVocab=Jaql;                // Call its vocabulary "Jaql"
    defaultErrorHandler = false;     // Don't generate parser error handlers
}

{
    public boolean done = false;
    public Env env = new Env(new Context()); // TODO: The Env should be passed in to the parser constructor
    
    protected void oops(String msg) throws RecognitionException, TokenStreamException
    { 
      throw new RecognitionException(msg, getFilename(), LT(1).getColumn(), LT(1).getLine()); 
    }
    
//    private Expr inlineGlobalFinalValueVar(Expr e)
//    {
//      // automatically inline global variables that are final and have a value
//      // true for all builtin functions
//      if (e instanceof VarExpr && e.isCompileTimeComputable().always())
//      {
//        Var v = ((VarExpr)e).var();
//        if (v.isGlobal() && v.isFinal() && v.type() == Var.Type.VALUE)
//        {
//          try
//          {
//            e = new ConstExpr(env.eval(e));
//          } 
//          catch (Exception ex)
//          {
//            throw com.ibm.jaql.lang.util.JaqlUtil.rethrow(ex);
//          } 
//        }
//      }
//      return e;
//    }
    
    // checks whether the next token matches the specified weak keyword
    private boolean nextIsWeakKw(String kw) throws TokenStreamException
    {
        return !env.isDefinedLocal(kw) && kw.equals(LT(1).getText());
    }
    
    private Expr assignGlobal(String name, Schema schema, boolean isValue, Expr expr)
       throws RecognitionException, TokenStreamException
    {
      Var var = env.findGlobal(name);
      if( var != null && var.isMutable() ) 
      {
      	// replace value of extern variable
        if( schema != null )
        {
          oops("cannot change schema of extern variable: "+name);
        }
        if( ! isValue )
        {
          oops("must use value assignment for extern variable: "+name+" := ...");
        }
        expr = new AssignExpr(env, var, expr);
      }
      else 
      {
        // make new non-extern variable
        if( schema == null ) schema = expr.getSchema();
        if( isValue )
        {
          var = env.scopeGlobalVal(name, schema);
          expr = new AssignExpr(env, var, expr);
        }
        else
        {
          var = env.scopeGlobalExpr(name, schema, expr);
          expr = null;
        }
      }
      return expr;
    } 

    private Expr materializeGlobal(String name, Schema schema, Expr expr)
       throws RecognitionException, TokenStreamException
    {
      Var var = env.findGlobal(name);
      if( var != null && var.isMutable() ) 
      {
        oops("cannot materialize an extern variable: "+name);
      }
      if( expr == null )
      {
        var = env.inscopeGlobal(name);
        if( var.type() == Var.Type.EXPR ) 
        {
          expr = new AssignExpr(env, var, var.undefineExpr());
        }
        else  // just ignore materialize request - a warning might be nice...
        {
          expr = null;
        }
      }
      else
      {
      	if( schema == null ) schema = SchemaFactory.anySchema();
        var = env.scopeGlobalExpr(name, schema, expr);
        expr = new AssignExpr(env, var, expr);
      }
      return expr;
    } 
     
    private Expr makeExternVar(String name, Schema schema, Expr expr)
       throws RecognitionException, TokenStreamException
    {
      Var var = env.findGlobal(name);
      if( var != null )  // variable already in scope
      {
        if( schema == null  ) 
        {
          if( var.isMutable() )
          {
            // preserve existing schema of previous extern declaration
            schema = var.getSchema();
          }
          else
          {
            // allow any value
            schema = SchemaFactory.anySchema();
          }
        }
        
        if( ! var.isMutable() ) 
        {
          // shadow an exising non-extern
          // preserved existing value
          // if( expr != null ) warn extern default ignored ?
          Var var2 = env.scopeGlobalMutable(name, schema);
          expr = new AssignExpr(env, var2, new VarExpr(var) );
        }
        else
        {
          // redeclare an existing extern
          Schema oldSchema = var.getSchema();
          if( oldSchema == schema ||
              oldSchema.equals(schema) ||
              oldSchema.equals(SchemaFactory.anySchema()) )
          {
          	// TODO: allow schema that is a restriction over the old declaration
            if( var.isDefined() )
            {
              JsonValue val;
              try { 
              	val = var.getValue(null); // null ok because this is a extern value variable
              } catch( Exception e ) {
              	throw new RuntimeException(e);
              }
              if( ! schema.matchesUnsafe(val) )
              {
                oops( "cannot change schema of extern variable "+name+" to "+schema+" because value is "+val);
              }
              // preserved existing value
              // if( expr != null ) warn extern default ignored ?
              // set expr null - no further action
              expr = null;
            }
            var.setSchema( schema );
            if( expr != null )
            {
              expr = new AssignExpr(env, var, expr);
            }
          }
          else
          {
            oops( "cannot change schema of extern variable "+name+" from "+oldSchema+" to "+schema);
          }
        }
      }
      else // new name
      {
        if( schema == null ) schema = SchemaFactory.anySchema();
        var = env.scopeGlobalMutable(name, schema);
        if( expr != null )
        {
          expr = new AssignExpr(env, var, expr);
        }
        // else expr stays null - no further action
      }
      
      return expr;
    } 
    
    private Expr addAnnotations(Expr annotations, Expr expr)
      throws RecognitionException, TokenStreamException
    {
      return new AnnotationExpr(annotations,expr);
      /*
      if( !annotations.isCompileTimeComputable().always() )
      {
        oops("annotations must be compile-time computable: "+annotations);
      }
      try
      {
        JsonValue val = env.eval(annotations);
        expr.addAnnotations((JsonRecord)val);
      }
      catch( Exception e )
      {
      	oops( e.getMessage() );
      }
      return expr;
      */
    }

}

// -- main productions ----------------------------------------------------------------------------

// top-level rule
parse returns [Expr r=null]
    { Expr s; }
    : s=stmt { r = env.postParse(s); } ( SEMI | EOF )  
    | SEMI
    | EOF { done = true; }
    ;

// a statement without statement delimiter
protected stmt returns [Expr r=null]
    : (kwImport id)       => r=importExpr
    | (kwQuit)            => kwQuit { r = null; done = true; }
    | (kwExplain)         => kwExplain r=topAssign  { r = r == null ? null : new ExplainExpr(env,r); }
    | (kwQuit)            => kwQuit { r = null; done = true; }
    | r=topAssign
    ;

// namespace import
importExpr returns [Expr r=null]
    { String v; String var; 
      ArrayList<String> names = new ArrayList<String>();}
    : kwImport v=id (
          { env.importNamespace(Namespace.get(v)); } 
        | "(" (
            "*" ")" {
                env.importNamespace(Namespace.get(v));
                env.importAllFrom(Namespace.get(v));
            }
            | var=id {names.add(var);} ("," var=id {names.add(var);})* ")" {
                env.importNamespace(Namespace.get(v));
                env.importFrom(Namespace.get(v), names);
            }
        )
    )
    ;
    
    
// top level assignment, creates global variables and inlines referenced globals
topAssign returns [Expr r=null]
    { String n; Schema s = null; boolean isVal = false; }
    : (id ("=" | ":=" | ":")) => 
          n=id (":" s=schema)? ( "=" | ":=" {isVal=true;} ) r=rvalue 
            { r = assignGlobal(n,s,isVal,r); }
    | (kwMaterialize) => kwMaterialize n=id ((":" s=schema)? ("=" | ":=") r=rvalue)?
            { r=materializeGlobal(n,s,r); }
    | (kwExtern) => kwExtern n=id (":" s=schema)? (":=" r=rvalue)?
            { r=makeExternVar(n,s,r); }
    | (kwRegisterFunction) => r=registerFunction  // TODO: deprecated: remove; use javaudf instead
    | r=pipe
    ;


// local assignment, creates local variables
assign returns [Expr r=null]
    { String v; }
    : (id "=") => v=id "=" r=rvalue  
                  { r = new BindingExpr(BindingExpr.Type.EQ, env.scope(v, r.getSchema()), null, r); } 
                | r=pipe  
    ;

// expression that can appear on the right-hand side of an assignment    
rvalue returns [Expr r = null]
    // : ( kwExtern id ) => r=extern
    : r=pipe
    ;

// external function NYI
//extern returns [Expr r = null]
//    { String lang; }
//    : kwExtern lang=id kwFn r=expr
//      { r = new ExternFunctionExpr(lang, r); }
//    ;

// a pipe
pipe returns [Expr r=null]
    : r=expr r=subpipe[r]
    | r=fn
    | r=pipeFn
    // | r=sqlTableExpr
    ;
    
// a subpipe
subpipe[Expr e] returns [Expr r=e]
    : ( "->" ) => "->" r=op[r] r=subpipe[r]
                | ()
    ;

// -- expressions ---------------------------------------------------------------------------------

// an expression that can occur at the beginning of a pipe   
expr returns [Expr r]
    { Expr a; }
    : (kwGroup)    => r=group
    | (kwJoin)     => r=join
    | (kwEquijoin) => r=equijoin
    | (kwIf)       => r=ifExpr
    | (kwFor)      => r=forExpr
    | (kwWhile)    => r=whileExpr
    | (kwUnroll)   => r=unroll
    | r=orExpr
    | "@" a=record r=expr { r=addAnnotations(a,r); }
//  | r=combineExpr
    ;
    
orExpr returns [Expr r]
    { Expr s; }
    : r=andExpr ( kwOr s=andExpr { r = new OrExpr(r,s); } )*
    ;

andExpr returns [Expr r]
    { Expr s; }
    : r=notExpr ( kwAnd s=notExpr { r = new AndExpr(r,s); } )*
    ;

notExpr returns [Expr r]
    : kwNot r=notExpr  { r = new NotExpr(r); }
    | (kwIsnull)    => r=isnullExpr
    | (kwIsdefined) => r=isdefinedExpr
    | r=inExpr
    ;

isnullExpr returns [Expr r]
    : kwIsnull r=inExpr
    { r = new IsnullExpr(r); }
    ;

isdefinedExpr returns [Expr r]
    { Expr n; }
    : kwIsdefined r=call n=projName // TODO: this should be a path expression
    { r = new IsdefinedExpr(r,n); }
    ;
    
inExpr returns [Expr r = null]
    { Expr s; }
    : r=compareExpr ( kwIn s=compareExpr  { r = new InExpr(r,s); } )?
    ;

compareExpr returns [Expr r = null]
    { int c; Expr s; Expr t; }
    : r=instanceofExpr 
          ( c=compareOp  s=instanceofExpr  { r = new CompareExpr(c,r,s); }
               ( c=compareOp       { s = s.clone(new VarMap()); }  // TODO: introduce a variable?
                 t=instanceofExpr  { r = new AndExpr( r, new CompareExpr(c,s,t) ); s=t; } 
               )*
          )? 
    ;

// infix comparison operators
compareOp returns [int r = -1]
    : "==" { r = CompareExpr.EQ; }
    | "<"  { r = CompareExpr.LT; }
    | ">"  { r = CompareExpr.GT; }
    | "!=" { r = CompareExpr.NE; }
    | "<=" { r = CompareExpr.LE; }
    | ">=" { r = CompareExpr.GE; }
    ;

// TODO: add astype operator?
// TODO: replace instanceof is istype?
instanceofExpr returns [Expr r]
    { Expr s; }
    : r=addExpr 
      ( kwInstanceof s=addExpr { r = new InstanceOfExpr(r,s); } )? 
    ;

addExpr returns [Expr r]
    { Expr s; int op; }
    : r=multExpr (op=addOp s=multExpr  { r = new MathExpr(op,r,s); })*
    ;

// infix additive operators
addOp returns [int op=0]
    : "+" { op=MathExpr.PLUS; }
    | "-" { op=MathExpr.MINUS; }
    ;

multExpr returns [Expr r]
    { Expr s; int op; }
    : r=unaryAdd ( op=multOp s=unaryAdd  { r = new MathExpr(op,r,s); } )*
    ;

// infix multiplicative operators
multOp returns [int op=0]
    : "*" { op=MathExpr.MULTIPLY; }
    | "/" { op=MathExpr.DIVIDE; }
    ;

// TODO: there is a bug handling negative numbers minLong (= -maxLong -1) doesn't parse
unaryAdd returns [Expr r]
    : "-" r=typeExpr { r = MathExpr.negate(r); } 
    | ("+")? r=typeExpr 
    ;
    
typeExpr returns [Expr r=null]
    { Schema s; }
    : (kwSchema (id|kwNull|"["|"{")) => kwSchema s=schema   { r = new ConstExpr(new JsonSchema(s)); }
    | r=path
    ;   


forExpr returns [Expr r = null]
    { 
        ArrayList<BindingExpr> bs = new ArrayList<BindingExpr>();
    }
    : kwFor "(" forDef[bs] ("," forDef[bs])* ")" r=expr
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
    { String v; Expr e; BindingExpr.Type t = null; }
//    : b=vpipe
//    {
//      bindings.add(b);
//    }
    : v=id ( /*( "at" v2=var )?*/  kwIn e=pipe { t = BindingExpr.Type.IN; }
            // | ":" v2=var "in" e=pipe        { t = BindingExpr.Type.INREC; }
            // | "="  e=expr                { t = BindingExpr.Type.EQ; }
            )
    { 
      //Var var = env.scope(v, e.getSchema().elements());
      Var var = env.scope(v);
//      Var var2 = null;
//      if( v2 != null )
//      {
//        var2 = env.scope(v2); 
//      }
      bindings.add( new BindingExpr(t, var, null, e) );
    }
    ;

whileExpr returns [Expr r = null]
    { String v; Expr i; Expr c; Var var=null; }
    : kwWhile "(" 
           v=id "=" i=pipe ","   { var = env.scope(v); }
           c=pipe ")" 
           r=expr
        {  
        	env.unscope(var);
        	r = new WhileExpr(new BindingExpr(BindingExpr.Type.EQ, var, null, i), c, r); 
        }
    ;

ifExpr returns [Expr r=null]
    { Expr p=null; Expr s=null; }
    : kwIf "(" p=expr ")" r=expr 
      ( options {greedy=true;} : 
        kwElse s=expr )?
    {
        r = new IfExpr(p, r, s);
    }
    ;

// -- path expressions ----------------------------------------------------------------------------

// path expression
path returns [Expr r]
    { PathStep s=null; }
    : r=call
      ( ( step ) => s=step { r = new PathExpr(r,s); }
                    additionalSteps[s]
                  | ()
      )
    ;

// steps of a path exression that follow another step
additionalSteps[PathStep p]
    { PathStep s; }
    : ( step ) => s=step { p.setNext(s); p = s; } additionalSteps[p] 
                | ()
    ;

// a step of a path expression
step returns [PathStep r = null]
    { Expr e; Expr f; ArrayList<PathStep> pn; }
    : ( e=projName                   { r = new PathFieldValue(e); }
      | "{" pn=projFields "}"        { r = new PathRecord(pn); } // TODO: all steps after names: {.x.y, .z[2]}
      | "[" ( e=expr
                ( /*empty*/       { r = new PathIndex(e); }
                | ":" ( f=expr    { r = new PathArraySlice(e,f); }
                      | "*"       { r = new PathArrayTail(e); }
              ))
            | "*" ( /*empty*/     { r = new PathArrayAll(); }
                  | ":" ( e=expr  { r = new PathArrayHead(e); }
                        | "*"     { r = new PathArrayAll(); }
                ))
            | "?"                 { r = new PathToArray(); }
            | /*empty*/           { r = new PathExpand(); }
          ) // ( "*" { ((PathArray)r).setExpand(true); } )? // TODO: add shorthand to expand?
        "]"
      )
    ;

// path exression that projects field of a record
projFields returns [ArrayList<PathStep> names = new ArrayList<PathStep>()]
    { PathStep s; }
    : s=projField additionalSteps[s]             { names.add(s); } // TODO: ? indicator to eliminate nulls
      ( projFieldsMore[names] )?
    | s=projNotFields additionalSteps[s]         { names.add(s); }
    ;

projFieldsMore[ArrayList<PathStep> names]
    { PathStep s; }
    : "," ( s=projField additionalSteps[s]       { names.add(s); } // TODO: ? indicator to eliminate nulls
            ( projFieldsMore[names] )?
          | s=projNotFields additionalSteps[s]   { names.add(s); }
          )
    ;
    
projNotFields returns [PathStep s=null]
    { ArrayList<PathStep> names = null; }
    : "*"
      ( /* empty */       { s = new PathAllFields(); }
      | "-" s=projField   { names = new ArrayList<PathStep>(); names.add(s); }
        ( "," s=projField { names.add(s); }
        )*
                          { s = new PathNotFields(names); } 
      )
    ;

projField returns [PathOneField r=null]
    { Expr e; }
    : e=projName  { r = new PathOneField(e); } 
    ;

projName returns [Expr r=null]
    : r=dotName
    | DOT r=basic
    ;

dotName returns [Expr r = null]
    : i:DOT_ID     { r = new ConstExpr(new JsonString(i.getText())); }
    ;

    
basic returns [Expr r=null]
    : r=constant
    | r=record
    | r=array
    | ( kwBuiltin ) => r=builtinFunction
    | r=varExpr
    | r=cmpExpr
    | r=parenExpr
    ;

// -- blocks -------------------------------------------------------------------------------------- 
    
// a block including surrounding parens
parenExpr returns [Expr r=null]
    : "(" r=block ")"
    ;

// a block with local variables
block returns [Expr r=null] 
    { ArrayList<Expr> es=null; }
    : r=assign
      ("," { es = new ArrayList<Expr>(); es.add(r); }
         r=assign       { es.add(r); }
        ("," r=assign   { es.add(r); } )*
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
      {
        if( r instanceof BindingExpr )
        {
          r = new DoExpr(r);
        }
      }
    ;
    

// -- special forms in pipes ----------------------------------------------------------------------

// expression that can occur after a ->
op[Expr in] returns [Expr r=null]
    { BindingExpr b=null; Expr a; } 
    : (kwFilter)       => kwFilter b=each[in] r=expr     { r = new FilterExpr(b, r);    env.unscope(b.var); }
    | (kwTransform)    => kwTransform b=each[in] r=expr  { r = new TransformExpr(b, r); env.unscope(b.var); }
    | (kwExpand)       => kwExpand b=each[in] 
                          ( r=expr | /*empty*/ { r = new VarExpr(b.var); } )
                          { r = new ForExpr(b, r); env.unscope(b.var); }
    | (kwGroup)        => r=groupPipe[in]
    | (kwSort kw)      => r=sort[in]
    | (kwTop)          => r=top[in]
    | (kwAggregate kw) => r=aggregate[in]
    | (kwSplit)        => r=split[in]
    | "@" a=record r=op[in] { r=addAnnotations(a,r); } 
    | r=callPipe[in]
    // | r=partition[in]
    // TODO: add rename path
    // | r=window[in]
    ;

// variable binding to array elements    
each[Expr in] returns [BindingExpr b=null]
    { String v = "$"; }
    : ( (kwEach id) => kwEach v=id
      | ()
      )
    { 
      b = new BindingExpr(BindingExpr.Type.IN, env.scope(v), null, in);
    }
    ;
    
// top-n elements
top[Expr in] returns [Expr r=null]
    { Expr n; Expr by=null; }
    : kwTop n=expr (by=sortCmp)?
      {
        // TODO: add heap-based top operator
        if( by != null )
        {
          in = new SortExpr(in, by);
        }
        r = new PathExpr(in, new PathArrayHead(new MathExpr(MathExpr.MINUS, n, new ConstExpr(JsonLong.ONE))));
      }
    ;
    
// unroll 
unroll returns [Expr r=null]
    { ArrayList<Expr> args = new ArrayList<Expr>(); }
    : kwUnroll r=call  { args.add(r); }
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
    

// -- literals ------------------------------------------------------------------------------------

constant returns [Expr r=null]
    { String s; JsonNumber n; JsonBool b;}
    : s=str        { r = new ConstExpr(new JsonString(s)); }
    | n=numberLit { r = new ConstExpr(n); }
    | h:HEXSTR     { r = new ConstExpr(new JsonBinary(h.getText())); }
    | t:DATETIME   { r = new ConstExpr(new JsonDate(t.getText())); }
    | b=boolLit    { r = new ConstExpr(b); }
    | r=nullExpr
    ;

strLit returns [ JsonString v=null]
    { String s; }
    : s=str      { v = new JsonString(s); }
    ;
    
str returns [String r=null]
    : s:SQSTR           { r = s.getText(); }
    | d:DQSTR           { r = d.getText(); }
    | h:HERE_STRING     { r = h.getText(); }
    ;

numberLit returns [JsonNumber v=null]
    : v=longLit
    | v=doubleLit
    | v=decfloatLit
    ;

longLit returns [ JsonLong v=null]
    : i:INT      { v = new JsonLong(i.getText()); }
    ;

doubleLit returns [ JsonDouble v=null]
    : d:DOUBLE   { v = new JsonDouble(d.getText()); }
    ;

decfloatLit returns [ JsonDecimal v=null]
    : n:DEC      { v = new JsonDecimal(n.getText()); }
    ;

boolLit returns [JsonBool b=null]
    : kwTrue   { b = JsonBool.TRUE; }
    | kwFalse  { b = JsonBool.FALSE; }
    ;
    
nullExpr returns [Expr r=null]
    : kwNull   { r = new ConstExpr(null); }
    ;
    
record returns [Expr r = null]
    { ArrayList<FieldExpr> args = new ArrayList<FieldExpr>();
      FieldExpr f; }
    : "{" ( f=field  { args.add(f); } )? ( "," ( f=field  { args.add(f); } )? )*  "}"
    //    { r = new RecordExpr(args.toArray(new Expr[args.size()])); }
    { r = RecordExpr.make(env, args.toArray(new Expr[args.size()])); }
    ;

field returns [FieldExpr f=null]  // TODO: lexer ID "(" => FN_NAME | keyword ?
    { Expr e = null; Expr v=null; boolean required = true; }
    : (idExpr ("?")? ":") => e=idExpr ( "?" { required = false; } )?  v=fieldValue  
      { 
        f = new NameValueBinding(e, v, required); 
      }
    | e=path ( DOT_STAR    
               { 
                 f = new CopyRecord(e);
               }
             | ( "?" { required = false; } )?
               ( ":" v=pipe )?
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


fieldValue returns [Expr r=null]
    { boolean flat = false; }
    : ":" ( (kwFlatten) => kwFlatten {flat=true;}
          | /* empty */ 
          ) r=pipe
      {
        if( flat )
        {
          r = new FlattenExpr(r);
        }
      }
    ;
    
array returns [Expr r=null]
    { ArrayList<Expr> a; }
    : "[" a=exprListOrEmpty "]"
    { r = new ArrayExpr(a); }
    ;

exprListOrEmpty returns [ArrayList<Expr> r = new ArrayList<Expr>()]
    { Expr e; }
    : ( e=pipe  { r.add(e); } )? ("," ( e=pipe  { r.add(e); } )? )*
    ;
    
    
// -- split ---------------------------------------------------------------------------------------

split[Expr in] returns [Expr r=null]
    { 
      ArrayList<Expr> es = new ArrayList<Expr>();
      Expr e;
      BindingExpr b;
    }
//    : "split" b=each[in] "("  { es.add( b ); } 
//         ( "if"  p=expr e=subpipe[b.var]  { es.add(new IfExpr(p,e)); } )*
//         ( "else" e=subpipe[b.var]        { es.add(new IfExpr(new ConstExpr(JsonBool.trueItem), e) ); } )?
//       ")"
    : kwSplit b=each[in]     { es.add( b ); } 
         splitIfs[b, es]
         ( kwElse            { b.var.setHidden(true); } 
                e=expr       { es.add(new IfExpr(new ConstExpr(JsonBool.TRUE), e) ); } )?
      {
        r = new SplitExpr(es);
        env.unscope(b.var);
      }
    ;

splitIfs[BindingExpr b, ArrayList<Expr> es]
    : ( splitIf[b,es] ) => splitIf[b, es] splitIfs[b,es]
                         | ()
    ;

splitIf[BindingExpr b, ArrayList<Expr> es]
    { Expr p, e; }
    : kwIf              { b.var.setHidden(false); } 
       "(" p=expr ")"   { b.var.setHidden(true); }
       e=expr           { es.add(new IfExpr(p,e)); }
       ;


// -- comparators ---------------------------------------------------------------------------------

comparator returns [Expr r=null]
    : r=cmpArrayFn["$"]
//  | "default"      { r = new DefaultComparatorExpr(); }
//  | (s=name|s=str)     { oops("named comparators NYI: "+s); } // r=new InvokeBuiltinCmp(ctx, e);
    | r=cmpFnExpr
    | r=varExpr
    | r=parenExpr 
    ;

cmpExpr returns [Expr r=null]
    { String v; }
    : kwCmp ( "(" v=id ")" r=cmpArrayFn[v]
            | r=cmpArray )
    ;

cmpFnExpr returns [Expr r=null]
    { String v; }
    : kwCmp "(" v=id ")" r=cmpArrayFn[v]
    ;

cmpArrayFn[String vn] returns [Expr r=null]
    { Var var = vn==null ? null : env.scope(vn); } // vn is null when parser is guessing, so var not used.
    : r=cmpArray
    {
      env.unscope(var);
      r = new DefineJaqlFunctionExpr(new Var[]{var}, r); // TODO: DefineCmpFn()? Add Cmp type?
    }
    ;

cmpArray returns [CmpArray r=null]
    { ArrayList<CmpSpec> keys = new ArrayList<CmpSpec>(); }
    : "[" ( cmpSpec[keys] ("," (cmpSpec[keys])? )* )? "]"
    {
      r = new CmpArray(keys);
    }
    ;
cmpSpec[ArrayList<CmpSpec> keys]
    { Expr e; Expr c=null; CmpSpec.Order o = CmpSpec.Order.ASC; }
    : e=expr 
      ( kwUsing c=comparator { oops("nested comparators NYI"); } )? 
      ( kwAsc | kwDesc { o = CmpSpec.Order.DESC; } )?
    {
      keys.add( new CmpSpec(e, o) );
    }
    ;
    
    
// -- sorting -------------------------------------------------------------------------------------

sort[Expr in] returns [Expr r=null]
    : kwSort r=sortCmp
      { r = new SortExpr(in, r); }
    ;

sortCmp returns [Expr r=null]
    { String v="$"; }
    : (kwEach v=id)? kwBy r=cmpArrayFn[v]
    | kwUsing r=comparator
    ;


// -- grouping ------------------------------------------------------------------------------------

group returns [Expr r=null]
    { 
      BindingExpr in = null;
      BindingExpr by = null;
      Expr c=null;
      Expr ret=null;
      Expr opt=null;
      String v = "$";
      ArrayList<Var> as = new ArrayList<Var>();
    }
    : kwGroup
      ( (kwEach id kwIn) => kwEach v=id kwIn )?
        { 
          in=new BindingExpr(BindingExpr.Type.IN, env.makeVar(v), null, Expr.NO_EXPRS); 
        }
      by=groupIn[in,by,as] ( "," by=groupIn[in,by,as] )*
        { if( by.var != Var.UNUSED ) env.scope(by.var); } 
      ( kwUsing c=comparator { oops("comparators on group by NYI"); } )?
      ( (kwOptions) => kwOptions opt=expr )?
        {
          for( Var av: as )
          {
            env.scope(av);
          }
        }
      ret=groupReturn
        {
          if( by.var != Var.UNUSED ) env.unscope(by.var);
          for( Var av: as )
          {
            env.unscope(av);
          }
          r = new GroupByExpr(in, by, as, c, opt, ret);
        }
    ;

groupIn[BindingExpr in, BindingExpr prevBy, ArrayList<Var> asVars] returns [BindingExpr by]
    { Expr e; String v=null; }
    : e=expr { env.scope(in.var); } by=groupBy[prevBy] ( kwAs v=id )?    
        {
          if( v == null )
          {
            if( e instanceof VarExpr )
            {
              v = ((VarExpr)e).var().taggedName();
            }
            else
            {
              oops("\"as\" variable required when grouping expression is not a simple variable");
            }
          }
          for( Var as: asVars ) 
          {
            if( as.taggedName().equals(v) )
            {
              oops("duplicate group \"as\" variable: "+v);
            }
          }
          in.addChild(e); 
          env.unscope(in.var); // TODO: unscope or hide?
          asVars.add(env.makeVar(v, SchemaFactory.arrayOrNullSchema()));
        }
    ;

groupBy[BindingExpr by] returns [BindingExpr b=null]
    { String v = null; Expr e=null; }
    : ( kwBy 
        ( (id "=") => (v=id "=") e=expr
                    | e=expr
        ) 
      )?
    {
      if( e == null )
      {
        e = new ConstExpr(null);
      }
      if( by == null )
      {
        Var var;
        if( v == null )
        {
            var = Var.UNUSED;
        }
        else
        {
            var = env.makeVar(v);
        }
        b = new BindingExpr(BindingExpr.Type.EQ, var, null, e);
      }
      else if( v == null || (by.var != Var.UNUSED && by.var.taggedName().equals(v)) )
      {
        by.addChild(e);
        b = by;
      }
      else
      {
        oops("all group by variables must have the same name:" +by.var.taggedName()+" != "+v);
      }
    }
    ;

groupReturn returns [Expr r=null]
    : kwInto r=expr    { r = new ArrayExpr(r); }
    | kwExpand r=expr
    //| r=aggregate[new VarExpr(pipeVar)] 
    //    { if(numInputs != 1) throw new RuntimeException("cannot use aggregate with cogroup"); } 
    ;

groupPipe[Expr in] returns [Expr r=null]
    { 
      BindingExpr b; BindingExpr by=null; Expr ret;
      Expr cmp = null;  
      Expr opt = null; 
      String v="$"; Var asVar = null; 
    }
    : kwGroup b=each[in] 
      by=groupBy[null]       { env.unscope(b.var); if( by.var != Var.UNUSED ) env.scope(by.var); }
      ( kwAs v=id )?         { asVar=env.scope(v, SchemaFactory.arraySchema()); }
      ( kwUsing cmp=comparator { oops("comparators on group by NYI"); } )?
      ( (kwOptions) => kwOptions opt=expr )?
      ret=groupReturn
        {
          if( by.var != Var.UNUSED ) env.unscope(by.var);
          env.unscope(asVar);
          r = new GroupByExpr(b, by, asVar, cmp, opt, ret);
        }
    ;


// -- aggregation ---------------------------------------------------------------------------------

aggregate[Expr in] returns [Expr r=null]
    { String v="$"; BindingExpr b=null; ArrayList<Expr> a; }
//    : ("aggregate" | "agg") b=each[in] r=expr
//       { r = AggregateExpr.make(env, b.var, b.inExpr(), r, false); } // TODO: take binding!
    : kwAggregate (kwAs v=id)?
         {
           //b = new BindingExpr(BindingExpr.Type.EQ, env.scope(v, in.getSchema().elements()), null, in); 
           b = new BindingExpr(BindingExpr.Type.EQ, env.scope(v), null, in);
         }
      ( kwInto    r=expr        { r = new AggregateGeneralExpr(b, r); }
      | kwFull    a=aggList     { r = new AggregateFullExpr(b, a); }
      | kwInitial a=algAggList { r = new AggregateInitialExpr(b, a); }
      | kwPartial a=algAggList { r = new AggregatePartialExpr(b, a); }
      | kwFinal   a=algAggList { r = new AggregateFinalExpr(b, a); }
      )
      { env.unscope(b.var); }
    ;

// TODO: We used to detected Aggregate and AlgebraicAggregate in the parser, now
// it is left to a later pass.  These productions can be simplified.
aggList returns [ArrayList<Expr> r = new ArrayList<Expr>()]
    { Expr a; }
    : "[" ( a=aggFn { r.add(a); } ("," a=aggFn { r.add(a); } )* )? "]"
    ;

aggFn returns [Expr agg=null]
   // { Expr expr; }
    : agg = expr
//      {
//        if ( expr instanceof FunctionCallExpr )
//        {
//            // force inline of calls to aggregate functions
////            FunctionCallExpr call = (FunctionCallExpr)expr;
////            if (call.fnExpr().isCompileTimeComputable().always())
////            {
////                try
////                {
////                    Function ff = (Function)env.eval(call.fnExpr());
////                    if (ff instanceof BuiltInFunction)
////                    {
////                        BuiltInFunction f = (BuiltInFunction)ff;
////                        if (Aggregate.class.isAssignableFrom(f.getDescriptor().getImplementingClass()))
////                        {
////                            expr = call.inline();
////                        }
////                    }
////                } 
////                catch (Exception e1)
////                {
////                    // ignore
////                }
////            }
//        }
//      
//        if( !( expr instanceof Aggregate ) )
//        {
//          oops("Aggregate required");
//        }
//        agg = (Aggregate)expr;
//      }
    ;

algAggList returns [ArrayList<Expr> r = new ArrayList<Expr>()]
    { Expr a; }
    : "[" ( a=algAggFn { r.add(a); } ("," a=algAggFn { r.add(a); } )* )? "]"
    ;

algAggFn returns [Expr agg=null]
//    { Expr e; }
    : agg = expr
//      {
//        if( !( e instanceof AlgebraicAggregate ) )
//        {
//          oops("Aggregate required");
//        }
//        agg = (AlgebraicAggregate)e;
//      }
    ;


// -- joins ---------------------------------------------------------------------------------------

join returns [Expr r=null]
    { 
      ArrayList<BindingExpr> in = new ArrayList<BindingExpr>();
      Expr p; 
      BindingExpr b;
      Expr opt=null;
    }
    : kwJoin b=joinIn     { in.add(b); b.var.setHidden(true); }
            ("," b=joinIn { in.add(b); b.var.setHidden(true); } )+  
      {
        for( BindingExpr b2: in )
        {
          b2.var.setHidden(false);
        }
      }
      kwWhere p=expr
      ( (kwOptions) => kwOptions opt=expr )?
      ( kwInto r=expr     { r = new ArrayExpr(r); }
      | kwExpand r=expr )
      {
        for( BindingExpr b2: in )
        {
          env.unscope(b2.var);
        }
        // r = new MultiJoinExpr(in, p, opt, r).expand(env);  
        r = new MultiJoinExpr(in, p, opt, r);  
      }
    ;

joinIn returns [BindingExpr b=null]
    { boolean p = false; }
    : ( (kwPreserve) => kwPreserve { p = true; }  
        | ()
      ) b=vpipe
      {
        b.preserve = p;
      }
    ;
    
// A pipe must end with a binding, with the name still in scope
vpipe returns [BindingExpr r=null]
    { String v; Expr e=null; }
    : v=id  ( /*empty*/   { e = new VarExpr(env.inscope(v)); }
            | kwIn e=expr  
            )
    { r = new BindingExpr(BindingExpr.Type.IN, env.scope(v), null, e); }
    ;    

equijoin returns [Expr r=null]
    { 
        ArrayList<BindingExpr> in = new ArrayList<BindingExpr>(); 
        ArrayList<Expr> on = new ArrayList<Expr>(); 
        Expr c=null; 
        Expr opt=null;
    }
    : kwEquijoin ejoinIn[in,on] ( "," ejoinIn[in,on] )+ 
      ( kwUsing c=comparator { oops("comparators on joins are NYI"); } )?
      {
        for( BindingExpr b: in )
        {
          b.var.setHidden(false);
        }
      }
      ( (kwOptions) => kwOptions opt=expr )?
      ( kwInto r=expr     { r = new ArrayExpr(r); }
      | kwExpand r=expr )
    {
      r = new JoinExpr(in,on,opt,r); // TODO: add comparator
      for( BindingExpr b: in )
      {
        env.unscope(b.var);
      }
    }
    ;

ejoinIn[ArrayList<BindingExpr> in, ArrayList<Expr> on]
    { Expr e; BindingExpr b; }
    : b=joinIn       { in.add(b); } 
      kwOn e=expr    { on.add(e); b.var.setHidden(true); }
    ;


// -- function definitions ------------------------------------------------------------------------

// function definition using fn keyword
fn returns [Expr r = null]
    { List<Var> vs = new ArrayList<Var>();
      List<Expr> es = new ArrayList<Expr>();
      Schema s = SchemaFactory.anySchema();
    }
    : kwFn 
      params[vs,es] { for (Var v: vs) v.setHidden(false); }
      // (":" s=schema)?
      r=pipe
      { 
        r = new DefineJaqlFunctionExpr(vs, es, s, r);
        for( Var v: vs )
        {
          env.unscope(v);
        }
      }
    ;
    
// function definition using ->   
pipeFn returns [Expr r=null]
    {
        Var v = env.makeVar("$");
        v.setHidden(true);
        r = new VarExpr(v);
    }
    : "->" r=op[r] r=subpipe[r]
    {
        ArrayList<Var> p = new ArrayList<Var>();
        p.add(v);
        r = new DefineJaqlFunctionExpr(p, r); // TODO: use a diff class
    }
    ;
    
// a built-in function     
builtinFunction returns [Expr e = null]
    { Expr c; }
    : kwBuiltin "(" c=expr ")" { e=new BuiltInExpr(c); }; 
        
// backwards compatibility, registerFunction("f", e) is now written as f=javaudf(e)
registerFunction returns [Expr e = null]
    { Expr varName, className; }
    : kwRegisterFunction "(" varName=expr "," className=expr ")" {
        try {
          if (!varName.isCompileTimeComputable().always())
          {
            throw new IllegalArgumentException("variable name has to be a constant");
          }
          String name = env.eval(varName).toString();
          Var var = env.scopeGlobalVal(name, SchemaFactory.functionSchema());
          e = new AssignExpr(env, var, new JavaUdfExpr(className).expand(env));
        } catch(Exception ex) {
            throw new RuntimeException(ex); 
      }
    };    
    
// list of parameters with parentheses    
params[List<Var> vs, List<Expr> es]
    : "(" allParams[vs,es] ")" 
      {
        Set<String> names = new HashSet<String>();
        for (Var v : vs) {
            if (names.contains(v.name())) 
                throw new IllegalArgumentException("duplicate parameter: " + v.name());
            names.add(v.name());            
        }; 
      } 
    ;

// list of parameters w/o parentheses    
allParams[List<Var> vs, List<Expr> es]
    { Expr e; }   
    : paramVar[vs] ( "=" e=pipe { es.add(e); } ( "," optionalParams[vs,es] ) *
                   | "," { es.add(null); } allParams[vs,es]
                   | () { es.add(null); } 
                   )
    | ()
    ;

// list of parameters that including default value
optionalParams[List<Var> vs, List<Expr> es]
    { Expr e; }
    : paramVar[vs] "=" e=pipe { es.add(e); }
    ;

// parameter schema and name
paramVar[List<Var> vs]
    { String n; Schema s=SchemaFactory.anySchema(); }
    : ( ( id ( "," | "=" | ")" | ":" ) ) => n=id (":" s=schema)? 
      | ( kwSchema )               => kwSchema s=schema n=id 
      | s=schema n=id
      )
      { Var v = new Var(n, s); env.scope(v); v.setHidden(true); vs.add(v); }
    ;

// -- function calls ------------------------------------------------------------------------------

// an expression e that can be followed by a call and zero or more of those calls
// e ( args )*
call returns [Expr r=null]
    { 
      List<Expr> positionalArgs = new ArrayList<Expr>(); 
      Map<JsonString, Expr> namedArgs = new HashMap<JsonString, Expr>(); 
    }
    : r=basic
      r=callRepeat[r,positionalArgs, namedArgs] 
    ;

// an expression following "->" that can be followed by a call and zero or more of those calls
// e ( args )+
callPipe[Expr in] returns [Expr r=null]
    { 
      List<Expr> positionalArgs = new ArrayList<Expr>(); 
      positionalArgs.add(0, in);
      Map<JsonString, Expr> namedArgs = new HashMap<JsonString, Expr>();
    }
    : r=basic args[positionalArgs, namedArgs]  
        {
          FunctionCallExpr fc = new FunctionCallExpr(r, positionalArgs, namedArgs);
          // This is a hack to allow inlining during parsing.
          // We need to inline during parsing right now for things like schemas to parse correctly.
          // TODO: eliminate need for parse-time inlining...
          new QueryExpr(env, fc);
          r = fc.inlineIfPossible();
        }
      r=callRepeat[r, positionalArgs, namedArgs]
    ;
    
// an optional call: ( args )?
callRepeat[Expr f, List<Expr> positionalArgs, Map<JsonString, Expr> namedArgs] returns [Expr r=f]
    : ( "(" ) => { positionalArgs.clear(); namedArgs.clear(); } 
                 args[positionalArgs, namedArgs]
                 {      
                    FunctionCallExpr fc = new FunctionCallExpr(r, positionalArgs, namedArgs);
                    // This is a hack to allow inlining during parsing.
                    // We need to inline during parsing right now for things like schemas to parse correctly.
                    // TODO: eliminate need for parse-time inlining...
                    new QueryExpr(env, fc);
                    r = fc.inlineIfPossible();
                 }
                 r = callRepeat[r, positionalArgs, namedArgs]
               | () 
    ;    
    
// (possibly empty) list of positional arguments followed by a list of named arguments
args[List<Expr> positionalArgs, Map<JsonString, Expr> namedArgs] 
    { List<Expr> l=positionalArgs; Map<JsonString, Expr> m=namedArgs; }
    : "(" ( (narg[m]) => (narg[m] ("," narg[m])*)
          | (parg[l] moreArgs[l,m] )
          | ()
          )
      ")"
    ;
      
// helper method to LL(1)     
moreArgs[List<Expr> l, Map<JsonString, Expr> m]
    : ()
    | "," ( (narg[m]) => (narg[m] ("," narg[m])*) 
          | parg[l] moreArgs[l,m] )
    ;
  
// positional argument  
parg[List<Expr> p]
    { Expr e; }
    : e=pipe { p.add(e); }
    ;

// named argument    
narg[Map<JsonString, Expr> m]
    { JsonString name = null; Expr e; }
    : name=idJson { 
        if (m.containsKey(name)) throw new IllegalArgumentException("repeated argument: " + name);
      }
      "=" 
      e=pipe { m.put(name, e); }
    ;

// -- schema --------------------------------------------------------------------------------------

schema returns [Schema s = null]
    { List<Schema> alternatives = new ArrayList<Schema>(); Schema s2; }
    : (
        s = schemaTerm       { alternatives.add(s); }     
        ( "|"              
          s2 = schemaTerm     { alternatives.add(s2); } 
        )*
      ) {
          s = OrSchema.make(alternatives);
        }
    ;
    
schemaTerm returns [Schema s = null]
    : s=aSchema
      ( "?"        { s = SchemaTransformation.addNullability(s); }
      )?
    ;

aSchema returns [Schema s = null]
    : s=atomSchema
    | s=arraySchema
    | s=recordSchema
    ;


// -- schema: atomic values -----------------------------------------------------------------------

atomSchema returns [Schema s = null]
    { String id; JsonRecord args; JsonValueParameters p=null; }
    : kwNull                   { s = SchemaFactory.nullSchema(); }
    | id=id                    { p=SchemaFactory.getParameters(id); }
      args=atomSchemaArgs[p]   { s = SchemaFactory.make(id, args); }
    ;

atomSchemaArgs[JsonValueParameters d] returns [ JsonRecord args=null ]
    { 
      List<Expr> positionalArgs = new ArrayList<Expr>();
      Map<JsonString, Expr> namedArgs = new HashMap<JsonString, Expr>();
    }
    : ( "(" ) => args[positionalArgs,namedArgs]
                 { 
                   ArgumentExpr e = new ArgumentExpr(d, positionalArgs, namedArgs);
                   args = e.constEval(env); 
                 }
               | ()
    ;


// -- schema: arrays ------------------------------------------------------------------------------

arraySchema returns [ArraySchema s = null]
    { ArrayList<Schema> schemata = new ArrayList<Schema>(); 
      Boolean repeat = false; 
    }
    : "["
        arraySchemaList[schemata]
        repeat=arraySchemaRepeat
      "]" { 
            Schema rest = null;
            if (repeat)
            {
                rest = schemata.remove(schemata.size()-1);
            }
            Schema[] schemaArray = schemata.toArray(new Schema[schemata.size()]);
            s = new ArraySchema(schemaArray, rest);
          }
    ;

arraySchemaList[ArrayList<Schema> schemata]
    {
        Schema p;
    }
    : ( "*" ) => ( /* leave * on input */ ) { schemata.add(SchemaFactory.anySchema()); }
    | p = schema { schemata.add(p); } ("," arraySchemaList[schemata])?
    | () 
    ;

arraySchemaRepeat returns [Boolean repeat = false; ]
    { JsonLong i; }
    : /* empty */   { repeat = false; }
    | "*"           { repeat = true; }
    // remaining options are for backwards compatibility
    | DOT_DOT_DOT   { repeat = true; }
    | "<" ( i=longLit { if (i.get() != 0) 
                          throw new IllegalArgumentException(
                            "<n,n> syntax is deprecated; use * instead"); 
                      } 
            "," "*"
          | "*" ("," "*") ?
          ) ">"   { repeat = true; }
    ;


// -- schema: records -----------------------------------------------------------------------------

recordSchema returns [RecordSchema s = null]
    { 
      List<RecordSchema.Field> fields = new ArrayList<RecordSchema.Field>(); 
      Schema rest = null;
      RecordSchema.Field f = null;    
    }
    : "{" ( ( f=recordSchemaField                { fields.add(f); }
            | rest=recordSchemaRest[rest] 
            )
            ( "," ( f=recordSchemaField          { fields.add(f); } 
                  | rest=recordSchemaRest[rest] 
                  ) 
            )*
          )? 
      "}" { 
            RecordSchema.Field[] fieldsArray = fields.toArray(new RecordSchema.Field[fields.size()]);
            s = new RecordSchema(fieldsArray, rest);
          }
    ;

recordSchemaRest[Schema currentRest] returns [Schema s = null]
    : "*" s=recordSchemaFieldSchema
      { 
        if( currentRest != null ) 
        {
          oops("only one wildcard field is allowed in a record schema");
        }
      }
    ;

recordSchemaField returns [RecordSchema.Field s = null]
    { String n; boolean optional = false; Schema t; }
    : n=recordSchemaFieldName
      ( "?" { optional = true; } )?
      t=recordSchemaFieldSchema
      {
        s = new RecordSchema.Field(new JsonString(n), t, optional);
      }
    ;
    
recordSchemaFieldSchema returns [Schema s = null]
    : /*empty*/   { s = SchemaFactory.anySchema(); }
    | ":" s=schema
    ;

recordSchemaFieldName returns [String s=null]
    : s=id
    | s=str
    ;


// -- keywords ------------------------------------------------------------------------------------
    
// a keyword
kw
    : strictkw
    | weakkw
    | softkw
    ;

// Strict keywords: cannot be used as identifiers
strictkw : kwAnd 
         | kwCmp
         | kwFalse 
         | kwFn 
         | kwNot 
         | kwNull 
         | kwOr 
         | kwTrue;

kwAnd    : "and"   | "#and" ;
kwCmp    : "cmp"   | "#cmp" ;
kwFalse  : "false" | "#false" ;
kwFn     : "fn"    | "#fn" ;
kwNot    : "not"   | "#not" ;
kwNull   : "null"  | "#null" ; 
kwOr     : "or"    | "#or" ;
kwTrue   : "true"  | "#true" ;

// Weak keywords: shadowed by in-scope identifiers
weakkw   : (kwBuiltin) => kwBuiltin 
         | (kwEquijoin) => kwEquijoin
         | (kwExpand) => kwExpand 
         | (kwExplain) => kwExplain 
         | (kwFilter) => kwFilter 
         | (kwFlatten) => kwFlatten 
         | (kwFor) => kwFor 
         | (kwWhile) => kwWhile 
         | (kwGroup) => kwGroup
         | (kwIf) => kwIf 
         | (kwIsdefined) => kwIsdefined 
         | (kwIsnull) => kwIsnull 
         | (kwJoin) => kwJoin 
         | (kwPreserve) => kwPreserve 
         | (kwQuit) => kwQuit 
         | (kwRegisterFunction) => kwRegisterFunction
         | (kwSplit) => kwSplit
         | (kwTop) => kwTop 
         | (kwTransform) => kwTransform 
         | (kwUnroll) => kwUnroll 
         ;

kwBuiltin          : { nextIsWeakKw("builtin") }? ID          | "#builtin" ;
kwEquijoin         : { nextIsWeakKw("equijoin") }? ID         | "#equijoin" ;     
kwExpand           : { nextIsWeakKw("expand") }? ID           | "#expand" ;
kwExplain          : { nextIsWeakKw("explain") }? ID          | "#explain" ;
kwFilter           : { nextIsWeakKw("filter") }? ID           | "#filter" ;
kwFlatten          : { nextIsWeakKw("flatten") }? ID          | "#flatten" ;    
kwFor              : { nextIsWeakKw("for") }? ID              | "#for" ;
kwWhile            : { nextIsWeakKw("while") }? ID            | "#while" ;
kwGroup            : { nextIsWeakKw("group") }? ID            | "#group" ;
kwIf               : { nextIsWeakKw("if") }? ID               | "#if" ;
kwIsdefined        : { nextIsWeakKw("isdefined") }? ID        | "#isdefined" ;
kwIsnull           : { nextIsWeakKw("isnull") }? ID           | "#isnull" ;          
kwJoin             : { nextIsWeakKw("join") }? ID             | "#join" ;
kwPreserve         : { nextIsWeakKw("preserve") }? ID         | "#preserve" ;
kwQuit             : { nextIsWeakKw("quit") }? ID             | "#quit" ;    
kwRegisterFunction : { nextIsWeakKw("registerFunction") }? ID | "#registerFunction" ;
kwSplit            : { nextIsWeakKw("split") }? ID            | "#split" ;
kwTop              : { nextIsWeakKw("top") }? ID              | "#top" ;
kwTransform        : { nextIsWeakKw("transform") }? ID        | "#transform" ;
kwUnroll           : { nextIsWeakKw("unroll") }? ID           | "#unroll" ;
kwOptions          : { nextIsWeakKw("options") }? ID          | "#options" ;
       
// Soft keywords: occur in the grammar at places where no identifier is allowed 
// (thus no shadowing or ambiguitiy)
softkw : kwAggregate 
       | kwAs 
       | kwAsc 
       | kwBy 
       | kwDesc 
       | kwEach
       | kwElse
       | kwFinal
       | kwFull
       | kwImport
       | kwIn
       | kwInitial
       | kwInstanceof
       | kwInto
       | kwMaterialize
       | kwOn
       | kwPartial
       | kwSchema
       | kwSort
       | kwUsing
       | kwWhere
       ; 
    
kwAggregate        : "aggregate" | "#aggregate";   
kwAs               : "as" | "#as" ;
kwAsc              : "asc" | "#asc" ;
kwBy               : "by" | "#by" ;
kwDesc             : "desc" | "#desc" ;
kwEach             : "each" | "#each" ;
kwElse             : "else" | "#else" ;
kwExtern           : "extern" | "#extern" ;
kwFinal            : "final" | "#final" ;
// kwDefault          : "default" | "#default" ;
kwFull             : "full" | "#full" ;
kwImport           : "import" | "#import" ;
kwIn               : "in" | "#in" ;
kwInitial          : "initial" | "#initial" ;
kwInstanceof       : "instanceof" | "#instanceof" ;
kwInto             : "into" | "#into" ;
kwPartial          : "partial" | "#partial" ;
kwMaterialize      : "materialize" | "#materialize" ;
kwOn               : "on" | "#on" ;
kwSchema           : "schema" | "#schema" ;
kwSort             : "sort" | "#sort" ;
kwUsing            : "using" | "#using" ;
kwWhere            : "where" | "#where" ; 

// used for obtaining the string value of a soft keyword (for identifiers)
softkwToString returns [String s=null]
    : "aggregate"        { s = "aggregate"; }
    | "as"               { s = "as"; }
    | "asc"              { s = "asc"; }
    | "by"               { s = "by"; }
    | "desc"             { s = "desc"; }
    | "each"             { s = "each"; }
    | "else"             { s = "else"; }    
    | "extern"           { s = "extern"; }
    | "final"            { s = "final"; }
    | "full"             { s = "full"; }
    | "import"           { s = "import"; }
    | "in"               { s = "in"; }
    | "initial"          { s = "initial"; }
    | "instanceof"       { s = "instanceof"; }
    | "into"             { s = "into"; }
    | "partial"          { s = "partial"; }
    | "materialize"      { s = "materialize"; }
    | "on"               { s = "on"; }
    | "schema"           { s = "schema"; }    
    | "sort"             { s = "sort"; }
    | "using"            { s = "using"; }
    | "where"            { s = "where"; }
    ; 
  
// -- identifiers ---------------------------------------------------------------------------------
    
// an identifier as String    
id returns [String s=null]
    : i:ID              { s = i.getText(); }
    | s=softkwToString
    ;

// an identifier as JsonString
idJson returns [JsonString js=null]
    { String s; } 
    : s=id { js = new JsonString(s); }
    ;

// an identifier as ConstExpr(JsonString)
idExpr returns [Expr e=null]
    { JsonString s; }
    : s=idJson { e = new ConstExpr(s); }
    ;
    
    
// -- variables -----------------------------------------------------------------------------------    
    
// an in-scope variable
var returns [Var v = null]
    { String s; }
    : s=id              { v = env.inscope(s); }
    | m:NAMESPACE_ID    { s = m.getText(); 
                          String[] parts = s.split("::"); // known to occur precisely once
                          if (parts[0].isEmpty())
                          {
                            v = env.globals().inscopeLocal(parts[1]);
                          }
                          else
                          {
                            v = env.inscopeImport(parts[0], parts[1]);
                          } 
                        }
    ;

// an expression representing an in-scope variable
varExpr returns [Expr e = null]
    { Var v; }
    : v=var { e = new VarExpr(v); }
    ;

//// -- select statement ----------------------------------------------------------------------------------- 
//
//// TODO: sqlUnion etc...
//sqlTableExpr returns [Expr r=null]
//    { SqlTableExpr t; }
//    : t=sqlSelectStmt   { r = t.wrapToJaql(env); }
//    ;
//
//sqlSelectStmt returns [SqlSelect r=null]
//    { 
//        List<SqlColumnExpr> cols;
//        List<SqlTableImport> from;
//        SqlExpr where=null;
//    }
//    : /*(sqlWith)?*/ 
//      cols=sqlSelectClause 
//      from=sqlFromClause
//      (where=sqlWhereClause)? 
//      /*(groupClause[r])? (orderClause[r])?*/
//    {
//        r = new SqlSelect(from,where,cols);
//    }
//    ;
//
///*
//sqlWith
//    : "with" withTable ("," withTable)*
//    ;
//
//withTable
//    : tableAlias "(" selectStmt ")"
//    ;
//*/
//
//sqlSelectClause returns[List<SqlColumnExpr> cols=new ArrayList<SqlColumnExpr>()]
//    : kwSelect sqlSelectColumn[cols]
//          ("," sqlSelectColumn[cols] )*
//    ;
//
//// FIXME: make soft, support any casing
//kwSelect: "SELECT" | "select";
//kwFrom: "FROM" | "from";
//kwWhere2: "WHERE" | kwWhere;
//kwAs2: "AS" | kwAs;
//    
//sqlSelectColumn[List<SqlColumnExpr> cols]
//    { SqlExpr e; String i=null; }
//    : e=sqlExpr (kwAs2 i=sqlId)? { SqlColumnExpr col = new SqlColumnExpr(e,i,cols.size()); cols.add(col); }
//    ;
//
//sqlFromClause returns[List<SqlTableImport> from=new ArrayList<SqlTableImport>()]
//    { SqlTableImport t; }
//    : kwFrom t=sqlFromTable {from.add(t);} 
//        ( options {greedy=true;} :             // TODO: fix nondeterminism between from list and fn param -- move sql above pipe + into () 
//          "," t=sqlFromTable {from.add(t);} )*
//    ;
//    
//sqlFromTable returns [SqlTableImport table=null]
//    { SqlTableExpr t; String i; String a=null; }
//    : i=sqlId ( /*empty*/ {a=i;} ((kwAs2)? a=sqlTableAlias)?   { table = new SqlTableImport(new JaqlToSqlTable(env,i),a); }
//           /* | "(" tableFnArgs ")" kwAs2 a=tableAlias   { table = new SqlTableImport(new SqlTableFn(i,args),a); } */
//           )
//    | "table" "(" t=sqlNestedFromTable ")" (kwAs2)? a=sqlTableAlias { table = new SqlTableImport(t,a); }
//    /* | "jaql" "(" t=selectStmt ")" kwAs2 a=tableAlias { table = new SqlTableImport(new JaqlToSqlTable(...),a); }
//    /* | or table function */ 
//    ;
//    
//    
//sqlNestedFromTable returns [SqlTableExpr t=null]
//    { SqlExpr e; }
//    : t=sqlSelectStmt
//    | e=sqlExpr     { t=new SqlExprToTable(e); }
//    ;
//    
//sqlTableAlias returns [String t]
//    /*    : id ( "(" id ("," id)* ")" )? */
//    : t=sqlId
//    ; 
//
//sqlWhereClause returns [SqlExpr r]
//    : kwWhere2 r=sqlExpr
//    ;
//
///*
//sqlGroupClause
//    : "group" "by" sqlExpr ("," sqlExpr)*
//    ;
//    
//sqlOrderClause
//    : "order" "by" sqlExpr ("," sqlExpr)*
//    ;
//*/
//
//
//// -- sql expressions ---------------------------------------------------------------------------- 
//
//sqlExpr returns [SqlExpr r]
//    : r=sqlOrExpr
//    ;
//
//sqlOrExpr returns [SqlExpr r]
//    { SqlExpr s; }
//    : r=sqlAndExpr ( "or" s=sqlAndExpr { r=new JaqlFnSqlExpr(OrExpr.class,r,s); } )*
//    ;
//
//sqlAndExpr returns [SqlExpr r]
//    { SqlExpr s; }
//    : r=sqlNotExpr ( "and" s=sqlNotExpr { r=new JaqlFnSqlExpr(AndExpr.class,r,s); } )*
//    ;
//
//sqlNotExpr returns [SqlExpr r]
//    : "not" r=sqlNotExpr  { r=new JaqlFnSqlExpr(NotExpr.class,r); }
//    | r=sqlCompareExpr
//    ;
//
//sqlCompareExpr returns [SqlExpr r]
//    { int o; SqlExpr s; }
//    : r=sqlBasicExpr ( o=sqlCompareOp  s=sqlBasicExpr { r=new SqlCompareExpr(o,r,s); })?
//    ;
//
//sqlCompareOp returns [int r = -1]
//    : "="  { r = CompareExpr.EQ; }
//    | "==" { r = CompareExpr.EQ; }
//    | "<"  { r = CompareExpr.LT; }
//    | ">"  { r = CompareExpr.GT; }
//    | "!=" { r = CompareExpr.NE; }
//    | "<>" { r = CompareExpr.NE; }
//    | "<=" { r = CompareExpr.LE; }
//    | ">=" { r = CompareExpr.GE; }
//    ;
//
//sqlBasicExpr returns [SqlExpr r=null]
//    { Expr e; }
//    : (sqlId) => r=sqlColumnRef
//    | e=constant         { r=new JaqlToSqlExpr(e); }
//    | "(" r=sqlExpr ")"
//    ;
//
//sqlColumnRef returns [SqlColumnRef r=null]
//    { String c; String t=null; }
//    : c=sqlId (i:DOT_ID { t=c; c=i.getText(); })? { r=new SqlColumnRef(t,c); }
//    ;    
//
//// -- sql common elements ---------------------------------------------------------------------------- 
//    
//sqlId returns [String s=null]
//    : i:ID    { s=i.getText(); }
//    | q:DQSTR { s=q.getText(); }
//    ;
//
//sqlStr returns [String r=null]
//    : s:SQSTR       { r=s.getText(); }
//    | h:HERE_STRING { r=h.getText(); }
//    ;


// -- trashcan ------------------------------------------------------------------------------------

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
//      String c, a, p, n; 
//      Var v;
//    }
//    : (c=var) // ("at" a=var)? ("previous" p=var)? ("next" n=var)?
//    ;

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
//               // TODO: should copart introduce multiple streams?? 
//               // b.var.hidden = false;
//           }
//         }
//      "|-" r=opPipe[partVar] "-|" // r=srcPipe "-|" 
//         {
//           env.unscope( byVar );
//           for( BindingExpr b: inputs )
//           {
//               env.unscope(b.var);
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
//              byVar = inputs.get(0).var2;
//              if( bn != null && ! byVar.name().equals(bn) )
//              {
//                throw new RuntimeException("must use same name for all key expressions: "+bn+" != "+byVar.name());
//              }
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
    
    
    
// -- the lexer ----------------------------------------------------------------------------------- 
    
class JaqlLexer extends Lexer;

options {
  charVocabulary = '\u0000'..'\uFFFE'; // all characters except special ANTLR EOF (-1)
  testLiterals=false;    // don't automatically test for literals
  k=3;                   // lookahead // TODO: try to reduce to 2
}

{
    private int indent;
    private int blockIndent;
    private String blockTag;
    
    public boolean isLiteral(String s)
    {
        return literals.containsKey(new ANTLRHashString(s, this));
    }
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
    :   "/*"
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
        	// TODO: this is not doing \ escaping.  should it?
            _saveIndex=text.length();
            match(' ');
            text.setLength(_saveIndex);
        }
    }
    ;
    

//protected BLOCK_STRING1 // options { generateAmbigWarnings=false; }
//  { blockIndent = 0; }
//  : "|"! (' '! | '\t'!)* NL! (' '! {blockIndent++;})+ (~(' '|'\t'|'\n'|'\r'))* NL (BLOCK_LINE)*
//  { System.out.println("here: ["+new String(text.getBuffer(), _begin, text.length()-_begin)+"]"); }
//  ;
//
//protected BLOCK_LINE
//  : '|'! (' '! (~('\n'|'\r'))*)? NL
//  ;
//
//BLOCK_STRING
//  : (BLOCK_LINE (' '! | '\t'!)* )+
//  ;

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

HERE_STRING
    : '<'! '<'! HERE_TAG (HERE_LINE)+
    ;


protected IDWORD
    : ('$'|LETTER|'_') (LETTER|'_'|DIGIT)*
    ;

protected TAG
    :  '#' (LETTER|'_'|DIGIT)+
    ;

KEYWORD options { testLiterals=true; }
    : '#' (LETTER|'_'|DIGIT)+
    ;  
  
ID
    : ( IDWORD ':' ':' IDWORD ) => IDWORD ':' ':' IDWORD  { $setType(NAMESPACE_ID); }
    | ( IDWORD TAG ) => IDWORD TAG
    | ( IDWORD (WS)* ('?' (WS)* ':' | ':' ) ) => IDWORD   
    | IDWORD /* those IDs might also be keywords */       { _ttype = testLiteralsTable(_ttype); }
    ;
    
NAMESPACE_ID
    : ':' ':' IDWORD
    ;
    
SYM
    options { testLiterals=true; }
    : '(' | ')' | '[' | ']' | ','
    | '|' | '&' | '@'
    | '}' 
    | '=' ('=' | '>')? 
    | '<' ('>' | '=')? 
    | '>' ('=')? 
    | '!' ('=')?
    | '/' | '*' | '+' 
    | '-' ('>')?
    | '?'
    | ':' ( '=' )?
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
     | '\\' ( options { generateAmbigWarnings=false; }
            : '\''  { $setText("\'"); }
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
            | /* invalid escape sequence */
               {
               	  // attempt to recover by searching for the end of the string.
               	  // TODO: The recovery is insensitive to what kind of string we started. should it be?
               	  char bad = (char)LA(1);
                  int c;
               	  do
               	  {
                    c = LA(1);
               	  	if( c == EOF_CHAR ) 
               	  	{
               	  	  break;
               	  	}
               	  	match((char)c);
               	  }
               	  while( c != '\'' && c != '"' );
               	  throw new NoViableAltForCharException(bad, getFilename(), getLine(), getColumn());
               }
            )
     ;

SQSTR
    : '\''! ( STRCHAR | '\"' )* '\''!
    ;

DQSTR
    : '\"'! ( STRCHAR | '\'' )* '\"'!
    ;

// TODO: this is going away! use hex('str') instead
HEXSTR
    : ('x'!|'X'!) '\"'! (~('\"'))* '\"'!
    | ('x'!|'X'!) '\''! (~('\''))* '\''!
    ;
    
//protected REGEX_CHAR
//     : (~( '/' | '\\' ))
//     | '\\' ( '/'  { System.out.println("hi"); $setText("/"); }
//            | .
//          )
//    ;
//
//REGEX
//  : '/' ( REGEX_CHAR )* '/' ('g'|'i'|'m')*
//  ;

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
           ( /*empty*/       {$setType(DOUBLE);} 
           | "m"!            {$setType(DEC);}
           | "d"!            {$setType(DOUBLE);}
           )
    |  INT
        ( /*empty*/          {$setType(INT);} 
        | "m"!               {$setType(DEC);}
        | "d"!               {$setType(DOUBLE);}
        )
    | '0'('x'|'X')(HEX)+     {$setType(INT);}
    | (DOT_ID) => DOT_ID     {$setType(DOT_ID);}
    | (DOT_STAR) => DOT_STAR {$setType(DOT_STAR); }
    | '.' (                  {$setType(DOT);}
          | '.' '.'          {$setType(DOT_DOT_DOT);}
          )
    ;

protected DOT_DOT_DOT
    : '.' '.' '.' 
    ;

protected DOT_ID
    : '.'! IDWORD
    ;

protected DOT_STAR
    : '.' '*'
    ;

// TODO: this is going away! use date('...') instead
DATETIME
    : ('d'!|'D'!) '\"'! (~('\"'))* '\"'!
    | ('d'!|'D'!) '\''! (~('\''))* '\''!
    ;
    
