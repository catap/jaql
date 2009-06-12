// $ANTLR 2.7.6 (2005-12-22): "jaql.g" -> "JaqlParser.java"$

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


import antlr.TokenBuffer;
import antlr.TokenStreamException;
import antlr.TokenStreamIOException;
import antlr.ANTLRException;
import antlr.LLkParser;
import antlr.Token;
import antlr.TokenStream;
import antlr.RecognitionException;
import antlr.NoViableAltException;
import antlr.MismatchedTokenException;
import antlr.SemanticException;
import antlr.ParserSharedInputState;
import antlr.collections.impl.BitSet;

public class JaqlParser extends antlr.LLkParser       implements JaqlTokenTypes
 {

	public boolean done = false;
	public Env env = new Env();
    // static abstract class ExpandMapper { public abstract Expr remap(Expr ctx); } 

    public void oops(String msg) throws RecognitionException, TokenStreamException
    { 
      throw new RecognitionException(msg, getFilename(), LT(1).getColumn(), LT(1).getLine()); 
    }

protected JaqlParser(TokenBuffer tokenBuf, int k) {
  super(tokenBuf,k);
  tokenNames = _tokenNames;
}

public JaqlParser(TokenBuffer tokenBuf) {
  this(tokenBuf,1);
}

protected JaqlParser(TokenStream lexer, int k) {
  super(lexer,k);
  tokenNames = _tokenNames;
}

public JaqlParser(TokenStream lexer) {
  this(lexer,1);
}

public JaqlParser(ParserSharedInputState state) {
  super(state,1);
  tokenNames = _tokenNames;
}

	public final Expr  parse() throws RecognitionException, TokenStreamException {
		Expr r=null;
		
		
		switch ( LA(1)) {
		case SEMI:
		case LITERAL_explain:
		case LITERAL_materialize:
		case LITERAL_quit:
		case 9:
		case LITERAL_group:
		case LITERAL_cmp:
		case 22:
		case 24:
		case LITERAL_join:
		case LITERAL_equijoin:
		case LITERAL_if:
		case LITERAL_unroll:
		case LITERAL_fn:
		case LITERAL_script:
		case LITERAL_import:
		case LITERAL_for:
		case 47:
		case LITERAL_not:
		case LITERAL_isnull:
		case LITERAL_exists:
		case LITERAL_isdefined:
		case 65:
		case 66:
		case LITERAL_type:
		case INT:
		case DEC:
		case DOUBLE:
		case HEXSTR:
		case DATETIME:
		case LITERAL_true:
		case LITERAL_false:
		case LITERAL_null:
		case STR:
		case HERE_STRING:
		case BLOCK_STRING:
		case AVAR:
		case VAR:
		case ID:
		{
			r=stmtOrFn();
			break;
		}
		case EOF:
		{
			match(Token.EOF_TYPE);
			done = true;
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		return r;
	}
	
	public final Expr  stmtOrFn() throws RecognitionException, TokenStreamException {
		Expr r=null;
		
		
		switch ( LA(1)) {
		case LITERAL_explain:
		case LITERAL_materialize:
		case LITERAL_quit:
		case 9:
		case LITERAL_group:
		case LITERAL_cmp:
		case 22:
		case 24:
		case LITERAL_join:
		case LITERAL_equijoin:
		case LITERAL_if:
		case LITERAL_unroll:
		case LITERAL_fn:
		case LITERAL_for:
		case 47:
		case LITERAL_not:
		case LITERAL_isnull:
		case LITERAL_exists:
		case LITERAL_isdefined:
		case 65:
		case 66:
		case LITERAL_type:
		case INT:
		case DEC:
		case DOUBLE:
		case HEXSTR:
		case DATETIME:
		case LITERAL_true:
		case LITERAL_false:
		case LITERAL_null:
		case STR:
		case HERE_STRING:
		case BLOCK_STRING:
		case AVAR:
		case VAR:
		case ID:
		{
			r=stmt();
			{
			switch ( LA(1)) {
			case SEMI:
			{
				match(SEMI);
				break;
			}
			case EOF:
			{
				match(Token.EOF_TYPE);
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
			}
			break;
		}
		case SEMI:
		{
			match(SEMI);
			break;
		}
		case LITERAL_script:
		case LITERAL_import:
		{
			r=functionDef();
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		return r;
	}
	
	public final Expr  stmt() throws RecognitionException, TokenStreamException {
		Expr r=null;
		
		String v;
		
		switch ( LA(1)) {
		case 9:
		case LITERAL_group:
		case LITERAL_cmp:
		case 22:
		case 24:
		case LITERAL_join:
		case LITERAL_equijoin:
		case LITERAL_if:
		case LITERAL_unroll:
		case LITERAL_fn:
		case LITERAL_for:
		case 47:
		case LITERAL_not:
		case LITERAL_isnull:
		case LITERAL_exists:
		case LITERAL_isdefined:
		case 65:
		case 66:
		case LITERAL_type:
		case INT:
		case DEC:
		case DOUBLE:
		case HEXSTR:
		case DATETIME:
		case LITERAL_true:
		case LITERAL_false:
		case LITERAL_null:
		case STR:
		case HERE_STRING:
		case BLOCK_STRING:
		case AVAR:
		case VAR:
		case ID:
		{
			r=topAssign();
			break;
		}
		case LITERAL_explain:
		{
			match(LITERAL_explain);
			r=topAssign();
			r = new ExplainExpr(r);
			break;
		}
		case LITERAL_materialize:
		{
			match(LITERAL_materialize);
			v=var();
			r = new MaterializeExpr(env.inscope(v));
			break;
		}
		case LITERAL_quit:
		{
			match(LITERAL_quit);
			done = true;
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		return r;
	}
	
	public final Expr  functionDef() throws RecognitionException, TokenStreamException {
		Expr r=null;
		
		String lang; String s; String body; Expr e;
		
		switch ( LA(1)) {
		case LITERAL_script:
		{
			match(LITERAL_script);
			lang=name();
			body=str();
			
				  	r = new ScriptBlock(lang, body);
				
			break;
		}
		case LITERAL_import:
		{
			match(LITERAL_import);
			lang=name();
			s=name();
			e=expr();
			{
			switch ( LA(1)) {
			case SEMI:
			{
				match(SEMI);
				break;
			}
			case EOF:
			{
				match(Token.EOF_TYPE);
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
			}
			
				  	if( ! "java".equals(lang.toLowerCase()) ) oops("only java functions supported right now");
				  	r = new RegisterFunctionExpr(new ConstExpr(new JString(s)), e);
				
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		return r;
	}
	
	public final Expr  topAssign() throws RecognitionException, TokenStreamException {
		Expr r=null;
		
		String v;
		
		{
		switch ( LA(1)) {
		case AVAR:
		{
			v=avar();
			match(18);
			r=rvalue();
			r = new AssignExpr( env.sessionEnv().scopeGlobal(v), r);
			break;
		}
		case 9:
		case LITERAL_group:
		case LITERAL_cmp:
		case 22:
		case 24:
		case LITERAL_join:
		case LITERAL_equijoin:
		case LITERAL_if:
		case LITERAL_unroll:
		case LITERAL_fn:
		case LITERAL_for:
		case 47:
		case LITERAL_not:
		case LITERAL_isnull:
		case LITERAL_exists:
		case LITERAL_isdefined:
		case 65:
		case 66:
		case LITERAL_type:
		case INT:
		case DEC:
		case DOUBLE:
		case HEXSTR:
		case DATETIME:
		case LITERAL_true:
		case LITERAL_false:
		case LITERAL_null:
		case STR:
		case HERE_STRING:
		case BLOCK_STRING:
		case VAR:
		case ID:
		{
			r=pipe();
			{
			switch ( LA(1)) {
			case EOF:
			case SEMI:
			{
				r = env.importGlobals(r);
				break;
			}
			case 40:
			{
				match(40);
				v=var();
				r = new AssignExpr( env.sessionEnv().scopeGlobal(v), r);
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
			}
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		}
		return r;
	}
	
	public final String  var() throws RecognitionException, TokenStreamException {
		String r=null;
		
		Token  n = null;
		
		n = LT(1);
		match(VAR);
		r = n.getText();
		return r;
	}
	
	public final Expr  block() throws RecognitionException, TokenStreamException {
		Expr r=null;
		
		ArrayList<Expr> es=null;
		
		r=optAssign();
		{
		switch ( LA(1)) {
		case 8:
		{
			match(8);
			es = new ArrayList<Expr>(); es.add(r);
			r=optAssign();
			es.add(r);
			{
			_loop8:
			do {
				if ((LA(1)==8)) {
					match(8);
					r=optAssign();
					es.add(r);
				}
				else {
					break _loop8;
				}
				
			} while (true);
			}
			
				for(Expr e: es)
				{
					if( e instanceof BindingExpr )
					{
						env.unscope(((BindingExpr)e).var);
					}
				}
				r = new DoExpr(es);
				// r = new ParallelDoExpr(es); // TODO: parallel
			
			break;
		}
		case 23:
		{
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		}
		return r;
	}
	
	public final Expr  optAssign() throws RecognitionException, TokenStreamException {
		Expr r=null;
		
		String v;
		
		switch ( LA(1)) {
		case AVAR:
		{
			v=avar();
			match(18);
			r=rvalue();
			r = new BindingExpr(BindingExpr.Type.EQ, env.scope(v), null, r);
			break;
		}
		case 9:
		case LITERAL_group:
		case LITERAL_cmp:
		case 22:
		case 24:
		case LITERAL_join:
		case LITERAL_equijoin:
		case LITERAL_if:
		case LITERAL_unroll:
		case LITERAL_fn:
		case LITERAL_for:
		case 47:
		case LITERAL_not:
		case LITERAL_isnull:
		case LITERAL_exists:
		case LITERAL_isdefined:
		case 65:
		case 66:
		case LITERAL_type:
		case INT:
		case DEC:
		case DOUBLE:
		case HEXSTR:
		case DATETIME:
		case LITERAL_true:
		case LITERAL_false:
		case LITERAL_null:
		case STR:
		case HERE_STRING:
		case BLOCK_STRING:
		case VAR:
		case ID:
		{
			r=pipe();
			{
			switch ( LA(1)) {
			case 40:
			{
				match(40);
				v=var();
				r = new BindingExpr(BindingExpr.Type.EQ, env.scope(v), null, r);
				break;
			}
			case 8:
			case 23:
			{
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
			}
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		return r;
	}
	
	public final Expr  pipe() throws RecognitionException, TokenStreamException {
		Expr r=null;
		
		String n = "$";
		
		switch ( LA(1)) {
		case LITERAL_group:
		case LITERAL_cmp:
		case 22:
		case 24:
		case LITERAL_join:
		case LITERAL_equijoin:
		case LITERAL_if:
		case LITERAL_unroll:
		case LITERAL_fn:
		case LITERAL_for:
		case 47:
		case LITERAL_not:
		case LITERAL_isnull:
		case LITERAL_exists:
		case LITERAL_isdefined:
		case 65:
		case 66:
		case LITERAL_type:
		case INT:
		case DEC:
		case DOUBLE:
		case HEXSTR:
		case DATETIME:
		case LITERAL_true:
		case LITERAL_false:
		case LITERAL_null:
		case STR:
		case HERE_STRING:
		case BLOCK_STRING:
		case VAR:
		case ID:
		{
			r=expr();
			{
			_loop11:
			do {
				if ((LA(1)==9)) {
					match(9);
					r=op(r);
				}
				else {
					break _loop11;
				}
				
			} while (true);
			}
			break;
		}
		case 9:
		{
			r=pipeFn();
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		return r;
	}
	
	public final Expr  expr() throws RecognitionException, TokenStreamException {
		Expr r;
		
		
		switch ( LA(1)) {
		case LITERAL_group:
		{
			r=group();
			break;
		}
		case LITERAL_join:
		{
			r=join();
			break;
		}
		case LITERAL_equijoin:
		{
			r=equijoin();
			break;
		}
		case LITERAL_for:
		{
			r=forExpr();
			break;
		}
		case LITERAL_if:
		{
			r=ifExpr();
			break;
		}
		case LITERAL_unroll:
		{
			r=unroll();
			break;
		}
		case LITERAL_fn:
		{
			r=function();
			break;
		}
		case LITERAL_cmp:
		case 22:
		case 24:
		case 47:
		case LITERAL_not:
		case LITERAL_isnull:
		case LITERAL_exists:
		case LITERAL_isdefined:
		case 65:
		case 66:
		case LITERAL_type:
		case INT:
		case DEC:
		case DOUBLE:
		case HEXSTR:
		case DATETIME:
		case LITERAL_true:
		case LITERAL_false:
		case LITERAL_null:
		case STR:
		case HERE_STRING:
		case BLOCK_STRING:
		case VAR:
		case ID:
		{
			r=orExpr();
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		return r;
	}
	
	public final Expr  op(
		Expr in
	) throws RecognitionException, TokenStreamException {
		Expr r=null;
		
		BindingExpr b=null;
		
		switch ( LA(1)) {
		case LITERAL_filter:
		{
			match(LITERAL_filter);
			b=each(in);
			r=expr();
			r = new FilterExpr(b, r);    env.unscope(b.var);
			break;
		}
		case LITERAL_transform:
		{
			match(LITERAL_transform);
			b=each(in);
			r=expr();
			r = new TransformExpr(b, r); env.unscope(b.var);
			break;
		}
		case LITERAL_expand:
		{
			match(LITERAL_expand);
			b=each(in);
			{
			switch ( LA(1)) {
			case LITERAL_group:
			case LITERAL_cmp:
			case 22:
			case 24:
			case LITERAL_join:
			case LITERAL_equijoin:
			case LITERAL_if:
			case LITERAL_unroll:
			case LITERAL_fn:
			case LITERAL_for:
			case 47:
			case LITERAL_not:
			case LITERAL_isnull:
			case LITERAL_exists:
			case LITERAL_isdefined:
			case 65:
			case 66:
			case LITERAL_type:
			case INT:
			case DEC:
			case DOUBLE:
			case HEXSTR:
			case DATETIME:
			case LITERAL_true:
			case LITERAL_false:
			case LITERAL_null:
			case STR:
			case HERE_STRING:
			case BLOCK_STRING:
			case VAR:
			case ID:
			{
				r=expr();
				break;
			}
			case EOF:
			case SEMI:
			case 8:
			case 9:
			case 23:
			case 25:
			case 40:
			{
				r = new VarExpr(b.var);
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
			}
			r = new ForExpr(b, r);       env.unscope(b.var);
			break;
		}
		case LITERAL_group:
		{
			r=groupPipe(in);
			break;
		}
		case LITERAL_sort:
		{
			r=sort(in);
			break;
		}
		case LITERAL_top:
		{
			r=top(in);
			break;
		}
		case LITERAL_split:
		{
			r=split(in);
			break;
		}
		case LITERAL_aggregate:
		case LITERAL_agg:
		{
			r=aggregate(in);
			break;
		}
		case VAR:
		case ID:
		{
			r=call(in);
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		return r;
	}
	
	public final Expr  pipeFn() throws RecognitionException, TokenStreamException {
		Expr r=null;
		
		
		Var v = env.makeVar("$");
		
		
		r=subpipe(v);
		
		ArrayList<Var> p = new ArrayList<Var>();
		p.add(v);
		r = new DefineFunctionExpr(null, p, r); // TODO: use a diff class
		
		return r;
	}
	
	public final Expr  subpipe(
		Var inVar
	) throws RecognitionException, TokenStreamException {
		Expr r=null;
		
		
			inVar.hidden = true;
		//        r=new PipeInput(new VarExpr(inVar));
		r= new VarExpr(inVar);
		
		
		{
		int _cnt14=0;
		_loop14:
		do {
			if ((LA(1)==9)) {
				match(9);
				r=op(r);
			}
			else {
				if ( _cnt14>=1 ) { break _loop14; } else {throw new NoViableAltException(LT(1), getFilename());}
			}
			
			_cnt14++;
		} while (true);
		}
		return r;
	}
	
	public final BindingExpr  vpipe() throws RecognitionException, TokenStreamException {
		BindingExpr r=null;
		
		String v; Expr e=null;
		
		v=var();
		{
		switch ( LA(1)) {
		case 8:
		case LITERAL_where:
		case LITERAL_on:
		{
			e = new VarExpr(env.inscope(v));
			break;
		}
		case LITERAL_in:
		{
			match(LITERAL_in);
			e=expr();
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		}
		r = new BindingExpr(BindingExpr.Type.IN, env.scope(v), null, e);
		return r;
	}
	
	public final Expr  aggregate(
		Expr in
	) throws RecognitionException, TokenStreamException {
		Expr r=null;
		
		BindingExpr b;
		
		{
		switch ( LA(1)) {
		case LITERAL_aggregate:
		{
			match(LITERAL_aggregate);
			break;
		}
		case LITERAL_agg:
		{
			match(LITERAL_agg);
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		}
		b=each(in);
		r=expr();
		r = AggregateExpr.make(env, b.var, b.inExpr(), r);
		return r;
	}
	
	public final BindingExpr  each(
		Expr in
	) throws RecognitionException, TokenStreamException {
		BindingExpr b=null;
		
		String v = "$";
		
		{
		switch ( LA(1)) {
		case LITERAL_each:
		{
			match(LITERAL_each);
			v=var();
			break;
		}
		case EOF:
		case SEMI:
		case 8:
		case 9:
		case LITERAL_group:
		case LITERAL_using:
		case LITERAL_as:
		case LITERAL_by:
		case LITERAL_into:
		case LITERAL_expand:
		case LITERAL_cmp:
		case 22:
		case 23:
		case 24:
		case 25:
		case LITERAL_join:
		case LITERAL_equijoin:
		case LITERAL_if:
		case LITERAL_else:
		case LITERAL_unroll:
		case 40:
		case LITERAL_fn:
		case LITERAL_for:
		case 47:
		case LITERAL_not:
		case LITERAL_isnull:
		case LITERAL_exists:
		case LITERAL_isdefined:
		case 65:
		case 66:
		case LITERAL_type:
		case INT:
		case DEC:
		case DOUBLE:
		case HEXSTR:
		case DATETIME:
		case LITERAL_true:
		case LITERAL_false:
		case LITERAL_null:
		case STR:
		case HERE_STRING:
		case BLOCK_STRING:
		case VAR:
		case ID:
		{
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		}
		b = new BindingExpr(BindingExpr.Type.IN, env.scope(v), null, in);
		return b;
	}
	
	public final Expr  group() throws RecognitionException, TokenStreamException {
		Expr r=null;
		
		
			  BindingExpr in;
			  BindingExpr by = null;
			  Expr c=null;
			  String v = "$";
			  ArrayList<Var> as = new ArrayList<Var>();
			
		
		match(LITERAL_group);
		{
		switch ( LA(1)) {
		case LITERAL_each:
		{
			match(LITERAL_each);
			v=var();
			match(LITERAL_in);
			break;
		}
		case LITERAL_group:
		case LITERAL_cmp:
		case 22:
		case 24:
		case LITERAL_join:
		case LITERAL_equijoin:
		case LITERAL_if:
		case LITERAL_unroll:
		case LITERAL_fn:
		case LITERAL_for:
		case 47:
		case LITERAL_not:
		case LITERAL_isnull:
		case LITERAL_exists:
		case LITERAL_isdefined:
		case 65:
		case 66:
		case LITERAL_type:
		case INT:
		case DEC:
		case DOUBLE:
		case HEXSTR:
		case DATETIME:
		case LITERAL_true:
		case LITERAL_false:
		case LITERAL_null:
		case STR:
		case HERE_STRING:
		case BLOCK_STRING:
		case VAR:
		case ID:
		{
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		}
		
			      in=new BindingExpr(BindingExpr.Type.IN, env.makeVar(v), null, Expr.NO_EXPRS); 
			
		by=groupIn(in,by,as);
		{
		_loop23:
		do {
			if ((LA(1)==8)) {
				match(8);
				by=groupIn(in,by,as);
			}
			else {
				break _loop23;
			}
			
		} while (true);
		}
		if( by.var != Var.unused ) env.scope(by.var);
		{
		switch ( LA(1)) {
		case LITERAL_using:
		{
			match(LITERAL_using);
			c=comparator();
			oops("comparators on group by NYI");
			break;
		}
		case LITERAL_into:
		case LITERAL_expand:
		{
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		}
		
		for( Var av: as )
		{
		env.scope(av);
		}
			
		r=groupReturn();
		
			      if( by.var != Var.unused ) env.unscope(by.var);
			      for( Var av: as )
		{
		env.scope(av);
		}
		r = new GroupByExpr(in, by, as, c, r); // .expand(env);
			
		return r;
	}
	
	public final BindingExpr  groupIn(
		BindingExpr in, BindingExpr prevBy, ArrayList<Var> asVars
	) throws RecognitionException, TokenStreamException {
		BindingExpr by;
		
		Expr e; String v=null;
		
		e=expr();
		env.scope(in.var);
		by=groupBy(prevBy);
		{
		switch ( LA(1)) {
		case LITERAL_as:
		{
			match(LITERAL_as);
			v=var();
			break;
		}
		case 8:
		case LITERAL_using:
		case LITERAL_into:
		case LITERAL_expand:
		{
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		}
		
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
		
		return by;
	}
	
	public final Expr  comparator() throws RecognitionException, TokenStreamException {
		Expr r=null;
		
		String s; Expr e;
		
		switch ( LA(1)) {
		case 24:
		{
			r=cmpArrayFn("$");
			break;
		}
		case STR:
		case HERE_STRING:
		case BLOCK_STRING:
		case ID:
		{
			{
			switch ( LA(1)) {
			case ID:
			{
				s=name();
				break;
			}
			case STR:
			case HERE_STRING:
			case BLOCK_STRING:
			{
				s=str();
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
			}
			oops("named comparators NYI: "+s);
			break;
		}
		case LITERAL_cmp:
		{
			r=cmpFnExpr();
			break;
		}
		case VAR:
		{
			r=varExpr();
			break;
		}
		case 22:
		{
			r=parenExpr();
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		return r;
	}
	
	public final Expr  groupReturn() throws RecognitionException, TokenStreamException {
		Expr r=null;
		
		
		switch ( LA(1)) {
		case LITERAL_into:
		{
			match(LITERAL_into);
			r=expr();
			r = new ArrayExpr(r);
			break;
		}
		case LITERAL_expand:
		{
			match(LITERAL_expand);
			r=expr();
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		return r;
	}
	
	public final BindingExpr  groupBy(
		BindingExpr by
	) throws RecognitionException, TokenStreamException {
		BindingExpr b=null;
		
		String v = null; Expr e=null;
		
		{
		switch ( LA(1)) {
		case LITERAL_by:
		{
			match(LITERAL_by);
			{
			switch ( LA(1)) {
			case AVAR:
			{
				v=avar();
				match(18);
				break;
			}
			case LITERAL_group:
			case LITERAL_cmp:
			case 22:
			case 24:
			case LITERAL_join:
			case LITERAL_equijoin:
			case LITERAL_if:
			case LITERAL_unroll:
			case LITERAL_fn:
			case LITERAL_for:
			case 47:
			case LITERAL_not:
			case LITERAL_isnull:
			case LITERAL_exists:
			case LITERAL_isdefined:
			case 65:
			case 66:
			case LITERAL_type:
			case INT:
			case DEC:
			case DOUBLE:
			case HEXSTR:
			case DATETIME:
			case LITERAL_true:
			case LITERAL_false:
			case LITERAL_null:
			case STR:
			case HERE_STRING:
			case BLOCK_STRING:
			case VAR:
			case ID:
			{
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
			}
			e=expr();
			break;
		}
		case 8:
		case LITERAL_using:
		case LITERAL_as:
		case LITERAL_into:
		case LITERAL_expand:
		{
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		}
		
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
		
		return b;
	}
	
	public final String  avar() throws RecognitionException, TokenStreamException {
		String r=null;
		
		Token  n = null;
		
		n = LT(1);
		match(AVAR);
		r = n.getText();
		return r;
	}
	
	public final Expr  groupPipe(
		Expr in
	) throws RecognitionException, TokenStreamException {
		Expr r=null;
		
		
		BindingExpr b; BindingExpr by=null; Expr key=null;  Expr c; 
		String v="$"; Var asVar; 
		
		
		match(LITERAL_group);
		b=each(in);
		by=groupBy(null);
		env.unscope(b.var); if( by.var != Var.unused ) env.scope(by.var);
		{
		switch ( LA(1)) {
		case LITERAL_as:
		{
			match(LITERAL_as);
			v=var();
			break;
		}
		case LITERAL_using:
		case LITERAL_into:
		case LITERAL_expand:
		{
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		}
		asVar=env.scope(v);
		{
		switch ( LA(1)) {
		case LITERAL_using:
		{
			match(LITERAL_using);
			c=comparator();
			oops("comparators on group by NYI");
			break;
		}
		case LITERAL_into:
		case LITERAL_expand:
		{
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		}
		r=groupReturn();
		
		if( by.var != Var.unused ) env.unscope(by.var);
		env.unscope(asVar);
		r = new GroupByExpr(b,by,asVar,null,r);
		
		return r;
	}
	
	public final Expr  cmpArrayFn(
		String vn
	) throws RecognitionException, TokenStreamException {
		Expr r=null;
		
		Var var=env.scope(vn);
		
		r=cmpArray();
		
		env.unscope(var);
		r = new DefineFunctionExpr(null, new Var[]{var}, r); // TODO: DefineCmpFn()? Add Cmp type?
		
		return r;
	}
	
	public final String  name() throws RecognitionException, TokenStreamException {
		String r=null;
		
		Token  n = null;
		
		n = LT(1);
		match(ID);
		r = n.getText();
		return r;
	}
	
	public final String  str() throws RecognitionException, TokenStreamException {
		String r=null;
		
		Token  s = null;
		Token  h = null;
		Token  b = null;
		
		switch ( LA(1)) {
		case STR:
		{
			s = LT(1);
			match(STR);
			r = s.getText();
			break;
		}
		case HERE_STRING:
		{
			h = LT(1);
			match(HERE_STRING);
			r = h.getText();
			break;
		}
		case BLOCK_STRING:
		{
			b = LT(1);
			match(BLOCK_STRING);
			r = b.getText();
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		return r;
	}
	
	public final Expr  cmpFnExpr() throws RecognitionException, TokenStreamException {
		Expr r=null;
		
		String v; Var var;
		
		match(LITERAL_cmp);
		match(22);
		v=var();
		match(23);
		r=cmpArrayFn(v);
		return r;
	}
	
	public final Expr  varExpr() throws RecognitionException, TokenStreamException {
		Expr r = null;
		
		String v;
		
		v=var();
		r = new VarExpr( env.inscope(v) );
		return r;
	}
	
	public final Expr  parenExpr() throws RecognitionException, TokenStreamException {
		Expr r=null;
		
		
		match(22);
		r=block();
		match(23);
		return r;
	}
	
	public final Expr  cmpExpr() throws RecognitionException, TokenStreamException {
		Expr r=null;
		
		String v; Var var;
		
		match(LITERAL_cmp);
		{
		switch ( LA(1)) {
		case 22:
		{
			match(22);
			v=var();
			match(23);
			r=cmpArrayFn(v);
			break;
		}
		case 24:
		{
			r=cmpArray();
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		}
		return r;
	}
	
	public final CmpArray  cmpArray() throws RecognitionException, TokenStreamException {
		CmpArray r=null;
		
		Var var; ArrayList<CmpSpec> keys = new ArrayList<CmpSpec>();
		
		match(24);
		{
		switch ( LA(1)) {
		case LITERAL_group:
		case LITERAL_cmp:
		case 22:
		case 24:
		case LITERAL_join:
		case LITERAL_equijoin:
		case LITERAL_if:
		case LITERAL_unroll:
		case LITERAL_fn:
		case LITERAL_for:
		case 47:
		case LITERAL_not:
		case LITERAL_isnull:
		case LITERAL_exists:
		case LITERAL_isdefined:
		case 65:
		case 66:
		case LITERAL_type:
		case INT:
		case DEC:
		case DOUBLE:
		case HEXSTR:
		case DATETIME:
		case LITERAL_true:
		case LITERAL_false:
		case LITERAL_null:
		case STR:
		case HERE_STRING:
		case BLOCK_STRING:
		case VAR:
		case ID:
		{
			cmpSpec(keys);
			{
			_loop44:
			do {
				if ((LA(1)==8)) {
					match(8);
					{
					switch ( LA(1)) {
					case LITERAL_group:
					case LITERAL_cmp:
					case 22:
					case 24:
					case LITERAL_join:
					case LITERAL_equijoin:
					case LITERAL_if:
					case LITERAL_unroll:
					case LITERAL_fn:
					case LITERAL_for:
					case 47:
					case LITERAL_not:
					case LITERAL_isnull:
					case LITERAL_exists:
					case LITERAL_isdefined:
					case 65:
					case 66:
					case LITERAL_type:
					case INT:
					case DEC:
					case DOUBLE:
					case HEXSTR:
					case DATETIME:
					case LITERAL_true:
					case LITERAL_false:
					case LITERAL_null:
					case STR:
					case HERE_STRING:
					case BLOCK_STRING:
					case VAR:
					case ID:
					{
						cmpSpec(keys);
						break;
					}
					case 8:
					case 25:
					{
						break;
					}
					default:
					{
						throw new NoViableAltException(LT(1), getFilename());
					}
					}
					}
				}
				else {
					break _loop44;
				}
				
			} while (true);
			}
			break;
		}
		case 25:
		{
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		}
		match(25);
		
		r = new CmpArray(keys);
		
		return r;
	}
	
	public final void cmpSpec(
		ArrayList<CmpSpec> keys
	) throws RecognitionException, TokenStreamException {
		
		Expr e; Expr c=null; CmpSpec.Order o = CmpSpec.Order.ASC;
		
		e=expr();
		{
		switch ( LA(1)) {
		case LITERAL_using:
		{
			match(LITERAL_using);
			c=comparator();
			oops("nested comparators NYI");
			break;
		}
		case 8:
		case 25:
		case LITERAL_asc:
		case LITERAL_desc:
		{
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		}
		{
		switch ( LA(1)) {
		case LITERAL_asc:
		{
			match(LITERAL_asc);
			break;
		}
		case LITERAL_desc:
		{
			match(LITERAL_desc);
			o = CmpSpec.Order.DESC;
			break;
		}
		case 8:
		case 25:
		{
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		}
		
		keys.add( new CmpSpec(e, o) );
		
	}
	
	public final Expr  join() throws RecognitionException, TokenStreamException {
		Expr r=null;
		
		
		ArrayList<BindingExpr> in = new ArrayList<BindingExpr>();
		HashMap<String,Var> keys = new HashMap<String,Var>();
		Expr p; 
		BindingExpr b;
		
		
		match(LITERAL_join);
		b=joinIn();
		in.add(b); b.var.hidden=true;
		{
		int _cnt50=0;
		_loop50:
		do {
			if ((LA(1)==8)) {
				match(8);
				b=joinIn();
				in.add(b); b.var.hidden=true;
			}
			else {
				if ( _cnt50>=1 ) { break _loop50; } else {throw new NoViableAltException(LT(1), getFilename());}
			}
			
			_cnt50++;
		} while (true);
		}
		
		for( BindingExpr b2: in )
		{
		b2.var.hidden = false;
		}
		
		match(LITERAL_where);
		p=expr();
		{
		switch ( LA(1)) {
		case LITERAL_into:
		{
			match(LITERAL_into);
			r=expr();
			r = new ArrayExpr(r);
			break;
		}
		case LITERAL_expand:
		{
			match(LITERAL_expand);
			r=expr();
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		}
		
		for( BindingExpr b2: in )
		{
		env.unscope(b2.var);
		}
		r = new MultiJoinExpr(in, p, r).expand(env);  
		
		return r;
	}
	
	public final BindingExpr  joinIn() throws RecognitionException, TokenStreamException {
		BindingExpr b=null;
		
		boolean p = false;
		
		{
		switch ( LA(1)) {
		case LITERAL_preserve:
		{
			match(LITERAL_preserve);
			p = true;
			break;
		}
		case VAR:
		{
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		}
		b=vpipe();
		
		b.preserve = p;
		
		return b;
	}
	
	public final Expr  equijoin() throws RecognitionException, TokenStreamException {
		Expr r=null;
		
		
		ArrayList<BindingExpr> in = new ArrayList<BindingExpr>(); 
		ArrayList<Expr> on = new ArrayList<Expr>(); 
		Expr c=null; 
		
		
		match(LITERAL_equijoin);
		ejoinIn(in,on);
		{
		int _cnt56=0;
		_loop56:
		do {
			if ((LA(1)==8)) {
				match(8);
				ejoinIn(in,on);
			}
			else {
				if ( _cnt56>=1 ) { break _loop56; } else {throw new NoViableAltException(LT(1), getFilename());}
			}
			
			_cnt56++;
		} while (true);
		}
		{
		switch ( LA(1)) {
		case LITERAL_using:
		{
			match(LITERAL_using);
			c=comparator();
			oops("comparators on joins are NYI");
			break;
		}
		case LITERAL_into:
		case LITERAL_expand:
		{
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		}
		
			for( BindingExpr b: in )
		{
		b.var.hidden = false;
		}
		
		{
		switch ( LA(1)) {
		case LITERAL_into:
		{
			match(LITERAL_into);
			r=expr();
			r = new ArrayExpr(r);
			break;
		}
		case LITERAL_expand:
		{
			match(LITERAL_expand);
			r=expr();
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		}
		
		r = new JoinExpr(in,on,r); // TODO: add comparator
		for( BindingExpr b: in )
		{
			env.unscope(b.var);
		}
		
		return r;
	}
	
	public final void ejoinIn(
		ArrayList<BindingExpr> in, ArrayList<Expr> on
	) throws RecognitionException, TokenStreamException {
		
		Expr e; BindingExpr b;
		
		b=joinIn();
		in.add(b);
		match(LITERAL_on);
		e=expr();
		on.add(e); b.var.hidden=true;
	}
	
	public final Expr  split(
		Expr in
	) throws RecognitionException, TokenStreamException {
		Expr r=null;
		
		
		ArrayList<Expr> es = new ArrayList<Expr>();
		Var tmpVar = env.makeVar("$split");
		Expr p; Expr e;
		BindingExpr b;
		
		
		match(LITERAL_split);
		b=each(in);
		es.add( b );
		{
		_loop62:
		do {
			if ((LA(1)==LITERAL_if)) {
				match(LITERAL_if);
				b.var.hidden=false;
				match(22);
				p=expr();
				match(23);
				b.var.hidden=true;
				e=expr();
				es.add(new IfExpr(p,e));
			}
			else {
				break _loop62;
			}
			
		} while (true);
		}
		{
		switch ( LA(1)) {
		case LITERAL_else:
		{
			match(LITERAL_else);
			b.var.hidden=true;
			e=expr();
			es.add(new IfExpr(new ConstExpr(JBool.trueItem), e) );
			break;
		}
		case EOF:
		case SEMI:
		case 8:
		case 9:
		case 23:
		case 25:
		case 40:
		{
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		}
		
		r = new SplitExpr(es);
		env.unscope(b.var);
		
		return r;
	}
	
	public final Expr  sort(
		Expr in
	) throws RecognitionException, TokenStreamException {
		Expr r=null;
		
		
		match(LITERAL_sort);
		r=sortCmp();
		r = new SortExpr(in, r);
		return r;
	}
	
	public final Expr  top(
		Expr in
	) throws RecognitionException, TokenStreamException {
		Expr r=null;
		
		Expr n; Expr by=null;
		
		match(LITERAL_top);
		n=expr();
		{
		switch ( LA(1)) {
		case LITERAL_each:
		case LITERAL_using:
		case LITERAL_by:
		{
			by=sortCmp();
			break;
		}
		case EOF:
		case SEMI:
		case 8:
		case 9:
		case 23:
		case 25:
		case 40:
		{
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		}
		
			// TODO: add heap-based top operator
			if( by != null )
			{
		in = new SortExpr(in, by);
			}
			r = new PathExpr(in, new PathArrayHead(new MathExpr(MathExpr.MINUS, n, new ConstExpr(JLong.ONE_ITEM))));
		
		return r;
	}
	
	public final Expr  call(
		Expr in
	) throws RecognitionException, TokenStreamException {
		Expr r=null;
		
		String n; ArrayList<Expr> args;
		
		switch ( LA(1)) {
		case VAR:
		{
			n=var();
			{
			args=fnArgs();
			args.add(0, in); r = new FunctionCallExpr(new VarExpr(env.inscope(n)), args);
			{
			_loop71:
			do {
				if ((LA(1)==22)) {
					args=fnArgs();
					r = new FunctionCallExpr(r, args);
				}
				else {
					break _loop71;
				}
				
			} while (true);
			}
			}
			break;
		}
		case ID:
		{
			n=name();
			args=fnArgs();
			args.add(0, in); r = FunctionLib.lookup(env, n, args);
			{
			_loop73:
			do {
				if ((LA(1)==22)) {
					args=fnArgs();
					r = new FunctionCallExpr(r, args);
				}
				else {
					break _loop73;
				}
				
			} while (true);
			}
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		return r;
	}
	
	public final ArrayList<Expr>  fnArgs() throws RecognitionException, TokenStreamException {
		ArrayList<Expr> r = new ArrayList<Expr>();
		
		
		match(22);
		r=exprList();
		match(23);
		
			for(Expr e: r)
			{
				if( e instanceof AssignExpr )
				{
					oops("Call by name NYI");
				}
			}
		
		return r;
	}
	
	public final Expr  sortCmp() throws RecognitionException, TokenStreamException {
		Expr r=null;
		
		String v="$";
		
		switch ( LA(1)) {
		case LITERAL_each:
		case LITERAL_by:
		{
			{
			switch ( LA(1)) {
			case LITERAL_each:
			{
				match(LITERAL_each);
				v=var();
				break;
			}
			case LITERAL_by:
			{
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
			}
			match(LITERAL_by);
			r=cmpArrayFn(v);
			break;
		}
		case LITERAL_using:
		{
			match(LITERAL_using);
			r=comparator();
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		return r;
	}
	
	public final Expr  unroll() throws RecognitionException, TokenStreamException {
		Expr r=null;
		
		ArrayList<Expr> args = new ArrayList<Expr>();
		
		match(LITERAL_unroll);
		r=fnCall();
		args.add(r);
		{
		int _cnt78=0;
		_loop78:
		do {
			if ((LA(1)==24||LA(1)==DOT||LA(1)==DOT_ID)) {
				r=estep();
				args.add(r);
			}
			else {
				if ( _cnt78>=1 ) { break _loop78; } else {throw new NoViableAltException(LT(1), getFilename());}
			}
			
			_cnt78++;
		} while (true);
		}
		
		r = new UnrollExpr(args);
		
		return r;
	}
	
	public final Expr  fnCall() throws RecognitionException, TokenStreamException {
		Expr r=null;
		
		String s; ArrayList<Expr> args;
		
		{
		switch ( LA(1)) {
		case LITERAL_cmp:
		case 22:
		case 24:
		case 47:
		case INT:
		case DEC:
		case DOUBLE:
		case HEXSTR:
		case DATETIME:
		case LITERAL_true:
		case LITERAL_false:
		case LITERAL_null:
		case STR:
		case HERE_STRING:
		case BLOCK_STRING:
		case VAR:
		{
			r=basic();
			break;
		}
		case ID:
		{
			r=builtinCall();
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		}
		{
		_loop177:
		do {
			if ((LA(1)==22)) {
				args=fnArgs();
				r = new FunctionCallExpr(r, args);
			}
			else {
				break _loop177;
			}
			
		} while (true);
		}
		return r;
	}
	
	public final Expr  estep() throws RecognitionException, TokenStreamException {
		Expr r = null;
		
		
		switch ( LA(1)) {
		case DOT:
		case DOT_ID:
		{
			r=projName();
			r = new UnrollField(r);
			break;
		}
		case 24:
		{
			match(24);
			r=expr();
			match(25);
			r = new UnrollIndex(r);
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		return r;
	}
	
	public final Expr  projName() throws RecognitionException, TokenStreamException {
		Expr r=null;
		
		
		switch ( LA(1)) {
		case DOT_ID:
		{
			r=dotName();
			break;
		}
		case DOT:
		{
			match(DOT);
			r=basic();
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		return r;
	}
	
	public final Expr  assign() throws RecognitionException, TokenStreamException {
		Expr r=null;
		
		String v;
		
		switch ( LA(1)) {
		case AVAR:
		{
			v=avar();
			match(18);
			r=rvalue();
			r = new AssignExpr(env.sessionEnv().scopeGlobal(v), r);
			break;
		}
		case 9:
		case LITERAL_group:
		case LITERAL_cmp:
		case 22:
		case 24:
		case LITERAL_join:
		case LITERAL_equijoin:
		case LITERAL_if:
		case LITERAL_unroll:
		case LITERAL_fn:
		case LITERAL_for:
		case 47:
		case LITERAL_not:
		case LITERAL_isnull:
		case LITERAL_exists:
		case LITERAL_isdefined:
		case 65:
		case 66:
		case LITERAL_type:
		case INT:
		case DEC:
		case DOUBLE:
		case HEXSTR:
		case DATETIME:
		case LITERAL_true:
		case LITERAL_false:
		case LITERAL_null:
		case STR:
		case HERE_STRING:
		case BLOCK_STRING:
		case VAR:
		case ID:
		{
			r=pipe();
			match(40);
			v=var();
			r = new AssignExpr(env.sessionEnv().scopeGlobal(v), r);
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		return r;
	}
	
	public final Expr  rvalue() throws RecognitionException, TokenStreamException {
		Expr r = null;
		
		
		switch ( LA(1)) {
		case 9:
		case LITERAL_group:
		case LITERAL_cmp:
		case 22:
		case 24:
		case LITERAL_join:
		case LITERAL_equijoin:
		case LITERAL_if:
		case LITERAL_unroll:
		case LITERAL_fn:
		case LITERAL_for:
		case 47:
		case LITERAL_not:
		case LITERAL_isnull:
		case LITERAL_exists:
		case LITERAL_isdefined:
		case 65:
		case 66:
		case LITERAL_type:
		case INT:
		case DEC:
		case DOUBLE:
		case HEXSTR:
		case DATETIME:
		case LITERAL_true:
		case LITERAL_false:
		case LITERAL_null:
		case STR:
		case HERE_STRING:
		case BLOCK_STRING:
		case VAR:
		case ID:
		{
			r=pipe();
			break;
		}
		case LITERAL_extern:
		{
			r=extern();
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		return r;
	}
	
	public final Expr  extern() throws RecognitionException, TokenStreamException {
		Expr r = null;
		
		String lang;
		
		match(LITERAL_extern);
		lang=name();
		match(LITERAL_fn);
		r=expr();
		r = new ExternFunctionExpr(lang, r);
		return r;
	}
	
	public final Expr  function() throws RecognitionException, TokenStreamException {
		Expr r = null;
		
		ArrayList<Var> p;
		
		match(LITERAL_fn);
		p=params();
		r=expr();
		
		r = new DefineFunctionExpr(null, p, r);
		for( Var v: p )
		{
		env.unscope(v);
		}
		
		return r;
	}
	
	public final ArrayList<Var>  params() throws RecognitionException, TokenStreamException {
		ArrayList<Var> p = new ArrayList<Var>();
		
		String v;
		
		match(22);
		{
		switch ( LA(1)) {
		case VAR:
		{
			v=var();
			p.add(env.scope(v));
			{
			_loop92:
			do {
				if ((LA(1)==8)) {
					match(8);
					v=var();
					p.add(env.scope(v));
				}
				else {
					break _loop92;
				}
				
			} while (true);
			}
			break;
		}
		case 23:
		{
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		}
		match(23);
		return p;
	}
	
	public final Expr  forExpr() throws RecognitionException, TokenStreamException {
		Expr r = null;
		
		
			ArrayList<BindingExpr> bs = new ArrayList<BindingExpr>();
		
		
		match(LITERAL_for);
		match(22);
		forDef(bs);
		{
		_loop97:
		do {
			if ((LA(1)==8)) {
				match(8);
				forDef(bs);
			}
			else {
				break _loop97;
			}
			
		} while (true);
		}
		match(23);
		r=expr();
		
		for(int i = 0 ; i < bs.size() ; i++ )
		{
		BindingExpr e = bs.get(i);
		env.unscope(e.var);
		}
		MultiForExpr f = new MultiForExpr(bs, null, r /*new ArrayExpr(r)*/); // TODO: eleminate WHERE, array return, make native multifor
		r = f.expand(env);
		
		return r;
	}
	
	public final void forDef(
		ArrayList<BindingExpr> bindings
	) throws RecognitionException, TokenStreamException {
		
		String v; Expr e; String v2 = null; BindingExpr.Type t = null; BindingExpr b;
		
		v=var();
		{
		match(LITERAL_in);
		e=pipe();
		t = BindingExpr.Type.IN;
		}
		
		Var var = env.scope(v);
		//      Var var2 = null;
		//      if( v2 != null )
		//      {
		//        var2 = env.scope(v2); 
		//      }
		bindings.add( new BindingExpr(t, var, null, e) );
		
	}
	
	public final Expr  ifExpr() throws RecognitionException, TokenStreamException {
		Expr r=null;
		
		Expr p=null; Expr s=null;
		
		match(LITERAL_if);
		match(22);
		p=expr();
		match(23);
		r=expr();
		{
		if ((LA(1)==LITERAL_else)) {
			match(LITERAL_else);
			s=expr();
		}
		else if ((_tokenSet_0.member(LA(1)))) {
		}
		else {
			throw new NoViableAltException(LT(1), getFilename());
		}
		
		}
		
				r = new IfExpr(p, r, s);
			
		return r;
	}
	
	public final RecordExpr  record() throws RecognitionException, TokenStreamException {
		RecordExpr r = null;
		
		ArrayList<FieldExpr> args = new ArrayList<FieldExpr>();
			  FieldExpr f;
		
		match(47);
		{
		switch ( LA(1)) {
		case LITERAL_cmp:
		case 22:
		case 24:
		case 47:
		case INT:
		case DEC:
		case DOUBLE:
		case HEXSTR:
		case DATETIME:
		case LITERAL_true:
		case LITERAL_false:
		case LITERAL_null:
		case STR:
		case HERE_STRING:
		case BLOCK_STRING:
		case VAR:
		case ID:
		case FNAME:
		{
			f=field();
			args.add(f);
			break;
		}
		case 8:
		case 48:
		{
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		}
		{
		_loop109:
		do {
			if ((LA(1)==8)) {
				match(8);
				{
				switch ( LA(1)) {
				case LITERAL_cmp:
				case 22:
				case 24:
				case 47:
				case INT:
				case DEC:
				case DOUBLE:
				case HEXSTR:
				case DATETIME:
				case LITERAL_true:
				case LITERAL_false:
				case LITERAL_null:
				case STR:
				case HERE_STRING:
				case BLOCK_STRING:
				case VAR:
				case ID:
				case FNAME:
				{
					f=field();
					args.add(f);
					break;
				}
				case 8:
				case 48:
				{
					break;
				}
				default:
				{
					throw new NoViableAltException(LT(1), getFilename());
				}
				}
				}
			}
			else {
				break _loop109;
			}
			
		} while (true);
		}
		match(48);
		r = new RecordExpr(args.toArray(new Expr[args.size()]));
		return r;
	}
	
	public final FieldExpr  field() throws RecognitionException, TokenStreamException {
		FieldExpr f=null;
		
		Expr e = null; Expr v=null; boolean required = true;
		
		switch ( LA(1)) {
		case FNAME:
		{
			e=fname();
			{
			switch ( LA(1)) {
			case 49:
			{
				match(49);
				required = false;
				break;
			}
			case 50:
			{
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
			}
			match(50);
			v=expr();
			
				f = new NameValueBinding(e, v, required); 
			
			break;
		}
		case LITERAL_cmp:
		case 22:
		case 24:
		case 47:
		case INT:
		case DEC:
		case DOUBLE:
		case HEXSTR:
		case DATETIME:
		case LITERAL_true:
		case LITERAL_false:
		case LITERAL_null:
		case STR:
		case HERE_STRING:
		case BLOCK_STRING:
		case VAR:
		case ID:
		{
			e=path();
			{
			switch ( LA(1)) {
			case DOT_STAR:
			{
				match(DOT_STAR);
				
					           	 f = new CopyRecord(e);
					
				break;
			}
			case 8:
			case 48:
			case 49:
			case 50:
			{
				{
				switch ( LA(1)) {
				case 49:
				{
					match(49);
					required = false;
					break;
				}
				case 8:
				case 48:
				case 50:
				{
					break;
				}
				default:
				{
					throw new NoViableAltException(LT(1), getFilename());
				}
				}
				}
				{
				switch ( LA(1)) {
				case 50:
				{
					match(50);
					v=expr();
					break;
				}
				case 8:
				case 48:
				{
					break;
				}
				default:
				{
					throw new NoViableAltException(LT(1), getFilename());
				}
				}
				}
				
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
					
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
			}
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		return f;
	}
	
	public final Expr  fname() throws RecognitionException, TokenStreamException {
		Expr r=null;
		
		Token  i = null;
		
		i = LT(1);
		match(FNAME);
		r = new ConstExpr(new JString(i.getText()));
		return r;
	}
	
	public final Expr  path() throws RecognitionException, TokenStreamException {
		Expr r=null;
		
		PathStep s=null; PathStep in=null;
		
		r=fnCall();
		{
		switch ( LA(1)) {
		case 24:
		case 47:
		case 70:
		case DOT:
		case DOT_ID:
		{
			s=step(in = new PathExpr(r));
			{
			_loop150:
			do {
				if ((_tokenSet_1.member(LA(1)))) {
					s=step(s);
				}
				else {
					break _loop150;
				}
				
			} while (true);
			}
			r = in;
			break;
		}
		case EOF:
		case SEMI:
		case 8:
		case 9:
		case LITERAL_in:
		case LITERAL_each:
		case LITERAL_using:
		case LITERAL_as:
		case LITERAL_by:
		case LITERAL_into:
		case LITERAL_expand:
		case 23:
		case 25:
		case LITERAL_asc:
		case LITERAL_desc:
		case LITERAL_where:
		case LITERAL_on:
		case LITERAL_if:
		case LITERAL_else:
		case 40:
		case 48:
		case 49:
		case 50:
		case DOT_STAR:
		case LITERAL_or:
		case LITERAL_and:
		case 58:
		case 59:
		case 60:
		case 61:
		case 62:
		case 63:
		case LITERAL_instanceof:
		case 65:
		case 66:
		case 67:
		case 68:
		{
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		}
		return r;
	}
	
	public final Expr  orExpr() throws RecognitionException, TokenStreamException {
		Expr r;
		
		Expr s;
		
		r=andExpr();
		{
		_loop118:
		do {
			if ((LA(1)==LITERAL_or)) {
				match(LITERAL_or);
				s=andExpr();
				r = new OrExpr(r,s);
			}
			else {
				break _loop118;
			}
			
		} while (true);
		}
		return r;
	}
	
	public final Expr  andExpr() throws RecognitionException, TokenStreamException {
		Expr r;
		
		Expr s;
		
		r=notExpr();
		{
		_loop121:
		do {
			if ((LA(1)==LITERAL_and)) {
				match(LITERAL_and);
				s=notExpr();
				r = new AndExpr(r,s);
			}
			else {
				break _loop121;
			}
			
		} while (true);
		}
		return r;
	}
	
	public final Expr  notExpr() throws RecognitionException, TokenStreamException {
		Expr r;
		
		
		switch ( LA(1)) {
		case LITERAL_not:
		{
			match(LITERAL_not);
			r=notExpr();
			r = new NotExpr(r);
			break;
		}
		case LITERAL_cmp:
		case 22:
		case 24:
		case 47:
		case LITERAL_isnull:
		case LITERAL_exists:
		case LITERAL_isdefined:
		case 65:
		case 66:
		case LITERAL_type:
		case INT:
		case DEC:
		case DOUBLE:
		case HEXSTR:
		case DATETIME:
		case LITERAL_true:
		case LITERAL_false:
		case LITERAL_null:
		case STR:
		case HERE_STRING:
		case BLOCK_STRING:
		case VAR:
		case ID:
		{
			r=kwTest();
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		return r;
	}
	
	public final Expr  kwTest() throws RecognitionException, TokenStreamException {
		Expr r;
		
		
		switch ( LA(1)) {
		case LITERAL_cmp:
		case 22:
		case 24:
		case 47:
		case 65:
		case 66:
		case LITERAL_type:
		case INT:
		case DEC:
		case DOUBLE:
		case HEXSTR:
		case DATETIME:
		case LITERAL_true:
		case LITERAL_false:
		case LITERAL_null:
		case STR:
		case HERE_STRING:
		case BLOCK_STRING:
		case VAR:
		case ID:
		{
			r=inExpr();
			break;
		}
		case LITERAL_isnull:
		{
			r=isnullExpr();
			break;
		}
		case LITERAL_isdefined:
		{
			r=isdefinedExpr();
			break;
		}
		case LITERAL_exists:
		{
			r=existexpr();
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		return r;
	}
	
	public final Expr  inExpr() throws RecognitionException, TokenStreamException {
		Expr r = null;
		
		Expr s;
		
		r=compare();
		{
		switch ( LA(1)) {
		case LITERAL_in:
		{
			match(LITERAL_in);
			s=compare();
			r = new InExpr(r,s);
			break;
		}
		case EOF:
		case SEMI:
		case 8:
		case 9:
		case LITERAL_each:
		case LITERAL_using:
		case LITERAL_as:
		case LITERAL_by:
		case LITERAL_into:
		case LITERAL_expand:
		case 23:
		case 25:
		case LITERAL_asc:
		case LITERAL_desc:
		case LITERAL_where:
		case LITERAL_on:
		case LITERAL_if:
		case LITERAL_else:
		case 40:
		case 48:
		case 50:
		case LITERAL_or:
		case LITERAL_and:
		{
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		}
		return r;
	}
	
	public final Expr  isnullExpr() throws RecognitionException, TokenStreamException {
		Expr r;
		
		
		match(LITERAL_isnull);
		r=inExpr();
		r = new IsnullExpr(r);
		return r;
	}
	
	public final Expr  isdefinedExpr() throws RecognitionException, TokenStreamException {
		Expr r;
		
		Expr n;
		
		match(LITERAL_isdefined);
		r=fnCall();
		n=projName();
		r = new IsdefinedExpr(r,n);
		return r;
	}
	
	public final Expr  existexpr() throws RecognitionException, TokenStreamException {
		Expr r;
		
		
		match(LITERAL_exists);
		r=inExpr();
		r = new ExistsFn(r);
		return r;
	}
	
	public final Expr  compare() throws RecognitionException, TokenStreamException {
		Expr r = null;
		
		int c; Expr s; Expr t;
		
		r=instanceOfExpr();
		{
		switch ( LA(1)) {
		case 58:
		case 59:
		case 60:
		case 61:
		case 62:
		case 63:
		{
			c=compareOp();
			s=instanceOfExpr();
			r = new CompareExpr(c,r,s);
			{
			_loop132:
			do {
				if (((LA(1) >= 58 && LA(1) <= 63))) {
					c=compareOp();
					s = s.clone(new VarMap(env));
					t=instanceOfExpr();
					r = new AndExpr( r, new CompareExpr(c,s,t) ); s=t;
				}
				else {
					break _loop132;
				}
				
			} while (true);
			}
			break;
		}
		case EOF:
		case SEMI:
		case 8:
		case 9:
		case LITERAL_in:
		case LITERAL_each:
		case LITERAL_using:
		case LITERAL_as:
		case LITERAL_by:
		case LITERAL_into:
		case LITERAL_expand:
		case 23:
		case 25:
		case LITERAL_asc:
		case LITERAL_desc:
		case LITERAL_where:
		case LITERAL_on:
		case LITERAL_if:
		case LITERAL_else:
		case 40:
		case 48:
		case 50:
		case LITERAL_or:
		case LITERAL_and:
		{
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		}
		return r;
	}
	
	public final Expr  instanceOfExpr() throws RecognitionException, TokenStreamException {
		Expr r = null;
		
		Expr s;
		
		r=addExpr();
		{
		switch ( LA(1)) {
		case LITERAL_instanceof:
		{
			match(LITERAL_instanceof);
			s=addExpr();
			r = new InstanceOfExpr(r,s);
			break;
		}
		case EOF:
		case SEMI:
		case 8:
		case 9:
		case LITERAL_in:
		case LITERAL_each:
		case LITERAL_using:
		case LITERAL_as:
		case LITERAL_by:
		case LITERAL_into:
		case LITERAL_expand:
		case 23:
		case 25:
		case LITERAL_asc:
		case LITERAL_desc:
		case LITERAL_where:
		case LITERAL_on:
		case LITERAL_if:
		case LITERAL_else:
		case 40:
		case 48:
		case 50:
		case LITERAL_or:
		case LITERAL_and:
		case 58:
		case 59:
		case 60:
		case 61:
		case 62:
		case 63:
		{
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		}
		return r;
	}
	
	public final int  compareOp() throws RecognitionException, TokenStreamException {
		int r = -1;
		
		
		switch ( LA(1)) {
		case 58:
		{
			match(58);
			r = CompareExpr.EQ;
			break;
		}
		case 59:
		{
			match(59);
			r = CompareExpr.LT;
			break;
		}
		case 60:
		{
			match(60);
			r = CompareExpr.GT;
			break;
		}
		case 61:
		{
			match(61);
			r = CompareExpr.NE;
			break;
		}
		case 62:
		{
			match(62);
			r = CompareExpr.LE;
			break;
		}
		case 63:
		{
			match(63);
			r = CompareExpr.GE;
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		return r;
	}
	
	public final Expr  addExpr() throws RecognitionException, TokenStreamException {
		Expr r = null;
		
		Expr s; int op;
		
		r=multExpr();
		{
		_loop138:
		do {
			if ((LA(1)==65||LA(1)==66)) {
				op=addOp();
				s=multExpr();
				r = new MathExpr(op,r,s);
			}
			else {
				break _loop138;
			}
			
		} while (true);
		}
		return r;
	}
	
	public final Expr  multExpr() throws RecognitionException, TokenStreamException {
		Expr r = null;
		
		Expr s; int op;
		
		r=unaryAdd();
		{
		_loop142:
		do {
			if ((LA(1)==67||LA(1)==68)) {
				op=multOp();
				s=unaryAdd();
				r = new MathExpr(op,r,s);
			}
			else {
				break _loop142;
			}
			
		} while (true);
		}
		return r;
	}
	
	public final int  addOp() throws RecognitionException, TokenStreamException {
		int op=0;
		
		
		switch ( LA(1)) {
		case 65:
		{
			match(65);
			op=MathExpr.PLUS;
			break;
		}
		case 66:
		{
			match(66);
			op=MathExpr.MINUS;
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		return op;
	}
	
	public final Expr  unaryAdd() throws RecognitionException, TokenStreamException {
		Expr r = null;
		
		
		switch ( LA(1)) {
		case 66:
		{
			match(66);
			r=typeExpr();
			r = MathExpr.negate(r);
			break;
		}
		case LITERAL_cmp:
		case 22:
		case 24:
		case 47:
		case 65:
		case LITERAL_type:
		case INT:
		case DEC:
		case DOUBLE:
		case HEXSTR:
		case DATETIME:
		case LITERAL_true:
		case LITERAL_false:
		case LITERAL_null:
		case STR:
		case HERE_STRING:
		case BLOCK_STRING:
		case VAR:
		case ID:
		{
			{
			switch ( LA(1)) {
			case 65:
			{
				match(65);
				break;
			}
			case LITERAL_cmp:
			case 22:
			case 24:
			case 47:
			case LITERAL_type:
			case INT:
			case DEC:
			case DOUBLE:
			case HEXSTR:
			case DATETIME:
			case LITERAL_true:
			case LITERAL_false:
			case LITERAL_null:
			case STR:
			case HERE_STRING:
			case BLOCK_STRING:
			case VAR:
			case ID:
			{
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
			}
			r=typeExpr();
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		return r;
	}
	
	public final int  multOp() throws RecognitionException, TokenStreamException {
		int op=0;
		
		
		switch ( LA(1)) {
		case 67:
		{
			match(67);
			op=MathExpr.MULTIPLY;
			break;
		}
		case 68:
		{
			match(68);
			op=MathExpr.DIVIDE;
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		return op;
	}
	
	public final Expr  typeExpr() throws RecognitionException, TokenStreamException {
		Expr r = null;
		
		Schema s;
		
		switch ( LA(1)) {
		case LITERAL_type:
		{
			match(LITERAL_type);
			s=type();
			r = new ConstExpr(new JSchema(s));
			break;
		}
		case LITERAL_cmp:
		case 22:
		case 24:
		case 47:
		case INT:
		case DEC:
		case DOUBLE:
		case HEXSTR:
		case DATETIME:
		case LITERAL_true:
		case LITERAL_false:
		case LITERAL_null:
		case STR:
		case HERE_STRING:
		case BLOCK_STRING:
		case VAR:
		case ID:
		{
			r=path();
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		return r;
	}
	
	public final Schema  type() throws RecognitionException, TokenStreamException {
		Schema s = null;
		
		Schema s2 = null; SchemaOr os = null;
		
		s=typeTerm();
		{
		_loop206:
		do {
			if ((LA(1)==88)) {
				match(88);
				s2 = s; s = os = new SchemaOr(); os.addSchema(s2);
				s2=typeTerm();
				os.addSchema(s2);
			}
			else {
				break _loop206;
			}
			
		} while (true);
		}
		return s;
	}
	
	public final PathStep  step(
		PathStep ctx
	) throws RecognitionException, TokenStreamException {
		PathStep r = null;
		
		Expr e; Expr f; ArrayList<PathStep> pn;
		
		{
		switch ( LA(1)) {
		case DOT:
		case DOT_ID:
		{
			e=projName();
			r = new PathFieldValue(e);
			break;
		}
		case 47:
		{
			match(47);
			pn=projNames();
			match(48);
			r = new PathRecord(pn);
			break;
		}
		case 70:
		{
			match(70);
			match(47);
			pn=projNotNames();
			match(48);
			r = new PathNotRecord(pn);
			break;
		}
		case 24:
		{
			match(24);
			{
			switch ( LA(1)) {
			case LITERAL_group:
			case LITERAL_cmp:
			case 22:
			case 24:
			case LITERAL_join:
			case LITERAL_equijoin:
			case LITERAL_if:
			case LITERAL_unroll:
			case LITERAL_fn:
			case LITERAL_for:
			case 47:
			case LITERAL_not:
			case LITERAL_isnull:
			case LITERAL_exists:
			case LITERAL_isdefined:
			case 65:
			case 66:
			case LITERAL_type:
			case INT:
			case DEC:
			case DOUBLE:
			case HEXSTR:
			case DATETIME:
			case LITERAL_true:
			case LITERAL_false:
			case LITERAL_null:
			case STR:
			case HERE_STRING:
			case BLOCK_STRING:
			case VAR:
			case ID:
			{
				e=expr();
				{
				switch ( LA(1)) {
				case 25:
				{
					r = new PathIndex(e);
					break;
				}
				case 50:
				{
					match(50);
					{
					switch ( LA(1)) {
					case LITERAL_group:
					case LITERAL_cmp:
					case 22:
					case 24:
					case LITERAL_join:
					case LITERAL_equijoin:
					case LITERAL_if:
					case LITERAL_unroll:
					case LITERAL_fn:
					case LITERAL_for:
					case 47:
					case LITERAL_not:
					case LITERAL_isnull:
					case LITERAL_exists:
					case LITERAL_isdefined:
					case 65:
					case 66:
					case LITERAL_type:
					case INT:
					case DEC:
					case DOUBLE:
					case HEXSTR:
					case DATETIME:
					case LITERAL_true:
					case LITERAL_false:
					case LITERAL_null:
					case STR:
					case HERE_STRING:
					case BLOCK_STRING:
					case VAR:
					case ID:
					{
						f=expr();
						r = new PathArraySlice(e,f);
						break;
					}
					case 67:
					{
						match(67);
						r = new PathArrayTail(e);
						break;
					}
					default:
					{
						throw new NoViableAltException(LT(1), getFilename());
					}
					}
					}
					break;
				}
				default:
				{
					throw new NoViableAltException(LT(1), getFilename());
				}
				}
				}
				break;
			}
			case 67:
			{
				match(67);
				{
				switch ( LA(1)) {
				case 25:
				{
					r = new PathArrayAll();
					break;
				}
				case 50:
				{
					match(50);
					{
					switch ( LA(1)) {
					case LITERAL_group:
					case LITERAL_cmp:
					case 22:
					case 24:
					case LITERAL_join:
					case LITERAL_equijoin:
					case LITERAL_if:
					case LITERAL_unroll:
					case LITERAL_fn:
					case LITERAL_for:
					case 47:
					case LITERAL_not:
					case LITERAL_isnull:
					case LITERAL_exists:
					case LITERAL_isdefined:
					case 65:
					case 66:
					case LITERAL_type:
					case INT:
					case DEC:
					case DOUBLE:
					case HEXSTR:
					case DATETIME:
					case LITERAL_true:
					case LITERAL_false:
					case LITERAL_null:
					case STR:
					case HERE_STRING:
					case BLOCK_STRING:
					case VAR:
					case ID:
					{
						e=expr();
						r = new PathArrayHead(e);
						break;
					}
					case 67:
					{
						match(67);
						r = new PathArrayAll();
						break;
					}
					default:
					{
						throw new NoViableAltException(LT(1), getFilename());
					}
					}
					}
					break;
				}
				default:
				{
					throw new NoViableAltException(LT(1), getFilename());
				}
				}
				}
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
			}
			match(25);
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		}
		
			ctx.setNext(r);
		
		return r;
	}
	
	public final ArrayList<PathStep>  projNames() throws RecognitionException, TokenStreamException {
		ArrayList<PathStep> names = new ArrayList<PathStep>();
		
		
		{
		switch ( LA(1)) {
		case 67:
		case DOT:
		case DOT_ID:
		{
			wildProjName(names);
			{
			_loop161:
			do {
				if ((LA(1)==8)) {
					match(8);
					wildProjName(names);
				}
				else {
					break _loop161;
				}
				
			} while (true);
			}
			break;
		}
		case 48:
		{
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		}
		return names;
	}
	
	public final ArrayList<PathStep>  projNotNames() throws RecognitionException, TokenStreamException {
		ArrayList<PathStep> names = new ArrayList<PathStep>();
		
		
		{
		switch ( LA(1)) {
		case 67:
		case DOT:
		case DOT_ID:
		{
			wildProjNotName(names);
			{
			_loop170:
			do {
				if ((LA(1)==8)) {
					match(8);
					wildProjNotName(names);
				}
				else {
					break _loop170;
				}
				
			} while (true);
			}
			break;
		}
		case 48:
		{
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		}
		return names;
	}
	
	public final void wildProjName(
		ArrayList<PathStep> names
	) throws RecognitionException, TokenStreamException {
		
		Expr e; PathStep s=null;
		
		{
		switch ( LA(1)) {
		case DOT:
		case DOT_ID:
		{
			e=projName();
			{
			s = new PathOneField(e);
			}
			break;
		}
		case 67:
		{
			match(67);
			s = new PathAllFields();
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		}
		names.add(s);
		{
		_loop166:
		do {
			if ((_tokenSet_1.member(LA(1)))) {
				s=step(s);
			}
			else {
				break _loop166;
			}
			
		} while (true);
		}
	}
	
	public final void wildProjNotName(
		ArrayList<PathStep> names
	) throws RecognitionException, TokenStreamException {
		
		Expr e;
		
		switch ( LA(1)) {
		case DOT:
		case DOT_ID:
		{
			e=projName();
			{
			names.add(new PathOneField(e));
			}
			break;
		}
		case 67:
		{
			match(67);
			names.add(new PathAllFields());
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
	}
	
	public final Expr  dotName() throws RecognitionException, TokenStreamException {
		Expr r = null;
		
		Token  i = null;
		
		i = LT(1);
		match(DOT_ID);
		r = new ConstExpr(new JString(i.getText()));
		return r;
	}
	
	public final Expr  basic() throws RecognitionException, TokenStreamException {
		Expr r=null;
		
		
		switch ( LA(1)) {
		case INT:
		case DEC:
		case DOUBLE:
		case HEXSTR:
		case DATETIME:
		case LITERAL_true:
		case LITERAL_false:
		case LITERAL_null:
		case STR:
		case HERE_STRING:
		case BLOCK_STRING:
		{
			r=constant();
			break;
		}
		case 47:
		{
			r=record();
			break;
		}
		case 24:
		{
			r=array();
			break;
		}
		case VAR:
		{
			r=varExpr();
			break;
		}
		case LITERAL_cmp:
		{
			r=cmpExpr();
			break;
		}
		case 22:
		{
			r=parenExpr();
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		return r;
	}
	
	public final Expr  builtinCall() throws RecognitionException, TokenStreamException {
		Expr r=null;
		
		String s; ArrayList<Expr> args;
		
		s=name();
		args=fnArgs();
		
		r = FunctionLib.lookup(env, s, args);
		
		return r;
	}
	
	public final Expr  constant() throws RecognitionException, TokenStreamException {
		Expr r=null;
		
		Token  i = null;
		Token  n = null;
		Token  d = null;
		Token  h = null;
		Token  t = null;
		String s;
		
		switch ( LA(1)) {
		case STR:
		case HERE_STRING:
		case BLOCK_STRING:
		{
			s=str();
			r = new ConstExpr(new JString(s));
			break;
		}
		case INT:
		{
			i = LT(1);
			match(INT);
			r = new ConstExpr(new JLong(i.getText()));
			break;
		}
		case DEC:
		{
			n = LT(1);
			match(DEC);
			r = new ConstExpr(new JDecimal(n.getText()));
			break;
		}
		case DOUBLE:
		{
			d = LT(1);
			match(DOUBLE);
			r = new ConstExpr(new JDouble(d.getText()));
			break;
		}
		case HEXSTR:
		{
			h = LT(1);
			match(HEXSTR);
			r = new ConstExpr(new JBinary(h.getText()));
			break;
		}
		case DATETIME:
		{
			t = LT(1);
			match(DATETIME);
			r = new ConstExpr(new JDate(t.getText()));
			break;
		}
		case LITERAL_true:
		case LITERAL_false:
		{
			r=boolLit();
			break;
		}
		case LITERAL_null:
		{
			r=nullExpr();
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		return r;
	}
	
	public final Expr  array() throws RecognitionException, TokenStreamException {
		Expr r=null;
		
		ArrayList<Expr> a;
		
		match(24);
		a=exprList2();
		match(25);
		r = new ArrayExpr(a);
		return r;
	}
	
	public final ArrayList<Expr>  exprList2() throws RecognitionException, TokenStreamException {
		ArrayList<Expr> r = new ArrayList<Expr>();
		
		Expr e;
		
		{
		switch ( LA(1)) {
		case 9:
		case LITERAL_group:
		case LITERAL_cmp:
		case 22:
		case 24:
		case LITERAL_join:
		case LITERAL_equijoin:
		case LITERAL_if:
		case LITERAL_unroll:
		case LITERAL_fn:
		case LITERAL_for:
		case 47:
		case LITERAL_not:
		case LITERAL_isnull:
		case LITERAL_exists:
		case LITERAL_isdefined:
		case 65:
		case 66:
		case LITERAL_type:
		case INT:
		case DEC:
		case DOUBLE:
		case HEXSTR:
		case DATETIME:
		case LITERAL_true:
		case LITERAL_false:
		case LITERAL_null:
		case STR:
		case HERE_STRING:
		case BLOCK_STRING:
		case VAR:
		case ID:
		{
			e=pipe();
			r.add(e);
			break;
		}
		case 8:
		case 25:
		{
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		}
		{
		_loop192:
		do {
			if ((LA(1)==8)) {
				match(8);
				{
				switch ( LA(1)) {
				case 9:
				case LITERAL_group:
				case LITERAL_cmp:
				case 22:
				case 24:
				case LITERAL_join:
				case LITERAL_equijoin:
				case LITERAL_if:
				case LITERAL_unroll:
				case LITERAL_fn:
				case LITERAL_for:
				case 47:
				case LITERAL_not:
				case LITERAL_isnull:
				case LITERAL_exists:
				case LITERAL_isdefined:
				case 65:
				case 66:
				case LITERAL_type:
				case INT:
				case DEC:
				case DOUBLE:
				case HEXSTR:
				case DATETIME:
				case LITERAL_true:
				case LITERAL_false:
				case LITERAL_null:
				case STR:
				case HERE_STRING:
				case BLOCK_STRING:
				case VAR:
				case ID:
				{
					e=pipe();
					r.add(e);
					break;
				}
				case 8:
				case 25:
				{
					break;
				}
				default:
				{
					throw new NoViableAltException(LT(1), getFilename());
				}
				}
				}
			}
			else {
				break _loop192;
			}
			
		} while (true);
		}
		return r;
	}
	
	public final ArrayList<Expr>  exprList() throws RecognitionException, TokenStreamException {
		ArrayList<Expr> r = new ArrayList<Expr>();
		
		Expr e;
		
		{
		switch ( LA(1)) {
		case 9:
		case LITERAL_group:
		case LITERAL_cmp:
		case 22:
		case 24:
		case LITERAL_join:
		case LITERAL_equijoin:
		case LITERAL_if:
		case LITERAL_unroll:
		case LITERAL_fn:
		case LITERAL_for:
		case 47:
		case LITERAL_not:
		case LITERAL_isnull:
		case LITERAL_exists:
		case LITERAL_isdefined:
		case 65:
		case 66:
		case LITERAL_type:
		case INT:
		case DEC:
		case DOUBLE:
		case HEXSTR:
		case DATETIME:
		case LITERAL_true:
		case LITERAL_false:
		case LITERAL_null:
		case STR:
		case HERE_STRING:
		case BLOCK_STRING:
		case VAR:
		case ID:
		{
			e=pipe();
			r.add(e);
			{
			_loop187:
			do {
				if ((LA(1)==8)) {
					match(8);
					e=pipe();
					r.add(e);
				}
				else {
					break _loop187;
				}
				
			} while (true);
			}
			break;
		}
		case 23:
		{
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		}
		return r;
	}
	
	public final Expr  boolLit() throws RecognitionException, TokenStreamException {
		Expr r=null;
		
		
		switch ( LA(1)) {
		case LITERAL_true:
		{
			match(LITERAL_true);
			r = new ConstExpr(JBool.trueItem);
			break;
		}
		case LITERAL_false:
		{
			match(LITERAL_false);
			r = new ConstExpr(JBool.falseItem);
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		return r;
	}
	
	public final Expr  nullExpr() throws RecognitionException, TokenStreamException {
		Expr r=null;
		
		
		match(LITERAL_null);
		r = new ConstExpr(Item.nil);
		return r;
	}
	
	public final Expr  simpleField() throws RecognitionException, TokenStreamException {
		Expr f=null;
		
		
		{
		switch ( LA(1)) {
		case FNAME:
		{
			f=fname();
			break;
		}
		case 22:
		{
			match(22);
			f=pipe();
			match(23);
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		}
		match(50);
		return f;
	}
	
	public final Schema  typeTerm() throws RecognitionException, TokenStreamException {
		Schema s = null;
		
		
		switch ( LA(1)) {
		case 67:
		{
			match(67);
			s = new SchemaAny();
			break;
		}
		case 24:
		case 47:
		case LITERAL_type:
		case LITERAL_null:
		case ID:
		{
			s=oneType();
			{
			switch ( LA(1)) {
			case 49:
			{
				match(49);
				s = new SchemaOr(s, new SchemaAtom("null"));
				break;
			}
			case EOF:
			case SEMI:
			case 8:
			case 9:
			case LITERAL_in:
			case LITERAL_each:
			case LITERAL_using:
			case LITERAL_as:
			case LITERAL_by:
			case LITERAL_into:
			case LITERAL_expand:
			case 23:
			case 25:
			case LITERAL_asc:
			case LITERAL_desc:
			case LITERAL_where:
			case LITERAL_on:
			case LITERAL_if:
			case LITERAL_else:
			case 40:
			case 48:
			case 50:
			case LITERAL_or:
			case LITERAL_and:
			case 58:
			case 59:
			case 60:
			case 61:
			case 62:
			case 63:
			case LITERAL_instanceof:
			case 65:
			case 66:
			case 67:
			case 68:
			case 88:
			{
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
			}
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		return s;
	}
	
	public final Schema  oneType() throws RecognitionException, TokenStreamException {
		Schema s = null;
		
		
		switch ( LA(1)) {
		case LITERAL_type:
		case LITERAL_null:
		case ID:
		{
			s=atomType();
			break;
		}
		case 24:
		{
			s=arrayType();
			break;
		}
		case 47:
		{
			s=recordType();
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		return s;
	}
	
	public final SchemaAtom  atomType() throws RecognitionException, TokenStreamException {
		SchemaAtom s = null;
		
		Token  i = null;
		
		switch ( LA(1)) {
		case ID:
		{
			i = LT(1);
			match(ID);
			s = new SchemaAtom(i.getText());
			break;
		}
		case LITERAL_null:
		{
			match(LITERAL_null);
			s = new SchemaAtom("null");
			break;
		}
		case LITERAL_type:
		{
			match(LITERAL_type);
			s = new SchemaAtom("type");
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		return s;
	}
	
	public final SchemaArray  arrayType() throws RecognitionException, TokenStreamException {
		SchemaArray s = new SchemaArray();
		
		Schema head = null; Schema p; Schema q;
		
		match(24);
		{
		switch ( LA(1)) {
		case 24:
		case 47:
		case 67:
		case LITERAL_type:
		case LITERAL_null:
		case ID:
		{
			p=type();
			head = p;
			{
			_loop214:
			do {
				if ((LA(1)==8)) {
					match(8);
					q=type();
					p = p.nextSchema = q;
				}
				else {
					break _loop214;
				}
				
			} while (true);
			}
			arrayRepeat(head,s);
			break;
		}
		case 25:
		{
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		}
		match(25);
		return s;
	}
	
	public final SchemaRecord  recordType() throws RecognitionException, TokenStreamException {
		SchemaRecord s = new SchemaRecord();
		
		SchemaField f;
		
		match(47);
		{
		switch ( LA(1)) {
		case 67:
		case STR:
		case HERE_STRING:
		case BLOCK_STRING:
		case FNAME:
		{
			f=fieldType();
			s.addField(f);
			{
			_loop223:
			do {
				if ((LA(1)==8)) {
					match(8);
					f=fieldType();
					s.addField(f);
				}
				else {
					break _loop223;
				}
				
			} while (true);
			}
			break;
		}
		case 48:
		{
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		}
		match(48);
		return s;
	}
	
	public final void arrayRepeat(
		Schema typeList, SchemaArray s
	) throws RecognitionException, TokenStreamException {
		
		Token  i1 = null;
		Token  i2 = null;
		long lo = 0; long hi = 0;
		
		switch ( LA(1)) {
		case 25:
		{
			s.noRepeat(typeList);
			break;
		}
		case 59:
		case 65:
		case 67:
		{
			{
			switch ( LA(1)) {
			case 67:
			{
				match(67);
				lo = 0; hi = SchemaArray.UNLIMITED;
				break;
			}
			case 65:
			{
				match(65);
				lo = 1; hi = SchemaArray.UNLIMITED;
				break;
			}
			case 59:
			{
				match(59);
				{
				switch ( LA(1)) {
				case 67:
				{
					match(67);
					lo = 0; hi = SchemaArray.UNLIMITED;
					break;
				}
				case INT:
				{
					i1 = LT(1);
					match(INT);
					lo = Long.parseLong(i1.getText());
					{
					switch ( LA(1)) {
					case 60:
					{
						hi = lo;
						break;
					}
					case 8:
					{
						match(8);
						{
						switch ( LA(1)) {
						case 67:
						{
							match(67);
							hi = SchemaArray.UNLIMITED;
							break;
						}
						case INT:
						{
							i2 = LT(1);
							match(INT);
							hi = Long.parseLong(i2.getText());
							break;
						}
						default:
						{
							throw new NoViableAltException(LT(1), getFilename());
						}
						}
						}
						break;
					}
					default:
					{
						throw new NoViableAltException(LT(1), getFilename());
					}
					}
					}
					break;
				}
				default:
				{
					throw new NoViableAltException(LT(1), getFilename());
				}
				}
				}
				match(60);
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
			}
			s.setRepeat(typeList, lo, hi);
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
	}
	
	public final SchemaField  fieldType() throws RecognitionException, TokenStreamException {
		SchemaField f = new SchemaField();
		
		String n; Schema t;
		
		{
		switch ( LA(1)) {
		case STR:
		case HERE_STRING:
		case BLOCK_STRING:
		case FNAME:
		{
			n=constFieldName();
			f.name = new JString(n);
			{
			switch ( LA(1)) {
			case 67:
			{
				match(67);
				f.wildcard = true;
				break;
			}
			case 49:
			{
				match(49);
				f.optional = true;
				break;
			}
			case 50:
			{
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
			}
			break;
		}
		case 67:
		{
			match(67);
			f.name = new JString(""); 
			f.wildcard = true;
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		}
		match(50);
		t=type();
		f.schema = t;
		return f;
	}
	
	public final String  constFieldName() throws RecognitionException, TokenStreamException {
		String s=null;
		
		Token  i = null;
		
		switch ( LA(1)) {
		case FNAME:
		{
			i = LT(1);
			match(FNAME);
			s = i.getText();
			break;
		}
		case STR:
		case HERE_STRING:
		case BLOCK_STRING:
		{
			s=str();
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		return s;
	}
	
	
	public static final String[] _tokenNames = {
		"<0>",
		"EOF",
		"<2>",
		"NULL_TREE_LOOKAHEAD",
		"SEMI",
		"\"explain\"",
		"\"materialize\"",
		"\"quit\"",
		"\",\"",
		"\"->\"",
		"\"in\"",
		"\"aggregate\"",
		"\"agg\"",
		"\"group\"",
		"\"each\"",
		"\"using\"",
		"\"as\"",
		"\"by\"",
		"\"=\"",
		"\"into\"",
		"\"expand\"",
		"\"cmp\"",
		"\"(\"",
		"\")\"",
		"\"[\"",
		"\"]\"",
		"\"asc\"",
		"\"desc\"",
		"\"join\"",
		"\"where\"",
		"\"preserve\"",
		"\"equijoin\"",
		"\"on\"",
		"\"split\"",
		"\"if\"",
		"\"else\"",
		"\"filter\"",
		"\"transform\"",
		"\"top\"",
		"\"unroll\"",
		"\"=>\"",
		"\"extern\"",
		"\"fn\"",
		"\"script\"",
		"\"import\"",
		"\"for\"",
		"\"sort\"",
		"\"{\"",
		"\"}\"",
		"\"?\"",
		"\":\"",
		"DOT_STAR",
		"\"or\"",
		"\"and\"",
		"\"not\"",
		"\"isnull\"",
		"\"exists\"",
		"\"isdefined\"",
		"\"==\"",
		"\"<\"",
		"\">\"",
		"\"!=\"",
		"\"<=\"",
		"\">=\"",
		"\"instanceof\"",
		"\"+\"",
		"\"-\"",
		"\"*\"",
		"\"/\"",
		"\"type\"",
		"\"!\"",
		"DOT",
		"INT",
		"DEC",
		"DOUBLE",
		"HEXSTR",
		"DATETIME",
		"\"true\"",
		"\"false\"",
		"\"null\"",
		"STR",
		"HERE_STRING",
		"BLOCK_STRING",
		"AVAR",
		"VAR",
		"ID",
		"DOT_ID",
		"FNAME",
		"\"|\"",
		"DIGIT",
		"HEX",
		"LETTER",
		"WS",
		"COMMENT",
		"NL",
		"ML_COMMENT",
		"BLOCK_LINE1",
		"HERE_TAG",
		"HERE_LINE",
		"HERE_END",
		"VAR1",
		"SYM",
		"SYM2",
		"STRCHAR",
		"DOTTY",
		"IDWORD"
	};
	
	private static final long[] mk_tokenSet_0() {
		long[] data = { 1408531011715858L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_0 = new BitSet(mk_tokenSet_0());
	private static final long[] mk_tokenSet_1() {
		long[] data = { 140737505132544L, 4194496L, 0L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_1 = new BitSet(mk_tokenSet_1());
	
	}
