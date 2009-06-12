// $ANTLR 2.7.6 (2005-12-22): "jaql.g" -> "JaqlParser.java"$

package com.ibm.jaql.lang.parser;

import java.util.*;

import com.ibm.jaql.lang.core.*;
import com.ibm.jaql.lang.expr.core.*;
import com.ibm.jaql.lang.expr.top.*;

import com.ibm.jaql.json.type.*;
import com.ibm.jaql.json.schema.*;
import com.ibm.jaql.json.util.*;

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

  public Env env = new Env();
  public boolean done;

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

	public final Expr  query() throws RecognitionException, TokenStreamException {
		Expr r = null;
		
		done = false; DefineFunctionExpr f;
		
		switch ( LA(1)) {
		case LITERAL_declare:
		{
			match(LITERAL_declare);
			f=functionDef(true);
			break;
		}
		case LITERAL_explain:
		case LITERAL_materialize:
		case LITERAL_quit:
		case LITERAL_fn:
		case 11:
		case 14:
		case STR:
		case LITERAL_not:
		case 34:
		case 35:
		case 37:
		case LITERAL_true:
		case LITERAL_false:
		case LITERAL_null:
		case INT:
		case DEC:
		case DOUBLE:
		case HEXSTR:
		case DATETIME:
		case LITERAL_for:
		case LITERAL_let:
		case LITERAL_if:
		case LITERAL_join:
		case LITERAL_group:
		case LITERAL_combine:
		case LITERAL_reduce:
		case LITERAL_sort:
		case VAR:
		case AVAR:
		case ID:
		case LITERAL_type:
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
		case EOF:
		{
			match(Token.EOF_TYPE);
			if ( inputState.guessing==0 ) {
				done=true;
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
	
	public final DefineFunctionExpr  functionDef(
		boolean declaring
	) throws RecognitionException, TokenStreamException {
		DefineFunctionExpr fn = null;
		
		String i = null; ArrayList<Var> p; Var fnVar = null; Expr body;
		
		match(LITERAL_fn);
		{
		switch ( LA(1)) {
		case ID:
		{
			i=id();
			if ( inputState.guessing==0 ) {
				fnVar = env.scope(i);
			}
			break;
		}
		case 11:
		{
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		}
		p=params();
		body=expr();
		if ( inputState.guessing==0 ) {
			
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
		return fn;
	}
	
	public final Expr  stmt() throws RecognitionException, TokenStreamException {
		Expr r = null;
		
		String v;
		
		switch ( LA(1)) {
		case AVAR:
		{
			r=assignment();
			break;
		}
		case LITERAL_fn:
		case 11:
		case 14:
		case STR:
		case LITERAL_not:
		case 34:
		case 35:
		case 37:
		case LITERAL_true:
		case LITERAL_false:
		case LITERAL_null:
		case INT:
		case DEC:
		case DOUBLE:
		case HEXSTR:
		case DATETIME:
		case LITERAL_for:
		case LITERAL_let:
		case LITERAL_if:
		case LITERAL_join:
		case LITERAL_group:
		case LITERAL_combine:
		case LITERAL_reduce:
		case LITERAL_sort:
		case VAR:
		case ID:
		case LITERAL_type:
		{
			r=expr();
			if ( inputState.guessing==0 ) {
				r = new QueryExpr(env.importGlobals(r));
			}
			break;
		}
		case LITERAL_explain:
		{
			match(LITERAL_explain);
			r=expr();
			if ( inputState.guessing==0 ) {
				r = new ExplainExpr(env.importGlobals(r));
			}
			break;
		}
		case LITERAL_materialize:
		{
			match(LITERAL_materialize);
			v=var();
			if ( inputState.guessing==0 ) {
				r = new MaterializeExpr(env.sessionEnv().inscope(v));
			}
			break;
		}
		case LITERAL_quit:
		{
			match(LITERAL_quit);
			if ( inputState.guessing==0 ) {
				done=true;
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
	
	public final Expr  assignment() throws RecognitionException, TokenStreamException {
		Expr r = null;
		
		String v;
		
		v=avar();
		match(9);
		r=expr();
		if ( inputState.guessing==0 ) {
			r = new AssignExpr(v, r);
		}
		return r;
	}
	
	public final Expr  expr() throws RecognitionException, TokenStreamException {
		Expr r = null;
		
		
		switch ( LA(1)) {
		case LITERAL_fn:
		case LITERAL_for:
		case LITERAL_let:
		case LITERAL_if:
		case LITERAL_join:
		case LITERAL_group:
		case LITERAL_combine:
		case LITERAL_reduce:
		case LITERAL_sort:
		{
			r=kwExpr();
			break;
		}
		case 11:
		case 14:
		case STR:
		case LITERAL_not:
		case 34:
		case 35:
		case 37:
		case LITERAL_true:
		case LITERAL_false:
		case LITERAL_null:
		case INT:
		case DEC:
		case DOUBLE:
		case HEXSTR:
		case DATETIME:
		case VAR:
		case ID:
		case LITERAL_type:
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
	
	public final String  var() throws RecognitionException, TokenStreamException {
		String s = null;
		
		Token  v = null;
		
		v = LT(1);
		match(VAR);
		if ( inputState.guessing==0 ) {
			s = v.getText();
		}
		return s;
	}
	
	public final String  avar() throws RecognitionException, TokenStreamException {
		String s = null;
		
		Token  v = null;
		
		v = LT(1);
		match(AVAR);
		if ( inputState.guessing==0 ) {
			s = v.getText();
		}
		return s;
	}
	
	public final Expr  kwExpr() throws RecognitionException, TokenStreamException {
		Expr r = null;
		
		
		switch ( LA(1)) {
		case LITERAL_for:
		{
			r=forExpr();
			break;
		}
		case LITERAL_let:
		{
			r=letExpr();
			break;
		}
		case LITERAL_if:
		{
			r=ifExpr();
			break;
		}
		case LITERAL_join:
		{
			r=joinExpr();
			break;
		}
		case LITERAL_group:
		{
			r=groupExpr();
			break;
		}
		case LITERAL_sort:
		{
			r=sortExpr();
			break;
		}
		case LITERAL_combine:
		{
			r=combineExpr();
			break;
		}
		case LITERAL_reduce:
		{
			r=reduceExpr();
			break;
		}
		case LITERAL_fn:
		{
			r=functionDef(false);
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		return r;
	}
	
	public final Expr  orExpr() throws RecognitionException, TokenStreamException {
		Expr r = null;
		
		Expr s;
		
		r=andExpr();
		{
		_loop38:
		do {
			if ((LA(1)==LITERAL_or)) {
				match(LITERAL_or);
				s=andExpr();
				if ( inputState.guessing==0 ) {
					r = new OrExpr(r,s);
				}
			}
			else {
				break _loop38;
			}
			
		} while (true);
		}
		return r;
	}
	
	public final Expr  forExpr() throws RecognitionException, TokenStreamException {
		Expr r = null;
		
		ArrayList<BindingExpr> b = new ArrayList<BindingExpr>();
		
		match(LITERAL_for);
		match(11);
		forDef(b);
		{
		_loop87:
		do {
			if ((LA(1)==12)) {
				match(12);
				forDef(b);
			}
			else {
				break _loop87;
			}
			
		} while (true);
		}
		match(13);
		r=expr();
		if ( inputState.guessing==0 ) {
			
			for(int i = 0 ; i < b.size() ; i++ )
			{
				BindingExpr e = b.get(i);
				env.unscope(e.var);
				  }
			MultiForExpr f = new MultiForExpr(b, null, r); // TODO: eleminate WHERE
			r = f.expand(env);
			
		}
		return r;
	}
	
	public final Expr  letExpr() throws RecognitionException, TokenStreamException {
		Expr r = null;
		
		ArrayList<BindingExpr> b = new ArrayList<BindingExpr>();
		
		match(LITERAL_let);
		match(11);
		letDef(b);
		{
		_loop93:
		do {
			if ((LA(1)==12)) {
				match(12);
				letDef(b);
			}
			else {
				break _loop93;
			}
			
		} while (true);
		}
		match(13);
		r=expr();
		if ( inputState.guessing==0 ) {
			
			for(int i = 0 ; i < b.size() ; i++ )
			{
				BindingExpr e = b.get(i);
				env.unscope(e.var);
				  }
			r = new LetExpr(b, r);
			
		}
		return r;
	}
	
	public final Expr  ifExpr() throws RecognitionException, TokenStreamException {
		Expr r = null;
		
		Expr p; Expr t; Expr f = null;
		
		match(LITERAL_if);
		match(11);
		p=expr();
		match(13);
		t=expr();
		{
		if ((LA(1)==LITERAL_else)) {
			match(LITERAL_else);
			f=expr();
		}
		else if ((_tokenSet_0.member(LA(1)))) {
		}
		else {
			throw new NoViableAltException(LT(1), getFilename());
		}
		
		}
		if ( inputState.guessing==0 ) {
			r = new IfExpr(p,t,f);
		}
		return r;
	}
	
	public final JoinExpr  joinExpr() throws RecognitionException, TokenStreamException {
		JoinExpr r = null;
		
		Expr e; ArrayList<BindingExpr> bs = new ArrayList<BindingExpr>();
		
		match(LITERAL_join);
		match(11);
		joinDef(bs);
		match(12);
		{
		joinDef(bs);
		}
		{
		_loop100:
		do {
			if ((LA(1)==12)) {
				match(12);
				joinDef(bs);
			}
			else {
				break _loop100;
			}
			
		} while (true);
		}
		match(13);
		if ( inputState.guessing==0 ) {
			
				  	for(int i = 0 ; i < bs.size() ; i++ )
				  	{
				  	  BindingExpr b = bs.get(i);
				  	  b.var.hidden = false;
				  	}
				
		}
		e=expr();
		if ( inputState.guessing==0 ) {
			
				    for(int i = 0 ; i < bs.size() ; i++ )
				    {
				      BindingExpr b = bs.get(i);
				  	  env.unscope(b.var);
				    }
			r = new JoinExpr(bs, e);
				
		}
		return r;
	}
	
	public final GroupByExpr  groupExpr() throws RecognitionException, TokenStreamException {
		GroupByExpr r = null;
		
		Expr e; ArrayList<BindingExpr> bs = new ArrayList<BindingExpr>();
		
		match(LITERAL_group);
		match(11);
		groupDef(bs);
		{
		_loop106:
		do {
			if ((LA(1)==12)) {
				match(12);
				{
				switch ( LA(1)) {
				case VAR:
				{
					groupDef(bs);
					break;
				}
				case 12:
				case 13:
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
				break _loop106;
			}
			
		} while (true);
		}
		match(13);
		if ( inputState.guessing==0 ) {
			
				  	bs.get(0).var.hidden = false;
				  	for(int i = 1 ; i < bs.size() ; i++ )
				  	{
				  	  BindingExpr b = bs.get(i);
				  	  b.var2.hidden = false;
				  	}
				
		}
		e=expr();
		if ( inputState.guessing==0 ) {
			
			env.unscope(bs.get(0).var);
				    for(int i = 1 ; i < bs.size() ; i++ )
				    {
			BindingExpr b = bs.get(i);
				  	  env.unscope(b.var2);
				    }
			r = new GroupByExpr(bs, e);
				
		}
		return r;
	}
	
	public final SortExpr  sortExpr() throws RecognitionException, TokenStreamException {
		SortExpr s = null;
		
		
			  String v;
			  Expr e;
			  Var var = null;
			  BindingExpr b = null;
			  ArrayList<OrderExpr> by = new ArrayList<OrderExpr>();
			
		
		match(LITERAL_sort);
		match(11);
		v=var();
		match(LITERAL_in);
		e=expr();
		if ( inputState.guessing==0 ) {
			
				  	var = env.scope(v);
				  	b = new BindingExpr(BindingExpr.Type.IN, var, null, e);
				
		}
		match(LITERAL_by);
		sortSpec(by);
		{
		_loop115:
		do {
			if ((LA(1)==12)) {
				match(12);
				sortSpec(by);
			}
			else {
				break _loop115;
			}
			
		} while (true);
		}
		match(13);
		if ( inputState.guessing==0 ) {
			
				  	env.unscope(var);
				  	s = new SortExpr(b, by);
				
		}
		return s;
	}
	
	public final Expr  combineExpr() throws RecognitionException, TokenStreamException {
		Expr r = null;
		
		String v1, v2; Var var1 = null; Var var2 = null; Expr in; Expr use;
		
		match(LITERAL_combine);
		match(11);
		v1=var();
		match(12);
		v2=var();
		match(LITERAL_in);
		in=expr();
		match(13);
		if ( inputState.guessing==0 ) {
			
				       var1 = env.scope(v1);
				       var2 = env.scope(v2);
				
		}
		use=expr();
		if ( inputState.guessing==0 ) {
			
				       env.unscope(var1);
				       env.unscope(var2);
				   r = new CombineExpr(var1, var2, in, use);
				
		}
		return r;
	}
	
	public final ReduceExpr  reduceExpr() throws RecognitionException, TokenStreamException {
		ReduceExpr r = null;
		
		
			  String v; Expr inExpr; Expr ret; Var inVar = null; int n = 0;
			  BindingExpr a;
			  ArrayList<BindingExpr> aggs = new ArrayList<BindingExpr>(); 
			
		
		match(LITERAL_reduce);
		match(11);
		v=var();
		match(LITERAL_in);
		inExpr=expr();
		if ( inputState.guessing==0 ) {
			
				     	inVar = env.scope(v);
				
		}
		match(LITERAL_into);
		a=agg();
		if ( inputState.guessing==0 ) {
			aggs.add(a);
		}
		{
		_loop111:
		do {
			if ((LA(1)==12)) {
				match(12);
				a=agg();
				if ( inputState.guessing==0 ) {
					aggs.add(a);
				}
			}
			else {
				break _loop111;
			}
			
		} while (true);
		}
		if ( inputState.guessing==0 ) {
			
				     	env.unscope(inVar);
				     	n = aggs.size();
				     	for(int i = 0 ; i < n ; i++)
				     	{
				     		aggs.get(i).var.hidden = false;
				     	}
				
		}
		match(13);
		ret=expr();
		if ( inputState.guessing==0 ) {
			
				     	for(int i = 0 ; i < n ; i++)
				     	{
				 	        env.unscope( aggs.get(i).var );
				     	}
				     	r = new ReduceExpr(inVar, inExpr, aggs, ret);
				
		}
		return r;
	}
	
	public final String  id() throws RecognitionException, TokenStreamException {
		String s = null;
		
		Token  i = null;
		
		i = LT(1);
		match(ID);
		if ( inputState.guessing==0 ) {
			s = i.getText();
		}
		return s;
	}
	
	public final ArrayList<Var>  params() throws RecognitionException, TokenStreamException {
		ArrayList<Var> p = new ArrayList<Var>();
		
		String v;
		
		match(11);
		{
		switch ( LA(1)) {
		case VAR:
		{
			v=var();
			if ( inputState.guessing==0 ) {
				p.add(env.scope(v));
			}
			{
			_loop12:
			do {
				if ((LA(1)==12)) {
					match(12);
					v=var();
					if ( inputState.guessing==0 ) {
						p.add(env.scope(v));
					}
				}
				else {
					break _loop12;
				}
				
			} while (true);
			}
			break;
		}
		case 13:
		{
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		}
		match(13);
		return p;
	}
	
	public final RecordExpr  recordExpr() throws RecognitionException, TokenStreamException {
		RecordExpr r = null;
		
		ArrayList<FieldExpr> args = new ArrayList<FieldExpr>();
			  FieldExpr f;
		
		match(14);
		{
		switch ( LA(1)) {
		case 11:
		case STR:
		case INT:
		case DEC:
		case DOUBLE:
		case HEXSTR:
		case DATETIME:
		case VAR:
		case ID:
		{
			f=fieldExpr();
			if ( inputState.guessing==0 ) {
				args.add(f);
			}
			{
			_loop17:
			do {
				if ((LA(1)==12)) {
					match(12);
					{
					switch ( LA(1)) {
					case 11:
					case STR:
					case INT:
					case DEC:
					case DOUBLE:
					case HEXSTR:
					case DATETIME:
					case VAR:
					case ID:
					{
						f=fieldExpr();
						if ( inputState.guessing==0 ) {
							args.add(f);
						}
						break;
					}
					case 12:
					case 15:
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
					break _loop17;
				}
				
			} while (true);
			}
			break;
		}
		case 15:
		{
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		}
		match(15);
		if ( inputState.guessing==0 ) {
			r = new RecordExpr(args.toArray(new Expr[args.size()]));
		}
		return r;
	}
	
	public final FieldExpr  fieldExpr() throws RecognitionException, TokenStreamException {
		FieldExpr f;
		
		Expr n = null; String i; VarExpr v;
		
		switch ( LA(1)) {
		case ID:
		{
			i=id();
			f=fieldValue(new ConstExpr(new JString(i)));
			break;
		}
		case 11:
		case STR:
		case INT:
		case DEC:
		case DOUBLE:
		case HEXSTR:
		case DATETIME:
		{
			n=literalExpr();
			{
			switch ( LA(1)) {
			case 16:
			case 17:
			{
				f=fieldValue(n);
				break;
			}
			case 19:
			case DOT_ID:
			{
				f=projPattern(n);
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
		case VAR:
		{
			v=varExpr();
			{
			boolean synPredMatched22 = false;
			if (((LA(1)==16||LA(1)==17))) {
				int _m22 = mark();
				synPredMatched22 = true;
				inputState.guessing++;
				try {
					{
					match(16);
					match(17);
					}
				}
				catch (RecognitionException pe) {
					synPredMatched22 = false;
				}
				rewind(_m22);
inputState.guessing--;
			}
			if ( synPredMatched22 ) {
				f=fieldValue(v);
			}
			else if ((LA(1)==19||LA(1)==DOT_ID)) {
				f=projPattern(v);
			}
			else if ((LA(1)==12||LA(1)==15||LA(1)==16)) {
				f=varField(v);
			}
			else {
				throw new NoViableAltException(LT(1), getFilename());
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
	
	public final FieldExpr  fieldValue(
		Expr name
	) throws RecognitionException, TokenStreamException {
		FieldExpr f = null;
		
		Expr v; boolean required = true;
		
		{
		switch ( LA(1)) {
		case 16:
		{
			match(16);
			if ( inputState.guessing==0 ) {
				required = false;
			}
			break;
		}
		case 17:
		{
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		}
		match(17);
		v=expr();
		if ( inputState.guessing==0 ) {
			f = new NameValueBinding(name, v, required);
		}
		return f;
	}
	
	public final Expr  literalExpr() throws RecognitionException, TokenStreamException {
		Expr r = null;
		
		Token  s = null;
		Token  i = null;
		Token  n = null;
		Token  d = null;
		Token  h = null;
		Token  t = null;
		
		switch ( LA(1)) {
		case STR:
		{
			s = LT(1);
			match(STR);
			if ( inputState.guessing==0 ) {
				r = new ConstExpr(new JString(s.getText()));
			}
			break;
		}
		case INT:
		{
			i = LT(1);
			match(INT);
			if ( inputState.guessing==0 ) {
				r = new ConstExpr(new JLong(i.getText()));
			}
			break;
		}
		case DEC:
		{
			n = LT(1);
			match(DEC);
			if ( inputState.guessing==0 ) {
				r = new ConstExpr(new JDecimal(n.getText()));
			}
			break;
		}
		case DOUBLE:
		{
			d = LT(1);
			match(DOUBLE);
			if ( inputState.guessing==0 ) {
				r = new ConstExpr(new JDouble(d.getText()));
			}
			break;
		}
		case HEXSTR:
		{
			h = LT(1);
			match(HEXSTR);
			if ( inputState.guessing==0 ) {
				r = new ConstExpr(new JBinary(h.getText()));
			}
			break;
		}
		case DATETIME:
		{
			t = LT(1);
			match(DATETIME);
			if ( inputState.guessing==0 ) {
				r = new ConstExpr(new JDate(t.getText()));
			}
			break;
		}
		case 11:
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
	
	public final FieldExpr  projPattern(
		Expr ctx
	) throws RecognitionException, TokenStreamException {
		FieldExpr f = null;
		
		Expr n = null; boolean wild = false;
		
		{
		switch ( LA(1)) {
		case DOT_ID:
		{
			n=dotId();
			{
			switch ( LA(1)) {
			case 18:
			{
				match(18);
				if ( inputState.guessing==0 ) {
					wild = true;
				}
				break;
			}
			case 12:
			case 15:
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
		case 19:
		{
			match(19);
			{
			switch ( LA(1)) {
			case 11:
			case STR:
			case INT:
			case DEC:
			case DOUBLE:
			case HEXSTR:
			case DATETIME:
			case VAR:
			{
				n=basic();
				{
				switch ( LA(1)) {
				case 18:
				{
					match(18);
					if ( inputState.guessing==0 ) {
						wild = true;
					}
					break;
				}
				case 12:
				case 15:
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
			case 18:
			{
				match(18);
				if ( inputState.guessing==0 ) {
					wild = true;
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
		if ( inputState.guessing==0 ) {
			f = new ProjPattern(ctx,n,wild);
		}
		return f;
	}
	
	public final VarExpr  varExpr() throws RecognitionException, TokenStreamException {
		VarExpr r = null;
		
		String v;
		
		v=var();
		if ( inputState.guessing==0 ) {
			
			Var var = env.inscope(v);
			r = new VarExpr(var);
			
		}
		return r;
	}
	
	public final FieldExpr  varField(
		VarExpr ve
	) throws RecognitionException, TokenStreamException {
		FieldExpr f = null;
		
		boolean required = true;
		
		{
		switch ( LA(1)) {
		case 16:
		{
			match(16);
			if ( inputState.guessing==0 ) {
				required = false;
			}
			break;
		}
		case 12:
		case 15:
		{
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		}
		if ( inputState.guessing==0 ) {
			
					String name = ve.var().name().substring(1);
			f = new NameValueBinding(name,ve,required);
			
		}
		return f;
	}
	
	public final Expr  dotId() throws RecognitionException, TokenStreamException {
		Expr r = null;
		
		Token  i = null;
		
		i = LT(1);
		match(DOT_ID);
		if ( inputState.guessing==0 ) {
			r = new ConstExpr(new JString(i.getText()));
		}
		return r;
	}
	
	public final Expr  basic() throws RecognitionException, TokenStreamException {
		Expr r = null;
		
		
		switch ( LA(1)) {
		case VAR:
		{
			r=varExpr();
			break;
		}
		case 11:
		case STR:
		case INT:
		case DEC:
		case DOUBLE:
		case HEXSTR:
		case DATETIME:
		{
			r=literalExpr();
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		return r;
	}
	
	public final Expr  dotName() throws RecognitionException, TokenStreamException {
		Expr r = null;
		
		
		switch ( LA(1)) {
		case DOT_ID:
		{
			r=dotId();
			break;
		}
		case 19:
		{
			match(19);
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
	
	public final Expr  fieldName() throws RecognitionException, TokenStreamException {
		Expr r = null;
		
		String i;
		
		switch ( LA(1)) {
		case 11:
		case STR:
		case INT:
		case DEC:
		case DOUBLE:
		case HEXSTR:
		case DATETIME:
		case VAR:
		{
			r=basic();
			break;
		}
		case ID:
		{
			i=id();
			if ( inputState.guessing==0 ) {
				r = new ConstExpr(new JString(i));
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
	
	public final String  constFieldName() throws RecognitionException, TokenStreamException {
		String name = null;
		
		Token  s = null;
		
		switch ( LA(1)) {
		case ID:
		{
			name=id();
			break;
		}
		case STR:
		{
			s = LT(1);
			match(STR);
			if ( inputState.guessing==0 ) {
				name=s.getText();
			}
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		return name;
	}
	
	public final Expr  andExpr() throws RecognitionException, TokenStreamException {
		Expr r = null;
		
		Expr s;
		
		r=notExpr();
		{
		_loop41:
		do {
			if ((LA(1)==LITERAL_and)) {
				match(LITERAL_and);
				s=notExpr();
				if ( inputState.guessing==0 ) {
					r = new AndExpr(r,s);
				}
			}
			else {
				break _loop41;
			}
			
		} while (true);
		}
		return r;
	}
	
	public final Expr  notExpr() throws RecognitionException, TokenStreamException {
		Expr r = null;
		
		
		switch ( LA(1)) {
		case LITERAL_not:
		{
			match(LITERAL_not);
			r=notExpr();
			if ( inputState.guessing==0 ) {
				r = new NotExpr(r);
			}
			break;
		}
		case 11:
		case 14:
		case STR:
		case 34:
		case 35:
		case 37:
		case LITERAL_true:
		case LITERAL_false:
		case LITERAL_null:
		case INT:
		case DEC:
		case DOUBLE:
		case HEXSTR:
		case DATETIME:
		case VAR:
		case ID:
		case LITERAL_type:
		{
			r=inExpr();
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
			if ( inputState.guessing==0 ) {
				r = new InExpr(r,s);
			}
			break;
		}
		case EOF:
		case SEMI:
		case 12:
		case 13:
		case 15:
		case LITERAL_or:
		case LITERAL_and:
		case 38:
		case LITERAL_else:
		case LITERAL_on:
		case LITERAL_by:
		case LITERAL_into:
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
		return r;
	}
	
	public final Expr  compare() throws RecognitionException, TokenStreamException {
		Expr r = null;
		
		int c; Expr r2;
		
		r=instanceOfExpr();
		{
		switch ( LA(1)) {
		case 26:
		case 27:
		case 28:
		case 29:
		case 30:
		case 31:
		{
			c=compareOp();
			r2=instanceOfExpr();
			if ( inputState.guessing==0 ) {
				r = new CompareExpr(c,r,r2);
			}
			break;
		}
		case EOF:
		case SEMI:
		case 12:
		case 13:
		case 15:
		case LITERAL_or:
		case LITERAL_and:
		case LITERAL_in:
		case 38:
		case LITERAL_else:
		case LITERAL_on:
		case LITERAL_by:
		case LITERAL_into:
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
		return r;
	}
	
	public final Expr  instanceOfExpr() throws RecognitionException, TokenStreamException {
		Expr r = null;
		
		Expr s;
		
		r=toExpr();
		{
		switch ( LA(1)) {
		case LITERAL_instanceof:
		{
			match(LITERAL_instanceof);
			s=toExpr();
			if ( inputState.guessing==0 ) {
				r = new InstanceOfExpr(r,s);
			}
			break;
		}
		case EOF:
		case SEMI:
		case 12:
		case 13:
		case 15:
		case LITERAL_or:
		case LITERAL_and:
		case LITERAL_in:
		case 26:
		case 27:
		case 28:
		case 29:
		case 30:
		case 31:
		case 38:
		case LITERAL_else:
		case LITERAL_on:
		case LITERAL_by:
		case LITERAL_into:
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
		return r;
	}
	
	public final int  compareOp() throws RecognitionException, TokenStreamException {
		int r = -1;
		
		
		switch ( LA(1)) {
		case 26:
		{
			match(26);
			if ( inputState.guessing==0 ) {
				r = CompareExpr.EQ;
			}
			break;
		}
		case 27:
		{
			match(27);
			if ( inputState.guessing==0 ) {
				r = CompareExpr.LT;
			}
			break;
		}
		case 28:
		{
			match(28);
			if ( inputState.guessing==0 ) {
				r = CompareExpr.GT;
			}
			break;
		}
		case 29:
		{
			match(29);
			if ( inputState.guessing==0 ) {
				r = CompareExpr.NE;
			}
			break;
		}
		case 30:
		{
			match(30);
			if ( inputState.guessing==0 ) {
				r = CompareExpr.LE;
			}
			break;
		}
		case 31:
		{
			match(31);
			if ( inputState.guessing==0 ) {
				r = CompareExpr.GE;
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
	
	public final Expr  toExpr() throws RecognitionException, TokenStreamException {
		Expr r = null;
		
		Expr s;
		
		r=addExpr();
		{
		switch ( LA(1)) {
		case LITERAL_to:
		{
			match(LITERAL_to);
			s=addExpr();
			if ( inputState.guessing==0 ) {
				r = new RangeExpr(r,s);
			}
			break;
		}
		case EOF:
		case SEMI:
		case 12:
		case 13:
		case 15:
		case LITERAL_or:
		case LITERAL_and:
		case LITERAL_in:
		case 26:
		case 27:
		case 28:
		case 29:
		case 30:
		case 31:
		case LITERAL_instanceof:
		case 38:
		case LITERAL_else:
		case LITERAL_on:
		case LITERAL_by:
		case LITERAL_into:
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
		return r;
	}
	
	public final Expr  addExpr() throws RecognitionException, TokenStreamException {
		Expr r = null;
		
		Expr s; int op;
		
		r=multExpr();
		{
		_loop54:
		do {
			if ((LA(1)==34||LA(1)==35)) {
				op=addOp();
				s=multExpr();
				if ( inputState.guessing==0 ) {
					r = new MathExpr(op,r,s);
				}
			}
			else {
				break _loop54;
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
		_loop58:
		do {
			if ((LA(1)==18||LA(1)==36)) {
				op=multOp();
				s=unaryAdd();
				if ( inputState.guessing==0 ) {
					r = new MathExpr(op,r,s);
				}
			}
			else {
				break _loop58;
			}
			
		} while (true);
		}
		return r;
	}
	
	public final int  addOp() throws RecognitionException, TokenStreamException {
		int op=0;
		
		
		switch ( LA(1)) {
		case 34:
		{
			match(34);
			if ( inputState.guessing==0 ) {
				op=MathExpr.PLUS;
			}
			break;
		}
		case 35:
		{
			match(35);
			if ( inputState.guessing==0 ) {
				op=MathExpr.MINUS;
			}
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
		case 35:
		{
			match(35);
			r=access();
			if ( inputState.guessing==0 ) {
				r = MathExpr.negate(r);
			}
			break;
		}
		case 11:
		case 14:
		case STR:
		case 34:
		case 37:
		case LITERAL_true:
		case LITERAL_false:
		case LITERAL_null:
		case INT:
		case DEC:
		case DOUBLE:
		case HEXSTR:
		case DATETIME:
		case VAR:
		case ID:
		case LITERAL_type:
		{
			{
			switch ( LA(1)) {
			case 34:
			{
				match(34);
				break;
			}
			case 11:
			case 14:
			case STR:
			case 37:
			case LITERAL_true:
			case LITERAL_false:
			case LITERAL_null:
			case INT:
			case DEC:
			case DOUBLE:
			case HEXSTR:
			case DATETIME:
			case VAR:
			case ID:
			case LITERAL_type:
			{
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
			}
			r=access();
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
		case 18:
		{
			match(18);
			if ( inputState.guessing==0 ) {
				op=MathExpr.MULTIPLY;
			}
			break;
		}
		case 36:
		{
			match(36);
			if ( inputState.guessing==0 ) {
				op=MathExpr.DIVIDE;
			}
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		return op;
	}
	
	public final Expr  access() throws RecognitionException, TokenStreamException {
		Expr r = null;
		
		String i; ArrayList<Expr> args;
		
		{
		switch ( LA(1)) {
		case 11:
		case 14:
		case STR:
		case 37:
		case LITERAL_true:
		case LITERAL_false:
		case LITERAL_null:
		case INT:
		case DEC:
		case DOUBLE:
		case HEXSTR:
		case DATETIME:
		case VAR:
		case LITERAL_type:
		{
			r=construct();
			break;
		}
		case ID:
		{
			i=id();
			match(11);
			args=exprList();
			match(13);
			if ( inputState.guessing==0 ) {
				r = FunctionLib.lookup(env, i, args);
			}
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		}
		r=step(r);
		return r;
	}
	
	public final Expr  construct() throws RecognitionException, TokenStreamException {
		Expr r = null;
		
		
		switch ( LA(1)) {
		case LITERAL_true:
		{
			match(LITERAL_true);
			if ( inputState.guessing==0 ) {
				r = new ConstExpr(JBool.trueItem);
			}
			break;
		}
		case LITERAL_false:
		{
			match(LITERAL_false);
			if ( inputState.guessing==0 ) {
				r = new ConstExpr(JBool.falseItem);
			}
			break;
		}
		case LITERAL_null:
		{
			match(LITERAL_null);
			if ( inputState.guessing==0 ) {
				r = new ConstExpr(Item.nil);
			}
			break;
		}
		case LITERAL_type:
		{
			r=typeExpr();
			break;
		}
		case 37:
		{
			r=arrayExpr();
			break;
		}
		case 14:
		{
			r=recordExpr();
			break;
		}
		case 11:
		case STR:
		case INT:
		case DEC:
		case DOUBLE:
		case HEXSTR:
		case DATETIME:
		case VAR:
		{
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
	
	public final ArrayList<Expr>  exprList() throws RecognitionException, TokenStreamException {
		ArrayList<Expr> es = new ArrayList<Expr>();
		
		Expr e;
		
		{
		_loop79:
		do {
			if ((LA(1)==12)) {
				match(12);
			}
			else {
				break _loop79;
			}
			
		} while (true);
		}
		{
		switch ( LA(1)) {
		case LITERAL_fn:
		case 11:
		case 14:
		case STR:
		case LITERAL_not:
		case 34:
		case 35:
		case 37:
		case LITERAL_true:
		case LITERAL_false:
		case LITERAL_null:
		case INT:
		case DEC:
		case DOUBLE:
		case HEXSTR:
		case DATETIME:
		case LITERAL_for:
		case LITERAL_let:
		case LITERAL_if:
		case LITERAL_join:
		case LITERAL_group:
		case LITERAL_combine:
		case LITERAL_reduce:
		case LITERAL_sort:
		case VAR:
		case ID:
		case LITERAL_type:
		{
			e=expr();
			if ( inputState.guessing==0 ) {
				es.add(e);
			}
			{
			_loop84:
			do {
				if ((LA(1)==12)) {
					{
					match(12);
					}
					{
					switch ( LA(1)) {
					case LITERAL_fn:
					case 11:
					case 14:
					case STR:
					case LITERAL_not:
					case 34:
					case 35:
					case 37:
					case LITERAL_true:
					case LITERAL_false:
					case LITERAL_null:
					case INT:
					case DEC:
					case DOUBLE:
					case HEXSTR:
					case DATETIME:
					case LITERAL_for:
					case LITERAL_let:
					case LITERAL_if:
					case LITERAL_join:
					case LITERAL_group:
					case LITERAL_combine:
					case LITERAL_reduce:
					case LITERAL_sort:
					case VAR:
					case ID:
					case LITERAL_type:
					{
						e=expr();
						if ( inputState.guessing==0 ) {
							es.add(e);
						}
						break;
					}
					case 12:
					case 13:
					case 38:
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
					break _loop84;
				}
				
			} while (true);
			}
			break;
		}
		case 13:
		case 38:
		{
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		}
		return es;
	}
	
	public final Expr  step(
		Expr ctx
	) throws RecognitionException, TokenStreamException {
		Expr r = null;
		
		Var var = null; ArrayList<Expr> args; boolean addArray = true;
		
		switch ( LA(1)) {
		case EOF:
		case SEMI:
		case 12:
		case 13:
		case 15:
		case 18:
		case LITERAL_or:
		case LITERAL_and:
		case LITERAL_in:
		case 26:
		case 27:
		case 28:
		case 29:
		case 30:
		case 31:
		case LITERAL_instanceof:
		case LITERAL_to:
		case 34:
		case 35:
		case 36:
		case 38:
		case LITERAL_else:
		case LITERAL_on:
		case LITERAL_by:
		case LITERAL_into:
		case LITERAL_asc:
		case LITERAL_desc:
		{
			if ( inputState.guessing==0 ) {
				r = ctx;
			}
			break;
		}
		case 11:
		case 19:
		case DOT_ID:
		case 37:
		{
			{
			{
			switch ( LA(1)) {
			case 19:
			case DOT_ID:
			{
				r=dotName();
				if ( inputState.guessing==0 ) {
					r = new FieldValueExpr(ctx, r);
				}
				break;
			}
			case 11:
			{
				match(11);
				args=exprList();
				match(13);
				if ( inputState.guessing==0 ) {
					r = new FunctionCallExpr(ctx, args);
				}
				break;
			}
			case 37:
			{
				match(37);
				{
				switch ( LA(1)) {
				case LITERAL_fn:
				case 11:
				case 14:
				case STR:
				case LITERAL_not:
				case 34:
				case 35:
				case 37:
				case LITERAL_true:
				case LITERAL_false:
				case LITERAL_null:
				case INT:
				case DEC:
				case DOUBLE:
				case HEXSTR:
				case DATETIME:
				case LITERAL_for:
				case LITERAL_let:
				case LITERAL_if:
				case LITERAL_join:
				case LITERAL_group:
				case LITERAL_combine:
				case LITERAL_reduce:
				case LITERAL_sort:
				case VAR:
				case ID:
				case LITERAL_type:
				{
					r=expr();
					match(38);
					if ( inputState.guessing==0 ) {
						r = new IndexExpr(ctx, r);
					}
					break;
				}
				case 18:
				{
					match(18);
					{
					switch ( LA(1)) {
					case 18:
					{
						match(18);
						if ( inputState.guessing==0 ) {
							addArray = false;
						}
						break;
					}
					case 38:
					{
						break;
					}
					default:
					{
						throw new NoViableAltException(LT(1), getFilename());
					}
					}
					}
					match(38);
					if ( inputState.guessing==0 ) {
						var = env.makeVar("$star"); r = new VarExpr(var);
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
			r=step(r);
			}
			if ( inputState.guessing==0 ) {
				
					if( var != null ) 
					{
					  if( addArray )
					  {
					  	r = new ArrayExpr(r); 
					  }
					  r = new ForExpr(var, ctx, r); 
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
	
	public final Expr  typeExpr() throws RecognitionException, TokenStreamException {
		Expr r = null;
		
		Schema s;
		
		match(LITERAL_type);
		s=type();
		if ( inputState.guessing==0 ) {
			r = new ConstExpr(new JSchema(s));
		}
		return r;
	}
	
	public final Expr  arrayExpr() throws RecognitionException, TokenStreamException {
		Expr r = null;
		
		ArrayList<Expr> es;
		
		match(37);
		es=exprList();
		match(38);
		if ( inputState.guessing==0 ) {
			r = new ArrayExpr(es);
		}
		return r;
	}
	
	public final Expr  parenExpr() throws RecognitionException, TokenStreamException {
		Expr r = null;
		
		ArrayList<BindingExpr> b = new ArrayList<BindingExpr>();
		
		match(11);
		{
		_loop74:
		do {
			if ((LA(1)==AVAR)) {
				letDef(b);
				match(12);
			}
			else {
				break _loop74;
			}
			
		} while (true);
		}
		r=expr();
		match(13);
		if ( inputState.guessing==0 ) {
			
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
		return r;
	}
	
	public final void letDef(
		ArrayList<BindingExpr> bindings
	) throws RecognitionException, TokenStreamException {
		
		String v; Expr e;
		
		v=avar();
		match(9);
		e=expr();
		if ( inputState.guessing==0 ) {
			
				  Var var = env.scope(v);
				  bindings.add( new BindingExpr(BindingExpr.Type.EQ, var, null, e) );
				
		}
	}
	
	public final void forDef(
		ArrayList<BindingExpr> bindings
	) throws RecognitionException, TokenStreamException {
		
		String v; Expr e; String v2 = null; BindingExpr.Type t = null;
		
		v=var();
		{
		switch ( LA(1)) {
		case LITERAL_in:
		case LITERAL_at:
		{
			{
			switch ( LA(1)) {
			case LITERAL_at:
			{
				match(LITERAL_at);
				v2=var();
				break;
			}
			case LITERAL_in:
			{
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
			}
			match(LITERAL_in);
			e=expr();
			if ( inputState.guessing==0 ) {
				t = BindingExpr.Type.IN;
			}
			break;
		}
		case 17:
		{
			match(17);
			v2=var();
			match(LITERAL_in);
			e=expr();
			if ( inputState.guessing==0 ) {
				t = BindingExpr.Type.INREC;
			}
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		}
		if ( inputState.guessing==0 ) {
			
				  Var var = env.scope(v);
				  Var var2 = null;
				  if( v2 != null )
				  {
				  	var2 = env.scope(v2); 
				  }
				  bindings.add( new BindingExpr(t, var, var2, e) );
				
		}
	}
	
	public final void joinDef(
		ArrayList<BindingExpr> bindings
	) throws RecognitionException, TokenStreamException {
		
		String v1; Expr e1; Expr e2; Var var1 = null; 
		boolean opt = false;
		
		{
		switch ( LA(1)) {
		case LITERAL_optional:
		{
			match(LITERAL_optional);
			if ( inputState.guessing==0 ) {
				opt = true;
			}
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
		v1=var();
		match(LITERAL_in);
		e1=expr();
		if ( inputState.guessing==0 ) {
			var1 = env.scope(v1);
		}
		match(LITERAL_on);
		e2=expr();
		if ( inputState.guessing==0 ) {
			
				  var1.hidden = true;
				  BindingExpr b = new BindingExpr(BindingExpr.Type.IN, var1, null, e1, e2);
				  b.optional = opt;
				  bindings.add( b );
				
		}
	}
	
	public final void groupDef(
		ArrayList<BindingExpr> bindings
	) throws RecognitionException, TokenStreamException {
		
		String v1, v2, v3; Expr e1, e2; Var inVar = null;
		
		v1=var();
		match(LITERAL_in);
		e1=expr();
		if ( inputState.guessing==0 ) {
			inVar = env.scope(v1);
		}
		match(LITERAL_by);
		v2=avar();
		match(9);
		e2=expr();
		match(LITERAL_into);
		v3=var();
		if ( inputState.guessing==0 ) {
			
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
	}
	
	public final BindingExpr  agg() throws RecognitionException, TokenStreamException {
		BindingExpr b=null;
		
		String v; Var var=null; String i; Expr e;
		
		v=avar();
		match(9);
		e=expr();
		if ( inputState.guessing==0 ) {
			
					var = env.scope(v);
					var.hidden = true;
					b = new BindingExpr(BindingExpr.Type.AGGFN, var, null, e);
				
		}
		return b;
	}
	
	public final void sortSpec(
		ArrayList<OrderExpr> by
	) throws RecognitionException, TokenStreamException {
		
		Expr e; OrderExpr.Order order = OrderExpr.Order.ASC;
		
		e=expr();
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
			if ( inputState.guessing==0 ) {
				order = OrderExpr.Order.DESC;
			}
			break;
		}
		case 12:
		case 13:
		{
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		}
		if ( inputState.guessing==0 ) {
			by.add( new OrderExpr(e, order) );
		}
	}
	
	public final Schema  type() throws RecognitionException, TokenStreamException {
		Schema s = null;
		
		Schema s2 = null; SchemaOr os = null;
		
		s=typeTerm();
		{
		_loop124:
		do {
			if ((LA(1)==67)) {
				match(67);
				if ( inputState.guessing==0 ) {
					s2 = s; s = os = new SchemaOr(); os.addSchema(s2);
				}
				s2=typeTerm();
				if ( inputState.guessing==0 ) {
					os.addSchema(s2);
				}
			}
			else {
				break _loop124;
			}
			
		} while (true);
		}
		return s;
	}
	
	public final Schema  typeTerm() throws RecognitionException, TokenStreamException {
		Schema s = null;
		
		
		switch ( LA(1)) {
		case 18:
		{
			match(18);
			if ( inputState.guessing==0 ) {
				s = new SchemaAny();
			}
			break;
		}
		case 14:
		case 37:
		case LITERAL_null:
		case ID:
		case LITERAL_type:
		{
			s=oneType();
			{
			switch ( LA(1)) {
			case 16:
			{
				match(16);
				if ( inputState.guessing==0 ) {
					s = new SchemaOr(s, new SchemaAtom("null"));
				}
				break;
			}
			case EOF:
			case SEMI:
			case 11:
			case 12:
			case 13:
			case 15:
			case 18:
			case 19:
			case DOT_ID:
			case LITERAL_or:
			case LITERAL_and:
			case LITERAL_in:
			case 26:
			case 27:
			case 28:
			case 29:
			case 30:
			case 31:
			case LITERAL_instanceof:
			case LITERAL_to:
			case 34:
			case 35:
			case 36:
			case 37:
			case 38:
			case LITERAL_else:
			case LITERAL_on:
			case LITERAL_by:
			case LITERAL_into:
			case LITERAL_asc:
			case LITERAL_desc:
			case 67:
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
		case LITERAL_null:
		case ID:
		case LITERAL_type:
		{
			s=atomType();
			break;
		}
		case 37:
		{
			s=arrayType();
			break;
		}
		case 14:
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
			if ( inputState.guessing==0 ) {
				s = new SchemaAtom(i.getText());
			}
			break;
		}
		case LITERAL_null:
		{
			match(LITERAL_null);
			if ( inputState.guessing==0 ) {
				s = new SchemaAtom("null");
			}
			break;
		}
		case LITERAL_type:
		{
			match(LITERAL_type);
			if ( inputState.guessing==0 ) {
				s = new SchemaAtom("type");
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
	
	public final SchemaArray  arrayType() throws RecognitionException, TokenStreamException {
		SchemaArray s = new SchemaArray();
		
		Schema head = null; Schema p; Schema q;
		
		match(37);
		{
		switch ( LA(1)) {
		case 14:
		case 18:
		case 37:
		case LITERAL_null:
		case ID:
		case LITERAL_type:
		{
			p=type();
			if ( inputState.guessing==0 ) {
				head = p;
			}
			{
			_loop132:
			do {
				if ((LA(1)==12)) {
					match(12);
					q=type();
					if ( inputState.guessing==0 ) {
						p = p.nextSchema = q;
					}
				}
				else {
					break _loop132;
				}
				
			} while (true);
			}
			arrayRepeat(head,s);
			break;
		}
		case 38:
		{
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		}
		match(38);
		return s;
	}
	
	public final SchemaRecord  recordType() throws RecognitionException, TokenStreamException {
		SchemaRecord s = new SchemaRecord();
		
		SchemaField f;
		
		match(14);
		{
		switch ( LA(1)) {
		case 18:
		case STR:
		case ID:
		{
			f=fieldType();
			if ( inputState.guessing==0 ) {
				s.addField(f);
			}
			{
			_loop140:
			do {
				if ((LA(1)==12)) {
					match(12);
					f=fieldType();
					if ( inputState.guessing==0 ) {
						s.addField(f);
					}
				}
				else {
					break _loop140;
				}
				
			} while (true);
			}
			break;
		}
		case 15:
		{
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		}
		match(15);
		return s;
	}
	
	public final void arrayRepeat(
		Schema typeList, SchemaArray s
	) throws RecognitionException, TokenStreamException {
		
		Token  i1 = null;
		Token  i2 = null;
		long lo = 0; long hi = 0;
		
		switch ( LA(1)) {
		case 38:
		{
			if ( inputState.guessing==0 ) {
				s.noRepeat(typeList);
			}
			break;
		}
		case 27:
		{
			match(27);
			{
			switch ( LA(1)) {
			case 18:
			{
				match(18);
				if ( inputState.guessing==0 ) {
					lo = 0; hi = SchemaArray.UNLIMITED;
				}
				break;
			}
			case INT:
			{
				i1 = LT(1);
				match(INT);
				if ( inputState.guessing==0 ) {
					lo = Long.parseLong(i1.getText());
				}
				{
				switch ( LA(1)) {
				case 28:
				{
					if ( inputState.guessing==0 ) {
						hi = lo;
					}
					break;
				}
				case 12:
				{
					match(12);
					{
					switch ( LA(1)) {
					case 18:
					{
						match(18);
						if ( inputState.guessing==0 ) {
							hi = SchemaArray.UNLIMITED;
						}
						break;
					}
					case INT:
					{
						i2 = LT(1);
						match(INT);
						if ( inputState.guessing==0 ) {
							hi = Long.parseLong(i2.getText());
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
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
			}
			match(28);
			if ( inputState.guessing==0 ) {
				s.setRepeat(typeList, lo, hi);
			}
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
		case ID:
		{
			n=constFieldName();
			if ( inputState.guessing==0 ) {
				f.name = new JString(n);
			}
			{
			switch ( LA(1)) {
			case 18:
			{
				match(18);
				if ( inputState.guessing==0 ) {
					f.wildcard = true;
				}
				break;
			}
			case 16:
			{
				match(16);
				if ( inputState.guessing==0 ) {
					f.optional = true;
				}
				break;
			}
			case 17:
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
		case 18:
		{
			match(18);
			if ( inputState.guessing==0 ) {
				f.name = new JString(""); 
					                        f.wildcard = true;
			}
			break;
		}
		default:
		{
			throw new NoViableAltException(LT(1), getFilename());
		}
		}
		}
		match(17);
		t=type();
		if ( inputState.guessing==0 ) {
			f.schema = t;
		}
		return f;
	}
	
	
	public static final String[] _tokenNames = {
		"<0>",
		"EOF",
		"<2>",
		"NULL_TREE_LOOKAHEAD",
		"\"declare\"",
		"SEMI",
		"\"explain\"",
		"\"materialize\"",
		"\"quit\"",
		"\"=\"",
		"\"fn\"",
		"\"(\"",
		"\",\"",
		"\")\"",
		"\"{\"",
		"\"}\"",
		"\"?\"",
		"\":\"",
		"\"*\"",
		"\".\"",
		"DOT_ID",
		"STR",
		"\"or\"",
		"\"and\"",
		"\"not\"",
		"\"in\"",
		"\"==\"",
		"\"<\"",
		"\">\"",
		"\"!=\"",
		"\"<=\"",
		"\">=\"",
		"\"instanceof\"",
		"\"to\"",
		"\"+\"",
		"\"-\"",
		"\"/\"",
		"\"[\"",
		"\"]\"",
		"\"true\"",
		"\"false\"",
		"\"null\"",
		"INT",
		"DEC",
		"DOUBLE",
		"HEXSTR",
		"DATETIME",
		"\"for\"",
		"\"at\"",
		"\"let\"",
		"\"if\"",
		"\"else\"",
		"\"join\"",
		"\"optional\"",
		"\"on\"",
		"\"group\"",
		"\"by\"",
		"\"into\"",
		"\"combine\"",
		"\"reduce\"",
		"\"sort\"",
		"\"asc\"",
		"\"desc\"",
		"VAR",
		"AVAR",
		"ID",
		"\"type\"",
		"\"|\"",
		"DIGIT",
		"HEX",
		"LETTER",
		"WS",
		"COMMENT",
		"ML_COMMENT",
		"VAR1",
		"SYM",
		"SYM2",
		"STRCHAR",
		"DOTTY",
		"IDWORD"
	};
	
	private static final long[] mk_tokenSet_0() {
		long[] data = { 7153968282955984930L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_0 = new BitSet(mk_tokenSet_0());
	
	}
