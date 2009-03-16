// $ANTLR 2.7.6 (2005-12-22): "jaql.g" -> "JaqlLexer.java"$

package com.ibm.jaql.lang.parser;

import java.util.*;

import com.ibm.jaql.lang.core.*;
import com.ibm.jaql.lang.expr.core.*;
import com.ibm.jaql.lang.expr.top.*;

import com.ibm.jaql.json.type.*;
import com.ibm.jaql.json.schema.*;
import com.ibm.jaql.json.util.*;

import com.ibm.jaql.util.*;


import java.io.InputStream;
import antlr.TokenStreamException;
import antlr.TokenStreamIOException;
import antlr.TokenStreamRecognitionException;
import antlr.CharStreamException;
import antlr.CharStreamIOException;
import antlr.ANTLRException;
import java.io.Reader;
import java.util.Hashtable;
import antlr.CharScanner;
import antlr.InputBuffer;
import antlr.ByteBuffer;
import antlr.CharBuffer;
import antlr.Token;
import antlr.CommonToken;
import antlr.RecognitionException;
import antlr.NoViableAltForCharException;
import antlr.MismatchedCharException;
import antlr.TokenStream;
import antlr.ANTLRHashString;
import antlr.LexerSharedInputState;
import antlr.collections.impl.BitSet;
import antlr.SemanticException;

public class JaqlLexer extends antlr.CharScanner implements JaqlTokenTypes, TokenStream
 {
public JaqlLexer(InputStream in) {
	this(new ByteBuffer(in));
}
public JaqlLexer(Reader in) {
	this(new CharBuffer(in));
}
public JaqlLexer(InputBuffer ib) {
	this(new LexerSharedInputState(ib));
}
public JaqlLexer(LexerSharedInputState state) {
	super(state);
	caseSensitiveLiterals = true;
	setCaseSensitive(true);
	literals = new Hashtable();
	literals.put(new ANTLRHashString("<", this), new Integer(27));
	literals.put(new ANTLRHashString("to", this), new Integer(33));
	literals.put(new ANTLRHashString("declare", this), new Integer(4));
	literals.put(new ANTLRHashString("for", this), new Integer(47));
	literals.put(new ANTLRHashString(")", this), new Integer(13));
	literals.put(new ANTLRHashString("/", this), new Integer(36));
	literals.put(new ANTLRHashString("optional", this), new Integer(53));
	literals.put(new ANTLRHashString("false", this), new Integer(40));
	literals.put(new ANTLRHashString("true", this), new Integer(39));
	literals.put(new ANTLRHashString("let", this), new Integer(49));
	literals.put(new ANTLRHashString("(", this), new Integer(11));
	literals.put(new ANTLRHashString("and", this), new Integer(23));
	literals.put(new ANTLRHashString("asc", this), new Integer(61));
	literals.put(new ANTLRHashString("desc", this), new Integer(62));
	literals.put(new ANTLRHashString(".", this), new Integer(19));
	literals.put(new ANTLRHashString(":", this), new Integer(17));
	literals.put(new ANTLRHashString("instanceof", this), new Integer(32));
	literals.put(new ANTLRHashString("on", this), new Integer(54));
	literals.put(new ANTLRHashString("group", this), new Integer(55));
	literals.put(new ANTLRHashString("-", this), new Integer(35));
	literals.put(new ANTLRHashString("}", this), new Integer(15));
	literals.put(new ANTLRHashString("?", this), new Integer(16));
	literals.put(new ANTLRHashString("combine", this), new Integer(58));
	literals.put(new ANTLRHashString("type", this), new Integer(66));
	literals.put(new ANTLRHashString("in", this), new Integer(25));
	literals.put(new ANTLRHashString("null", this), new Integer(41));
	literals.put(new ANTLRHashString("into", this), new Integer(57));
	literals.put(new ANTLRHashString(",", this), new Integer(12));
	literals.put(new ANTLRHashString("|", this), new Integer(67));
	literals.put(new ANTLRHashString("fn", this), new Integer(10));
	literals.put(new ANTLRHashString("]", this), new Integer(38));
	literals.put(new ANTLRHashString(">", this), new Integer(28));
	literals.put(new ANTLRHashString("materialize", this), new Integer(7));
	literals.put(new ANTLRHashString("or", this), new Integer(22));
	literals.put(new ANTLRHashString("reduce", this), new Integer(59));
	literals.put(new ANTLRHashString("!=", this), new Integer(29));
	literals.put(new ANTLRHashString("+", this), new Integer(34));
	literals.put(new ANTLRHashString("{", this), new Integer(14));
	literals.put(new ANTLRHashString("quit", this), new Integer(8));
	literals.put(new ANTLRHashString("at", this), new Integer(48));
	literals.put(new ANTLRHashString("=", this), new Integer(9));
	literals.put(new ANTLRHashString("if", this), new Integer(50));
	literals.put(new ANTLRHashString(">=", this), new Integer(31));
	literals.put(new ANTLRHashString("==", this), new Integer(26));
	literals.put(new ANTLRHashString("<=", this), new Integer(30));
	literals.put(new ANTLRHashString("*", this), new Integer(18));
	literals.put(new ANTLRHashString("join", this), new Integer(52));
	literals.put(new ANTLRHashString("else", this), new Integer(51));
	literals.put(new ANTLRHashString("not", this), new Integer(24));
	literals.put(new ANTLRHashString("explain", this), new Integer(6));
	literals.put(new ANTLRHashString("sort", this), new Integer(60));
	literals.put(new ANTLRHashString("[", this), new Integer(37));
	literals.put(new ANTLRHashString("by", this), new Integer(56));
}

public Token nextToken() throws TokenStreamException {
	Token theRetToken=null;
tryAgain:
	for (;;) {
		Token _token = null;
		int _ttype = Token.INVALID_TYPE;
		resetText();
		try {   // for char stream error handling
			try {   // for lexical error handling
				switch ( LA(1)) {
				case '\t':  case '\n':  case '\u000c':  case '\r':
				case ' ':
				{
					mWS(true);
					theRetToken=_returnToken;
					break;
				}
				case '$':
				{
					mVAR(true);
					theRetToken=_returnToken;
					break;
				}
				case '{':
				{
					mSYM2(true);
					theRetToken=_returnToken;
					break;
				}
				case ';':
				{
					mSEMI(true);
					theRetToken=_returnToken;
					break;
				}
				case '"':  case '\'':
				{
					mSTR(true);
					theRetToken=_returnToken;
					break;
				}
				case '.':  case '0':  case '1':  case '2':
				case '3':  case '4':  case '5':  case '6':
				case '7':  case '8':  case '9':
				{
					mDOTTY(true);
					theRetToken=_returnToken;
					break;
				}
				default:
					if ((LA(1)=='/') && (LA(2)=='/')) {
						mCOMMENT(true);
						theRetToken=_returnToken;
					}
					else if ((LA(1)=='/') && (LA(2)=='*')) {
						mML_COMMENT(true);
						theRetToken=_returnToken;
					}
					else if ((LA(1)=='X'||LA(1)=='x') && (LA(2)=='"'||LA(2)=='\'')) {
						mHEXSTR(true);
						theRetToken=_returnToken;
					}
					else if ((LA(1)=='D'||LA(1)=='d') && (LA(2)=='"'||LA(2)=='\'')) {
						mDATETIME(true);
						theRetToken=_returnToken;
					}
					else if ((_tokenSet_0.member(LA(1))) && (true)) {
						mSYM(true);
						theRetToken=_returnToken;
					}
					else if ((_tokenSet_1.member(LA(1))) && (true)) {
						mID(true);
						theRetToken=_returnToken;
					}
				else {
					if (LA(1)==EOF_CHAR) {uponEOF(); _returnToken = makeToken(Token.EOF_TYPE);}
				else {throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());}
				}
				}
				if ( _returnToken==null ) continue tryAgain; // found SKIP token
				_ttype = _returnToken.getType();
				_returnToken.setType(_ttype);
				return _returnToken;
			}
			catch (RecognitionException e) {
				throw new TokenStreamRecognitionException(e);
			}
		}
		catch (CharStreamException cse) {
			if ( cse instanceof CharStreamIOException ) {
				throw new TokenStreamIOException(((CharStreamIOException)cse).io);
			}
			else {
				throw new TokenStreamException(cse.getMessage());
			}
		}
	}
}

	protected final void mDIGIT(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = DIGIT;
		int _saveIndex;
		
		matchRange('0','9');
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	protected final void mHEX(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = HEX;
		int _saveIndex;
		
		switch ( LA(1)) {
		case '0':  case '1':  case '2':  case '3':
		case '4':  case '5':  case '6':  case '7':
		case '8':  case '9':
		{
			matchRange('0','9');
			break;
		}
		case 'a':  case 'b':  case 'c':  case 'd':
		case 'e':  case 'f':
		{
			matchRange('a','f');
			break;
		}
		case 'A':  case 'B':  case 'C':  case 'D':
		case 'E':  case 'F':
		{
			matchRange('A','F');
			break;
		}
		default:
		{
			throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());
		}
		}
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	protected final void mLETTER(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = LETTER;
		int _saveIndex;
		
		switch ( LA(1)) {
		case 'a':  case 'b':  case 'c':  case 'd':
		case 'e':  case 'f':  case 'g':  case 'h':
		case 'i':  case 'j':  case 'k':  case 'l':
		case 'm':  case 'n':  case 'o':  case 'p':
		case 'q':  case 'r':  case 's':  case 't':
		case 'u':  case 'v':  case 'w':  case 'x':
		case 'y':  case 'z':
		{
			matchRange('a','z');
			break;
		}
		case 'A':  case 'B':  case 'C':  case 'D':
		case 'E':  case 'F':  case 'G':  case 'H':
		case 'I':  case 'J':  case 'K':  case 'L':
		case 'M':  case 'N':  case 'O':  case 'P':
		case 'Q':  case 'R':  case 'S':  case 'T':
		case 'U':  case 'V':  case 'W':  case 'X':
		case 'Y':  case 'Z':
		{
			matchRange('A','Z');
			break;
		}
		default:
		{
			throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());
		}
		}
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	public final void mWS(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = WS;
		int _saveIndex;
		
		{
		int _cnt150=0;
		_loop150:
		do {
			switch ( LA(1)) {
			case ' ':
			{
				match(' ');
				break;
			}
			case '\t':
			{
				match('\t');
				break;
			}
			case '\u000c':
			{
				match('\f');
				break;
			}
			case '\n':  case '\r':
			{
				{
				if ((LA(1)=='\r') && (LA(2)=='\n') && (true)) {
					match("\r\n");
				}
				else if ((LA(1)=='\r') && (true) && (true)) {
					match('\r');
				}
				else if ((LA(1)=='\n')) {
					match('\n');
				}
				else {
					throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());
				}
				
				}
				if ( inputState.guessing==0 ) {
					newline();
				}
				break;
			}
			default:
			{
				if ( _cnt150>=1 ) { break _loop150; } else {throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());}
			}
			}
			_cnt150++;
		} while (true);
		}
		if ( inputState.guessing==0 ) {
			_ttype = Token.SKIP;
		}
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	public final void mCOMMENT(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = COMMENT;
		int _saveIndex;
		
		match("//");
		{
		_loop154:
		do {
			if ((_tokenSet_2.member(LA(1)))) {
				{
				match(_tokenSet_2);
				}
			}
			else {
				break _loop154;
			}
			
		} while (true);
		}
		if ( inputState.guessing==0 ) {
			_ttype = Token.SKIP;
		}
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	public final void mML_COMMENT(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = ML_COMMENT;
		int _saveIndex;
		
		match("/*");
		{
		_loop160:
		do {
			if ((LA(1)=='*') && (_tokenSet_3.member(LA(2)))) {
				match('*');
				{
				match(_tokenSet_3);
				}
			}
			else if ((_tokenSet_4.member(LA(1)))) {
				{
				match(_tokenSet_4);
				}
			}
			else if ((LA(1)=='\n'||LA(1)=='\r')) {
				{
				if ((LA(1)=='\r') && (LA(2)=='\n') && ((LA(3) >= '\u0003' && LA(3) <= '\u00ff'))) {
					match("\r\n");
				}
				else if ((LA(1)=='\r') && ((LA(2) >= '\u0003' && LA(2) <= '\u00ff')) && ((LA(3) >= '\u0003' && LA(3) <= '\u00ff'))) {
					match('\r');
				}
				else if ((LA(1)=='\n')) {
					match('\n');
				}
				else {
					throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());
				}
				
				}
				if ( inputState.guessing==0 ) {
					newline();
				}
			}
			else {
				break _loop160;
			}
			
		} while (true);
		}
		match("*/");
		if ( inputState.guessing==0 ) {
			_ttype = Token.SKIP;
		}
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	protected final void mVAR1(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = VAR1;
		int _saveIndex;
		
		match('$');
		{
		_loop163:
		do {
			switch ( LA(1)) {
			case '_':
			{
				match('_');
				break;
			}
			case 'A':  case 'B':  case 'C':  case 'D':
			case 'E':  case 'F':  case 'G':  case 'H':
			case 'I':  case 'J':  case 'K':  case 'L':
			case 'M':  case 'N':  case 'O':  case 'P':
			case 'Q':  case 'R':  case 'S':  case 'T':
			case 'U':  case 'V':  case 'W':  case 'X':
			case 'Y':  case 'Z':  case 'a':  case 'b':
			case 'c':  case 'd':  case 'e':  case 'f':
			case 'g':  case 'h':  case 'i':  case 'j':
			case 'k':  case 'l':  case 'm':  case 'n':
			case 'o':  case 'p':  case 'q':  case 'r':
			case 's':  case 't':  case 'u':  case 'v':
			case 'w':  case 'x':  case 'y':  case 'z':
			{
				mLETTER(false);
				break;
			}
			case '0':  case '1':  case '2':  case '3':
			case '4':  case '5':  case '6':  case '7':
			case '8':  case '9':
			{
				mDIGIT(false);
				break;
			}
			default:
			{
				break _loop163;
			}
			}
		} while (true);
		}
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	public final void mVAR(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = VAR;
		int _saveIndex;
		
		boolean synPredMatched168 = false;
		if (((LA(1)=='$') && (true) && (true))) {
			int _m168 = mark();
			synPredMatched168 = true;
			inputState.guessing++;
			try {
				{
				mVAR1(false);
				{
				switch ( LA(1)) {
				case '\t':  case '\n':  case '\u000c':  case '\r':
				case ' ':
				{
					_saveIndex=text.length();
					mWS(false);
					text.setLength(_saveIndex);
					break;
				}
				case '=':
				{
					break;
				}
				default:
				{
					throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());
				}
				}
				}
				match('=');
				{
				if ((_tokenSet_5.member(LA(1)))) {
					_saveIndex=text.length();
					mWS(false);
					text.setLength(_saveIndex);
				}
				else {
				}
				
				}
				}
			}
			catch (RecognitionException pe) {
				synPredMatched168 = false;
			}
			rewind(_m168);
inputState.guessing--;
		}
		if ( synPredMatched168 ) {
			mVAR1(false);
			{
			if ((_tokenSet_5.member(LA(1)))) {
				_saveIndex=text.length();
				mWS(false);
				text.setLength(_saveIndex);
			}
			else {
			}
			
			}
			if ( inputState.guessing==0 ) {
				_ttype = AVAR;
			}
		}
		else if ((LA(1)=='$') && (true) && (true)) {
			mVAR1(false);
		}
		else {
			throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());
		}
		
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	public final void mSYM(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = SYM;
		int _saveIndex;
		
		switch ( LA(1)) {
		case '(':
		{
			match('(');
			break;
		}
		case ')':
		{
			match(')');
			break;
		}
		case '[':
		{
			match('[');
			break;
		}
		case ']':
		{
			match(']');
			break;
		}
		case ',':
		{
			match(',');
			break;
		}
		case '|':
		{
			match('|');
			break;
		}
		case '}':
		{
			match('}');
			break;
		}
		case '=':
		{
			match('=');
			{
			if ((LA(1)=='=')) {
				match('=');
			}
			else {
			}
			
			}
			break;
		}
		case '<':  case '>':
		{
			{
			switch ( LA(1)) {
			case '<':
			{
				match('<');
				break;
			}
			case '>':
			{
				match('>');
				break;
			}
			default:
			{
				throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());
			}
			}
			}
			{
			if ((LA(1)=='=')) {
				match('=');
			}
			else {
			}
			
			}
			break;
		}
		case '!':
		{
			match("!=");
			break;
		}
		case '/':
		{
			match('/');
			break;
		}
		case '*':
		{
			match('*');
			break;
		}
		case '+':
		{
			match('+');
			break;
		}
		case '-':
		{
			match('-');
			break;
		}
		case ':':
		{
			match(':');
			{
			switch ( LA(1)) {
			case ':':
			{
				match(':');
				break;
			}
			case '=':
			{
				match('=');
				break;
			}
			default:
				{
				}
			}
			}
			break;
		}
		case '?':
		{
			match('?');
			break;
		}
		default:
		{
			throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());
		}
		}
		_ttype = testLiteralsTable(_ttype);
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	public final void mSYM2(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = SYM2;
		int _saveIndex;
		
		match('{');
		{
		if ((_tokenSet_6.member(LA(1)))) {
			{
			switch ( LA(1)) {
			case '=':
			{
				match('=');
				break;
			}
			case '<':  case '>':
			{
				{
				{
				switch ( LA(1)) {
				case '<':
				{
					match('<');
					break;
				}
				case '>':
				{
					match('>');
					break;
				}
				default:
				{
					throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());
				}
				}
				}
				{
				switch ( LA(1)) {
				case '=':
				{
					match('=');
					break;
				}
				case '}':
				{
					break;
				}
				default:
				{
					throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());
				}
				}
				}
				}
				break;
			}
			case '!':
			{
				match("!=");
				break;
			}
			default:
			{
				throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());
			}
			}
			}
			match('}');
		}
		else {
		}
		
		}
		_ttype = testLiteralsTable(_ttype);
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	public final void mSEMI(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = SEMI;
		int _saveIndex;
		
		match(';');
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	protected final void mSTRCHAR(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = STRCHAR;
		int _saveIndex;
		Token x1=null;
		Token x2=null;
		Token u1=null;
		Token u2=null;
		Token u3=null;
		Token u4=null;
		
		switch ( LA(1)) {
		case '\n':  case '\r':
		{
			{
			if ((LA(1)=='\r') && (LA(2)=='\n') && ((LA(3) >= '\u0003' && LA(3) <= '\u00ff'))) {
				match("\r\n");
			}
			else if ((LA(1)=='\r') && ((LA(2) >= '\u0003' && LA(2) <= '\u00ff')) && (true)) {
				match('\r');
			}
			else if ((LA(1)=='\n')) {
				match('\n');
			}
			else {
				throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());
			}
			
			}
			if ( inputState.guessing==0 ) {
				newline(); text.setLength(_begin); text.append("\n");
			}
			break;
		}
		case '\\':
		{
			match('\\');
			{
			switch ( LA(1)) {
			case '\'':
			{
				match('\'');
				if ( inputState.guessing==0 ) {
					text.setLength(_begin); text.append("\'");
				}
				break;
			}
			case '"':
			{
				match('\"');
				if ( inputState.guessing==0 ) {
					text.setLength(_begin); text.append("\"");
				}
				break;
			}
			case '\\':
			{
				match('\\');
				if ( inputState.guessing==0 ) {
					text.setLength(_begin); text.append("\\");
				}
				break;
			}
			case '/':
			{
				match('/');
				if ( inputState.guessing==0 ) {
					text.setLength(_begin); text.append("/");
				}
				break;
			}
			case 'b':
			{
				match('b');
				if ( inputState.guessing==0 ) {
					text.setLength(_begin); text.append("\b");
				}
				break;
			}
			case 'f':
			{
				match('f');
				if ( inputState.guessing==0 ) {
					text.setLength(_begin); text.append("\f");
				}
				break;
			}
			case 'n':
			{
				match('n');
				if ( inputState.guessing==0 ) {
					text.setLength(_begin); text.append("\n");
				}
				break;
			}
			case 'r':
			{
				match('r');
				if ( inputState.guessing==0 ) {
					text.setLength(_begin); text.append("\r");
				}
				break;
			}
			case 't':
			{
				match('t');
				if ( inputState.guessing==0 ) {
					text.setLength(_begin); text.append("\t");
				}
				break;
			}
			case 'X':  case 'x':
			{
				{
				switch ( LA(1)) {
				case 'x':
				{
					match('x');
					break;
				}
				case 'X':
				{
					match('X');
					break;
				}
				default:
				{
					throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());
				}
				}
				}
				mHEX(true);
				x1=_returnToken;
				mHEX(true);
				x2=_returnToken;
				if ( inputState.guessing==0 ) {
					byte b = BaseUtil.parseHexByte(x1.getText().charAt(0),
						                             x2.getText().charAt(0));
					String s = Character.toString((char)b);
					text.setLength(_begin); text.append( s);
				}
				break;
			}
			case 'U':  case 'u':
			{
				{
				switch ( LA(1)) {
				case 'u':
				{
					match('u');
					break;
				}
				case 'U':
				{
					match('U');
					break;
				}
				default:
				{
					throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());
				}
				}
				}
				mHEX(true);
				u1=_returnToken;
				mHEX(true);
				u2=_returnToken;
				mHEX(true);
				u3=_returnToken;
				mHEX(true);
				u4=_returnToken;
				if ( inputState.guessing==0 ) {
					char c = BaseUtil.parseUnicode(u1.getText().charAt(0),
						                             u2.getText().charAt(0),
						                             u3.getText().charAt(0),
						                             u4.getText().charAt(0));
					String s = Character.toString(c);
					text.setLength(_begin); text.append( s);
				}
				break;
			}
			default:
			{
				throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());
			}
			}
			}
			break;
		}
		default:
			if ((_tokenSet_7.member(LA(1)))) {
				{
				{
				match(_tokenSet_7);
				}
				}
			}
		else {
			throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());
		}
		}
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	public final void mSTR(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = STR;
		int _saveIndex;
		
		switch ( LA(1)) {
		case '"':
		{
			_saveIndex=text.length();
			match('\"');
			text.setLength(_saveIndex);
			{
			_loop191:
			do {
				if ((_tokenSet_8.member(LA(1)))) {
					mSTRCHAR(false);
				}
				else if ((LA(1)=='\'')) {
					match('\'');
				}
				else {
					break _loop191;
				}
				
			} while (true);
			}
			_saveIndex=text.length();
			match('\"');
			text.setLength(_saveIndex);
			break;
		}
		case '\'':
		{
			_saveIndex=text.length();
			match('\'');
			text.setLength(_saveIndex);
			{
			_loop193:
			do {
				if ((_tokenSet_8.member(LA(1)))) {
					mSTRCHAR(false);
				}
				else if ((LA(1)=='"')) {
					match('\"');
				}
				else {
					break _loop193;
				}
				
			} while (true);
			}
			_saveIndex=text.length();
			match('\'');
			text.setLength(_saveIndex);
			break;
		}
		default:
		{
			throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());
		}
		}
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	public final void mHEXSTR(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = HEXSTR;
		int _saveIndex;
		
		if ((LA(1)=='X'||LA(1)=='x') && (LA(2)=='"')) {
			{
			switch ( LA(1)) {
			case 'x':
			{
				_saveIndex=text.length();
				match('x');
				text.setLength(_saveIndex);
				break;
			}
			case 'X':
			{
				_saveIndex=text.length();
				match('X');
				text.setLength(_saveIndex);
				break;
			}
			default:
			{
				throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());
			}
			}
			}
			_saveIndex=text.length();
			match('\"');
			text.setLength(_saveIndex);
			{
			_loop198:
			do {
				if ((_tokenSet_9.member(LA(1)))) {
					{
					match(_tokenSet_9);
					}
				}
				else {
					break _loop198;
				}
				
			} while (true);
			}
			_saveIndex=text.length();
			match('\"');
			text.setLength(_saveIndex);
		}
		else if ((LA(1)=='X'||LA(1)=='x') && (LA(2)=='\'')) {
			{
			switch ( LA(1)) {
			case 'x':
			{
				_saveIndex=text.length();
				match('x');
				text.setLength(_saveIndex);
				break;
			}
			case 'X':
			{
				_saveIndex=text.length();
				match('X');
				text.setLength(_saveIndex);
				break;
			}
			default:
			{
				throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());
			}
			}
			}
			_saveIndex=text.length();
			match('\'');
			text.setLength(_saveIndex);
			{
			_loop202:
			do {
				if ((_tokenSet_10.member(LA(1)))) {
					{
					match(_tokenSet_10);
					}
				}
				else {
					break _loop202;
				}
				
			} while (true);
			}
			_saveIndex=text.length();
			match('\'');
			text.setLength(_saveIndex);
		}
		else {
			throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());
		}
		
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	protected final void mINT(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = INT;
		int _saveIndex;
		
		{
		int _cnt205=0;
		_loop205:
		do {
			if (((LA(1) >= '0' && LA(1) <= '9'))) {
				mDIGIT(false);
			}
			else {
				if ( _cnt205>=1 ) { break _loop205; } else {throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());}
			}
			
			_cnt205++;
		} while (true);
		}
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	protected final void mDEC(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = DEC;
		int _saveIndex;
		
		switch ( LA(1)) {
		case '0':  case '1':  case '2':  case '3':
		case '4':  case '5':  case '6':  case '7':
		case '8':  case '9':
		{
			mINT(false);
			{
			switch ( LA(1)) {
			case '.':
			{
				{
				match('.');
				mINT(false);
				{
				if ((LA(1)=='E'||LA(1)=='e')) {
					{
					switch ( LA(1)) {
					case 'e':
					{
						match('e');
						break;
					}
					case 'E':
					{
						match('E');
						break;
					}
					default:
					{
						throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());
					}
					}
					}
					{
					switch ( LA(1)) {
					case '+':
					{
						match('+');
						break;
					}
					case '-':
					{
						match('-');
						break;
					}
					case '0':  case '1':  case '2':  case '3':
					case '4':  case '5':  case '6':  case '7':
					case '8':  case '9':
					{
						break;
					}
					default:
					{
						throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());
					}
					}
					}
					mINT(false);
				}
				else {
				}
				
				}
				}
				break;
			}
			case 'E':  case 'e':
			{
				{
				{
				switch ( LA(1)) {
				case 'e':
				{
					match('e');
					break;
				}
				case 'E':
				{
					match('E');
					break;
				}
				default:
				{
					throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());
				}
				}
				}
				{
				switch ( LA(1)) {
				case '+':
				{
					match('+');
					break;
				}
				case '-':
				{
					match('-');
					break;
				}
				case '0':  case '1':  case '2':  case '3':
				case '4':  case '5':  case '6':  case '7':
				case '8':  case '9':
				{
					break;
				}
				default:
				{
					throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());
				}
				}
				}
				mINT(false);
				}
				break;
			}
			default:
			{
				throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());
			}
			}
			}
			break;
		}
		case '.':
		{
			match('.');
			mINT(false);
			{
			if ((LA(1)=='E'||LA(1)=='e')) {
				{
				switch ( LA(1)) {
				case 'e':
				{
					match('e');
					break;
				}
				case 'E':
				{
					match('E');
					break;
				}
				default:
				{
					throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());
				}
				}
				}
				{
				switch ( LA(1)) {
				case '+':
				{
					match('+');
					break;
				}
				case '-':
				{
					match('-');
					break;
				}
				case '0':  case '1':  case '2':  case '3':
				case '4':  case '5':  case '6':  case '7':
				case '8':  case '9':
				{
					break;
				}
				default:
				{
					throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());
				}
				}
				}
				mINT(false);
			}
			else {
			}
			
			}
			break;
		}
		default:
		{
			throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());
		}
		}
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	public final void mDOTTY(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = DOTTY;
		int _saveIndex;
		
		boolean synPredMatched220 = false;
		if (((_tokenSet_11.member(LA(1))) && (_tokenSet_12.member(LA(2))) && (true))) {
			int _m220 = mark();
			synPredMatched220 = true;
			inputState.guessing++;
			try {
				{
				mDEC(false);
				}
			}
			catch (RecognitionException pe) {
				synPredMatched220 = false;
			}
			rewind(_m220);
inputState.guessing--;
		}
		if ( synPredMatched220 ) {
			mDEC(false);
			{
			switch ( LA(1)) {
			case 'm':
			{
				_saveIndex=text.length();
				match("m");
				text.setLength(_saveIndex);
				if ( inputState.guessing==0 ) {
					_ttype = DEC;
				}
				break;
			}
			case 'd':
			{
				_saveIndex=text.length();
				match("d");
				text.setLength(_saveIndex);
				if ( inputState.guessing==0 ) {
					_ttype = DOUBLE;
				}
				break;
			}
			default:
				{
					if ( inputState.guessing==0 ) {
						_ttype = DEC;
					}
				}
			}
			}
		}
		else if ((LA(1)=='0') && (LA(2)=='X'||LA(2)=='x')) {
			match('0');
			{
			switch ( LA(1)) {
			case 'x':
			{
				match('x');
				break;
			}
			case 'X':
			{
				match('X');
				break;
			}
			default:
			{
				throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());
			}
			}
			}
			{
			int _cnt225=0;
			_loop225:
			do {
				if ((_tokenSet_13.member(LA(1)))) {
					mHEX(false);
				}
				else {
					if ( _cnt225>=1 ) { break _loop225; } else {throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());}
				}
				
				_cnt225++;
			} while (true);
			}
			if ( inputState.guessing==0 ) {
				_ttype = INT;
			}
		}
		else {
			boolean synPredMatched227 = false;
			if (((LA(1)=='.') && (_tokenSet_1.member(LA(2))) && (true))) {
				int _m227 = mark();
				synPredMatched227 = true;
				inputState.guessing++;
				try {
					{
					mDOT_ID(false);
					}
				}
				catch (RecognitionException pe) {
					synPredMatched227 = false;
				}
				rewind(_m227);
inputState.guessing--;
			}
			if ( synPredMatched227 ) {
				mDOT_ID(false);
				if ( inputState.guessing==0 ) {
					_ttype = DOT_ID;
				}
			}
			else if (((LA(1) >= '0' && LA(1) <= '9')) && (true) && (true)) {
				mINT(false);
				{
				switch ( LA(1)) {
				case 'm':
				{
					_saveIndex=text.length();
					match("m");
					text.setLength(_saveIndex);
					if ( inputState.guessing==0 ) {
						_ttype = INT;
					}
					break;
				}
				case 'd':
				{
					_saveIndex=text.length();
					match("d");
					text.setLength(_saveIndex);
					if ( inputState.guessing==0 ) {
						_ttype = DOUBLE;
					}
					break;
				}
				default:
					{
						if ( inputState.guessing==0 ) {
							_ttype = INT;
						}
					}
				}
				}
			}
			else if ((LA(1)=='.') && (true)) {
				match('.');
			}
			else {
				throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());
			}
			}
			_ttype = testLiteralsTable(_ttype);
			if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
				_token = makeToken(_ttype);
				_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
			}
			_returnToken = _token;
		}
		
	protected final void mDOT_ID(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = DOT_ID;
		int _saveIndex;
		
		_saveIndex=text.length();
		match('.');
		text.setLength(_saveIndex);
		mIDWORD(false);
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	protected final void mIDWORD(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = IDWORD;
		int _saveIndex;
		
		{
		switch ( LA(1)) {
		case '@':
		{
			match('@');
			break;
		}
		case '_':
		{
			match('_');
			break;
		}
		case 'A':  case 'B':  case 'C':  case 'D':
		case 'E':  case 'F':  case 'G':  case 'H':
		case 'I':  case 'J':  case 'K':  case 'L':
		case 'M':  case 'N':  case 'O':  case 'P':
		case 'Q':  case 'R':  case 'S':  case 'T':
		case 'U':  case 'V':  case 'W':  case 'X':
		case 'Y':  case 'Z':  case 'a':  case 'b':
		case 'c':  case 'd':  case 'e':  case 'f':
		case 'g':  case 'h':  case 'i':  case 'j':
		case 'k':  case 'l':  case 'm':  case 'n':
		case 'o':  case 'p':  case 'q':  case 'r':
		case 's':  case 't':  case 'u':  case 'v':
		case 'w':  case 'x':  case 'y':  case 'z':
		{
			mLETTER(false);
			break;
		}
		default:
		{
			throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());
		}
		}
		}
		{
		_loop231:
		do {
			switch ( LA(1)) {
			case '@':
			{
				match('@');
				break;
			}
			case '_':
			{
				match('_');
				break;
			}
			case '#':
			{
				match('#');
				break;
			}
			case 'A':  case 'B':  case 'C':  case 'D':
			case 'E':  case 'F':  case 'G':  case 'H':
			case 'I':  case 'J':  case 'K':  case 'L':
			case 'M':  case 'N':  case 'O':  case 'P':
			case 'Q':  case 'R':  case 'S':  case 'T':
			case 'U':  case 'V':  case 'W':  case 'X':
			case 'Y':  case 'Z':  case 'a':  case 'b':
			case 'c':  case 'd':  case 'e':  case 'f':
			case 'g':  case 'h':  case 'i':  case 'j':
			case 'k':  case 'l':  case 'm':  case 'n':
			case 'o':  case 'p':  case 'q':  case 'r':
			case 's':  case 't':  case 'u':  case 'v':
			case 'w':  case 'x':  case 'y':  case 'z':
			{
				mLETTER(false);
				break;
			}
			case '0':  case '1':  case '2':  case '3':
			case '4':  case '5':  case '6':  case '7':
			case '8':  case '9':
			{
				mDIGIT(false);
				break;
			}
			default:
			{
				break _loop231;
			}
			}
		} while (true);
		}
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	public final void mID(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = ID;
		int _saveIndex;
		
		boolean synPredMatched239 = false;
		if (((_tokenSet_1.member(LA(1))) && (true) && (true))) {
			int _m239 = mark();
			synPredMatched239 = true;
			inputState.guessing++;
			try {
				{
				mIDWORD(false);
				{
				switch ( LA(1)) {
				case '\t':  case '\n':  case '\u000c':  case '\r':
				case ' ':
				{
					_saveIndex=text.length();
					mWS(false);
					text.setLength(_saveIndex);
					break;
				}
				case '*':  case ':':  case '?':
				{
					break;
				}
				default:
				{
					throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());
				}
				}
				}
				{
				switch ( LA(1)) {
				case '*':
				{
					match('*');
					{
					switch ( LA(1)) {
					case '\t':  case '\n':  case '\u000c':  case '\r':
					case ' ':
					{
						_saveIndex=text.length();
						mWS(false);
						text.setLength(_saveIndex);
						break;
					}
					case ':':
					{
						break;
					}
					default:
					{
						throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());
					}
					}
					}
					break;
				}
				case '?':
				{
					match('?');
					{
					switch ( LA(1)) {
					case '\t':  case '\n':  case '\u000c':  case '\r':
					case ' ':
					{
						_saveIndex=text.length();
						mWS(false);
						text.setLength(_saveIndex);
						break;
					}
					case ':':
					{
						break;
					}
					default:
					{
						throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());
					}
					}
					}
					break;
				}
				case ':':
				{
					break;
				}
				default:
				{
					throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());
				}
				}
				}
				match(':');
				{
				if ((_tokenSet_5.member(LA(1)))) {
					_saveIndex=text.length();
					mWS(false);
					text.setLength(_saveIndex);
				}
				else {
				}
				
				}
				}
			}
			catch (RecognitionException pe) {
				synPredMatched239 = false;
			}
			rewind(_m239);
inputState.guessing--;
		}
		if ( synPredMatched239 ) {
			mIDWORD(false);
		}
		else if ((_tokenSet_1.member(LA(1))) && (true) && (true)) {
			mIDWORD(false);
			{
			if ((_tokenSet_5.member(LA(1)))) {
				_saveIndex=text.length();
				mWS(false);
				text.setLength(_saveIndex);
			}
			else {
			}
			
			}
			if ( inputState.guessing==0 ) {
				_ttype = testLiteralsTable(_ttype);
			}
		}
		else {
			throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());
		}
		
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	public final void mDATETIME(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = DATETIME;
		int _saveIndex;
		
		if ((LA(1)=='D'||LA(1)=='d') && (LA(2)=='"')) {
			{
			switch ( LA(1)) {
			case 'd':
			{
				_saveIndex=text.length();
				match('d');
				text.setLength(_saveIndex);
				break;
			}
			case 'D':
			{
				_saveIndex=text.length();
				match('D');
				text.setLength(_saveIndex);
				break;
			}
			default:
			{
				throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());
			}
			}
			}
			_saveIndex=text.length();
			match('\"');
			text.setLength(_saveIndex);
			{
			_loop246:
			do {
				if ((_tokenSet_9.member(LA(1)))) {
					{
					match(_tokenSet_9);
					}
				}
				else {
					break _loop246;
				}
				
			} while (true);
			}
			_saveIndex=text.length();
			match('\"');
			text.setLength(_saveIndex);
		}
		else if ((LA(1)=='D'||LA(1)=='d') && (LA(2)=='\'')) {
			{
			switch ( LA(1)) {
			case 'd':
			{
				_saveIndex=text.length();
				match('d');
				text.setLength(_saveIndex);
				break;
			}
			case 'D':
			{
				_saveIndex=text.length();
				match('D');
				text.setLength(_saveIndex);
				break;
			}
			default:
			{
				throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());
			}
			}
			}
			_saveIndex=text.length();
			match('\'');
			text.setLength(_saveIndex);
			{
			_loop250:
			do {
				if ((_tokenSet_10.member(LA(1)))) {
					{
					match(_tokenSet_10);
					}
				}
				else {
					break _loop250;
				}
				
			} while (true);
			}
			_saveIndex=text.length();
			match('\'');
			text.setLength(_saveIndex);
		}
		else {
			throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());
		}
		
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	
	private static final long[] mk_tokenSet_0() {
		long[] data = { -864481113144295424L, 3458764514491629568L, 0L, 0L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_0 = new BitSet(mk_tokenSet_0());
	private static final long[] mk_tokenSet_1() {
		long[] data = { 0L, 576460745995190271L, 0L, 0L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_1 = new BitSet(mk_tokenSet_1());
	private static final long[] mk_tokenSet_2() {
		long[] data = new long[8];
		data[0]=-9224L;
		for (int i = 1; i<=3; i++) { data[i]=-1L; }
		return data;
	}
	public static final BitSet _tokenSet_2 = new BitSet(mk_tokenSet_2());
	private static final long[] mk_tokenSet_3() {
		long[] data = new long[8];
		data[0]=-140737488355336L;
		for (int i = 1; i<=3; i++) { data[i]=-1L; }
		return data;
	}
	public static final BitSet _tokenSet_3 = new BitSet(mk_tokenSet_3());
	private static final long[] mk_tokenSet_4() {
		long[] data = new long[8];
		data[0]=-4398046520328L;
		for (int i = 1; i<=3; i++) { data[i]=-1L; }
		return data;
	}
	public static final BitSet _tokenSet_4 = new BitSet(mk_tokenSet_4());
	private static final long[] mk_tokenSet_5() {
		long[] data = { 4294981120L, 0L, 0L, 0L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_5 = new BitSet(mk_tokenSet_5());
	private static final long[] mk_tokenSet_6() {
		long[] data = { 8070450540837863424L, 0L, 0L, 0L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_6 = new BitSet(mk_tokenSet_6());
	private static final long[] mk_tokenSet_7() {
		long[] data = new long[8];
		data[0]=-566935692296L;
		data[1]=-268435457L;
		for (int i = 2; i<=3; i++) { data[i]=-1L; }
		return data;
	}
	public static final BitSet _tokenSet_7 = new BitSet(mk_tokenSet_7());
	private static final long[] mk_tokenSet_8() {
		long[] data = new long[8];
		data[0]=-566935683080L;
		for (int i = 1; i<=3; i++) { data[i]=-1L; }
		return data;
	}
	public static final BitSet _tokenSet_8 = new BitSet(mk_tokenSet_8());
	private static final long[] mk_tokenSet_9() {
		long[] data = new long[8];
		data[0]=-17179869192L;
		for (int i = 1; i<=3; i++) { data[i]=-1L; }
		return data;
	}
	public static final BitSet _tokenSet_9 = new BitSet(mk_tokenSet_9());
	private static final long[] mk_tokenSet_10() {
		long[] data = new long[8];
		data[0]=-549755813896L;
		for (int i = 1; i<=3; i++) { data[i]=-1L; }
		return data;
	}
	public static final BitSet _tokenSet_10 = new BitSet(mk_tokenSet_10());
	private static final long[] mk_tokenSet_11() {
		long[] data = { 288019269919178752L, 0L, 0L, 0L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_11 = new BitSet(mk_tokenSet_11());
	private static final long[] mk_tokenSet_12() {
		long[] data = { 288019269919178752L, 137438953504L, 0L, 0L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_12 = new BitSet(mk_tokenSet_12());
	private static final long[] mk_tokenSet_13() {
		long[] data = { 287948901175001088L, 541165879422L, 0L, 0L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_13 = new BitSet(mk_tokenSet_13());
	
	}
