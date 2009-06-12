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


public interface JaqlTokenTypes {
	int EOF = 1;
	int NULL_TREE_LOOKAHEAD = 3;
	int LITERAL_declare = 4;
	int SEMI = 5;
	int LITERAL_explain = 6;
	int LITERAL_materialize = 7;
	int LITERAL_quit = 8;
	// "=" = 9
	int LITERAL_fn = 10;
	// "(" = 11
	// "," = 12
	// ")" = 13
	// "{" = 14
	// "}" = 15
	// "?" = 16
	// ":" = 17
	// "*" = 18
	// "." = 19
	int DOT_ID = 20;
	int STR = 21;
	int LITERAL_or = 22;
	int LITERAL_and = 23;
	int LITERAL_not = 24;
	int LITERAL_in = 25;
	// "==" = 26
	// "<" = 27
	// ">" = 28
	// "!=" = 29
	// "<=" = 30
	// ">=" = 31
	int LITERAL_instanceof = 32;
	int LITERAL_to = 33;
	// "+" = 34
	// "-" = 35
	// "/" = 36
	// "[" = 37
	// "]" = 38
	int LITERAL_true = 39;
	int LITERAL_false = 40;
	int LITERAL_null = 41;
	int INT = 42;
	int DEC = 43;
	int DOUBLE = 44;
	int HEXSTR = 45;
	int DATETIME = 46;
	int LITERAL_for = 47;
	int LITERAL_at = 48;
	int LITERAL_let = 49;
	int LITERAL_if = 50;
	int LITERAL_else = 51;
	int LITERAL_join = 52;
	int LITERAL_optional = 53;
	int LITERAL_on = 54;
	int LITERAL_group = 55;
	int LITERAL_by = 56;
	int LITERAL_into = 57;
	int LITERAL_combine = 58;
	int LITERAL_reduce = 59;
	int LITERAL_sort = 60;
	int LITERAL_asc = 61;
	int LITERAL_desc = 62;
	int VAR = 63;
	int AVAR = 64;
	int ID = 65;
	int LITERAL_type = 66;
	// "|" = 67
	int DIGIT = 68;
	int HEX = 69;
	int LETTER = 70;
	int WS = 71;
	int COMMENT = 72;
	int ML_COMMENT = 73;
	int VAR1 = 74;
	int SYM = 75;
	int SYM2 = 76;
	int STRCHAR = 77;
	int DOTTY = 78;
	int IDWORD = 79;
}
