// $ANTLR 2.7.6 (2005-12-22): "jaql.g" -> "JaqlLexer.java"$

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


public interface JaqlTokenTypes {
	int EOF = 1;
	int NULL_TREE_LOOKAHEAD = 3;
	int SEMI = 4;
	int LITERAL_explain = 5;
	int LITERAL_materialize = 6;
	int LITERAL_quit = 7;
	// "," = 8
	// "->" = 9
	int LITERAL_in = 10;
	int LITERAL_aggregate = 11;
	int LITERAL_agg = 12;
	int LITERAL_group = 13;
	int LITERAL_each = 14;
	int LITERAL_using = 15;
	int LITERAL_as = 16;
	int LITERAL_by = 17;
	// "=" = 18
	int LITERAL_into = 19;
	int LITERAL_expand = 20;
	int LITERAL_cmp = 21;
	// "(" = 22
	// ")" = 23
	// "[" = 24
	// "]" = 25
	int LITERAL_asc = 26;
	int LITERAL_desc = 27;
	int LITERAL_join = 28;
	int LITERAL_where = 29;
	int LITERAL_preserve = 30;
	int LITERAL_equijoin = 31;
	int LITERAL_on = 32;
	int LITERAL_split = 33;
	int LITERAL_if = 34;
	int LITERAL_else = 35;
	int LITERAL_filter = 36;
	int LITERAL_transform = 37;
	int LITERAL_top = 38;
	int LITERAL_unroll = 39;
	// "=>" = 40
	int LITERAL_extern = 41;
	int LITERAL_fn = 42;
	int LITERAL_script = 43;
	int LITERAL_import = 44;
	int LITERAL_for = 45;
	int LITERAL_sort = 46;
	// "{" = 47
	// "}" = 48
	// "?" = 49
	// ":" = 50
	int DOT_STAR = 51;
	int LITERAL_or = 52;
	int LITERAL_and = 53;
	int LITERAL_not = 54;
	int LITERAL_isnull = 55;
	int LITERAL_exists = 56;
	int LITERAL_isdefined = 57;
	// "==" = 58
	// "<" = 59
	// ">" = 60
	// "!=" = 61
	// "<=" = 62
	// ">=" = 63
	int LITERAL_instanceof = 64;
	// "+" = 65
	// "-" = 66
	// "*" = 67
	// "/" = 68
	int LITERAL_type = 69;
	// "!" = 70
	int DOT = 71;
	int INT = 72;
	int DEC = 73;
	int DOUBLE = 74;
	int HEXSTR = 75;
	int DATETIME = 76;
	int LITERAL_true = 77;
	int LITERAL_false = 78;
	int LITERAL_null = 79;
	int STR = 80;
	int HERE_STRING = 81;
	int BLOCK_STRING = 82;
	int AVAR = 83;
	int VAR = 84;
	int ID = 85;
	int DOT_ID = 86;
	int FNAME = 87;
	// "|" = 88
	int DIGIT = 89;
	int HEX = 90;
	int LETTER = 91;
	int WS = 92;
	int COMMENT = 93;
	int NL = 94;
	int ML_COMMENT = 95;
	int BLOCK_LINE1 = 96;
	int HERE_TAG = 97;
	int HERE_LINE = 98;
	int HERE_END = 99;
	int VAR1 = 100;
	int SYM = 101;
	int SYM2 = 102;
	int STRCHAR = 103;
	int DOTTY = 104;
	int IDWORD = 105;
}
