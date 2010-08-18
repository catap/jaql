package com.ibm.jaql.lang.expr.sql;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.RecordSchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.RecordSchema.Field;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.lang.core.Env;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.ArrayExpr;
import com.ibm.jaql.lang.expr.core.BindingExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.FilterExpr;
import com.ibm.jaql.lang.expr.core.ForExpr;
import com.ibm.jaql.lang.expr.core.MultiJoinExpr;
import com.ibm.jaql.lang.expr.core.NameValueBinding;
import com.ibm.jaql.lang.expr.core.RecordExpr;
import com.ibm.jaql.lang.expr.core.TransformExpr;



public class SqlSelect extends SqlTableExpr
{
  private List<SqlTableImport> from;
  private SqlExpr where;
  private List<SqlColumnExpr> columns;

  public SqlSelect(List<SqlTableImport> from, SqlExpr where, List<SqlColumnExpr> columns)
  {
    this.from = from;
    this.where = where;
    this.columns = columns;
  }

  @Override
  public List<SqlColumnExpr> columns()
  {
    return columns;
  }

  @Override
  public void resolveColumns(Stack<SqlTableImport> context) throws SQLException
  {
    for( SqlTableImport f: from )
    {
      f.table.resolveColumns(context);
      if( f.alias == null )
      {
        f.alias = "i" + context.size();
      }
      context.push(f);
    }
    if( where != null )
    {
      where.resolveColumns(context);
    }
    for( SqlColumnExpr c: columns )
    {
      c.resolveColumns(context);
    }
    for( int i = from.size() ; i > 0 ; i-- )
    {
      context.pop();
    }
  }

  @Override
  public Expr toJaql(Env env) throws Exception
  {
    ArrayList<BindingExpr> bindings = new ArrayList<BindingExpr>(from.size());
    for(SqlTableImport t: from)
    {
      t.iterVar = env.makeVar(t.alias);
      Expr e = t.table.toJaql(env);
      bindings.add( new BindingExpr(BindingExpr.Type.IN, t.iterVar, null, e) );
    }
    
    Expr[] fieldExprs = new Expr[columns.size()];
    for( int i = 0 ; i < fieldExprs.length ; i++ )
    {
      SqlColumnExpr c = columns.get(i);
      Expr e = c.expr.toJaql(env);
      fieldExprs[i] = new NameValueBinding(c.id, e, true);
    }
    Expr ret = new RecordExpr(fieldExprs);
    
    if( from.size() > 1 )
    {
      ret = new ArrayExpr(ret);
      if( where == null )
      {
        for(int i = bindings.size() - 1 ; i >= 0 ; i-- )
        {
          ret = new ForExpr(bindings.get(i), ret);
        }
        return ret;
      }
      else
      {
        return new MultiJoinExpr(bindings, where.toJaql(env), null, ret).expand(env);
      }
    }
    else
    {
      BindingExpr bind = bindings.get(0);
      if( where != null )
      {
        Var iterVar2 = env.makeVar(bind.var.name());
        BindingExpr bind2 = new BindingExpr(BindingExpr.Type.IN, iterVar2, null, 
            new FilterExpr(bind, where.toJaql(env)));
        ret.replaceVar(bind.var, iterVar2);
        bind = bind2;
      }
      return new TransformExpr(bind, ret);
    }
  }

  @Override
  public RecordSchema getRowSchema()
  {
    Field[] fields = new Field[columns.size()];
    for(int i = 0 ; i < fields.length ; i++)
    {
      SqlColumnExpr c = columns.get(i);
      fields[i] = new Field(new JsonString(c.id), c.getSchema(), false);
    }
    return new RecordSchema(fields, null);
  }

  @Override
  public Schema getSchema()
  {
    return new ArraySchema(null, getRowSchema());
  }
}
