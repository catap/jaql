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

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JRecord;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.CopyField;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.FieldExpr;
import com.ibm.jaql.lang.expr.core.FieldValueExpr;
import com.ibm.jaql.lang.expr.core.NameValueBinding;
import com.ibm.jaql.lang.expr.core.RecordExpr;

/**
 * 
 */
public class ConstFieldAccess extends Rewrite
{
  /**
   * @param phase
   */
  public ConstFieldAccess(RewritePhase phase)
  {
    super(phase, FieldValueExpr.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.rewrite.Rewrite#rewrite(com.ibm.jaql.lang.expr.core.Expr)
   */
  @Override
  public boolean rewrite(Expr expr)
  {
    FieldValueExpr fe = (FieldValueExpr) expr;

    Expr recExpr = fe.recExpr();
    if (!(recExpr instanceof ConstExpr || recExpr instanceof RecordExpr))
    {
      return false;
    }

    Expr nameExpr = fe.nameExpr();
    if (!(nameExpr instanceof ConstExpr))
    {
      return false;
    }

    ConstExpr c = (ConstExpr) nameExpr;
    JString name = (JString) c.value.get();
    Expr replaceBy = null;
    if (name == null)
    {
      replaceBy = c; // rec.(null) -> null
    }
    else if (recExpr instanceof ConstExpr)
    {
      c = (ConstExpr) recExpr;
      if (!(c.value.get() instanceof JRecord))
      {
        return false;
      }
      JRecord rec = (JRecord) c.value.get();
      Item item = rec.getValue(name);
      c.value = item;
      replaceBy = c;
    }
    else  // recExpr instanceof RecordExpr
    {
      RecordExpr re = (RecordExpr) recExpr;
      for (int i = 0; i < re.numFields(); i++)
      {
        FieldExpr f = re.field(i);
        if (f instanceof NameValueBinding)
        {
          // TODO: we could do some analysis on ProjPatterns too.
          NameValueBinding b = (NameValueBinding) f;
          if ( b.staticNameMatches(name).always() )
          {
            if (replaceBy != null)
            {
              throw new RuntimeException("duplicate field in record:" + name);
            }
            replaceBy = b.valueExpr();
          }
        }
        else if (f instanceof CopyField)
        {
          // If a ProjPattern could match, then we won't do this rewrite (improve this?)
          CopyField p = (CopyField) f;
          if (p.staticNameMatches(name).always())
          {
            if (replaceBy != null)
            {
              throw new RuntimeException("duplicate field in record:" + name);
            }
            replaceBy = p.toPathExpr();
          }
        }
        else
        {
          return false;
        }
      }
      if (replaceBy == null)
      {
        replaceBy = new ConstExpr(Item.nil);
      }
    }
    expr.replaceInParent(replaceBy);
    return true;
  }
}
