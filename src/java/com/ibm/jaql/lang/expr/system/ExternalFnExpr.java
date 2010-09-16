/*
 * Copyright (C) IBM Corp. 2010.
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
package com.ibm.jaql.lang.expr.system;

import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Env;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.MacroExpr;
import com.ibm.jaql.lang.expr.core.RecordExpr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

/** An expression that constructs a JSON value for a Java UDF */
public class ExternalFnExpr extends MacroExpr {

    public static class Descriptor extends
            DefaultBuiltInFunctionDescriptor.Par11 {
        public Descriptor() {
            super("externalfn", ExternalFnExpr.class);
        }
    }

    public ExternalFnExpr(Expr... exprs) {
        super(exprs);
    }

    // @Override
    // public Schema getSchema()
    // {
    // return SchemaFactory.functionSchema();
    // }
    //  
    // public Map<ExprProperty, Boolean> getProperties()
    // {
    // Map<ExprProperty, Boolean> result = super.getProperties();
    // result.put(ExprProperty.ALLOW_COMPILE_TIME_COMPUTATION, true);
    // return result;
    // }
    //  
    // @Override
    // public ExternalFnFunction eval(Context context) throws Exception {
    // ExternalFnFunction f = new
    // ExternalFnFunction(((JsonString)exprs[0].eval(context)).toString());
    // return f;
    // }

    /**
     * JavaUdf is a MacroExpr to ensure that it is a literal class at parse
     * time.
     */
    @Override
    public Expr expand(Env env) throws Exception {
        Expr e = exprs[0];

        if (!(e instanceof RecordExpr)) {
            throw new RuntimeException(
                    "externalfn() requires a Json record");
        }

        JsonRecord rec = (JsonRecord) e.eval(null);
        ConstExpr ce = new ConstExpr(new ExternalFnFunction(rec));

        return ce;
    }

}
