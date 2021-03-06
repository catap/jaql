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
package com.ibm.jaql.lang.expr.catalog;

import com.ibm.jaql.catalog.Catalog;
import com.ibm.jaql.catalog.CatalogImpl;
import com.ibm.jaql.io.Adapter;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

/**
 * An expression to update catalog entry. It can only add records or fields. No
 * old records or fields will be overwritten.
 */
public class CatalogUpdateFn extends Expr 
{
	public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par12
	{
		public Descriptor()
		{
			super("catalogUpdate", CatalogUpdateFn.class);
		}
	}
	
	  /**
	   * @param exprs
	   */
	  public CatalogUpdateFn(Expr[] exprs)
	  {
	    super(exprs);
	  }
	  

	  /*
	   * (non-Javadoc)
	   * 
	   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
	   */
	  
	  public JsonRecord eval(final Context context) throws Exception
	  {
		  JsonRecord val = (JsonRecord) exprs[0].eval(context);
		  JsonString key = (JsonString) exprs[1].eval(context);
		  
		  if (key == null ) 
		  {
			  // need to create a key for this catalog entry
			  JsonString type = (JsonString) val.get(Adapter.TYPE_NAME);
			  JsonString location = (JsonString) val.get(Adapter.LOCATION_NAME);
			  key = new JsonString( type.toString() + "/" + location.toString() );
		  }

		  Catalog cat = new CatalogImpl();
		  cat.open();
		  cat.update(key, val, true);   // "upserts" old record, if any
		  return val;
	  }
}
