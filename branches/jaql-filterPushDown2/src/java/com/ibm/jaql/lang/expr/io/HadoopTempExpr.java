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
package com.ibm.jaql.lang.expr.io;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.ibm.jaql.io.Adapter;
import com.ibm.jaql.json.schema.RecordSchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.schema.StringSchema;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonSchema;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ExprProperty;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.lang.expr.function.JsonValueParameter;
import com.ibm.jaql.lang.expr.function.JsonValueParameters;
import com.ibm.jaql.util.DeleteFileTask;

/** Creates a file descriptor for temporary files used by Jaql. Takes a schema argument that
 * describes the schema of the individual values written to the file. */
public class HadoopTempExpr extends Expr
{
  public static final class Descriptor extends DefaultBuiltInFunctionDescriptor
  {
    public Descriptor()
    {
      super(
          "HadoopTemp",
          HadoopTempExpr.class,
          new JsonValueParameters(
              new JsonValueParameter("schema", SchemaFactory.schematypeSchema(), new JsonSchema(SchemaFactory.anySchema()))),
          SchemaFactory.recordSchema());
    }
  }
  
  
  /**
   * @param exprs
   */
  public HadoopTempExpr(Expr ... exprs)
  {
    super(exprs);
  }

  public Map<ExprProperty, Boolean> getProperties()
  {
    Map<ExprProperty, Boolean> result = super.getProperties();
    result.put(ExprProperty.HAS_SIDE_EFFECTS, true);
    return result;
  }
  

  /**
   * 
   */
  public HadoopTempExpr()
  {
    super(NO_EXPRS);
  }

  @Override
  public Schema getSchema()
  {
    // determine options 
    RecordSchema.Field options[] = new RecordSchema.Field[1];
    options[0] = new RecordSchema.Field(new JsonString("schema"), SchemaFactory.schematypeSchema(), false);
    if (exprs[0].isCompileTimeComputable().always())
    {
      try
      {
        JsonSchema schema = (JsonSchema)exprs[0].compileTimeEval();
        options[0] = new RecordSchema.Field(
            new JsonString("schema"), SchemaFactory.schemaOf(schema), false);
      } catch (Exception e)
      {
        // ignore
      }
    }
    
    // compute result
    RecordSchema.Field fields[] = new RecordSchema.Field[3];
    fields[0] = new RecordSchema.Field(
        Adapter.LOCATION_NAME, 
        SchemaFactory.stringSchema(), 
        false);
    fields[1] = new RecordSchema.Field(
        Adapter.TYPE_NAME, 
        new StringSchema(new JsonString("jaqltemp")), 
        false);
    fields[2] = new RecordSchema.Field(Adapter.OPTIONS_NAME, 
        new RecordSchema(options, SchemaFactory.anySchema()), false);
    
    // return resutl
    return new RecordSchema(fields, null);
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.Expr#eval(com.ibm.jaql.lang.Context)
   */
  public JsonRecord eval(Context context) throws Exception
  {
    String filename = "jaqltemp_" + System.nanoTime();     // FIXME: figure out where this should go
    BufferedJsonRecord r = new BufferedJsonRecord();
    r.add(Adapter.TYPE_NAME, new JsonString("jaqltemp"));
    r.add(Adapter.LOCATION_NAME, new JsonString(filename));
    BufferedJsonRecord options = new BufferedJsonRecord();
    JsonSchema schema = (JsonSchema)exprs[0].eval(context);
    options.add(new JsonString("schema"), schema);
    r.add(Adapter.OPTIONS_NAME, options);
    
    Configuration conf = new Configuration(); // TODO: where to get this from?
    Path path = new Path(filename);
    FileSystem fs = path.getFileSystem(conf);
    context.doAtReset(new DeleteFileTask(fs, path));
    return r; // TODO: memory
  }
}
