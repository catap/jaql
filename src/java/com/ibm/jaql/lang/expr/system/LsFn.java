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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonEnum;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonUtil;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.MutableJsonDate;
import com.ibm.jaql.json.type.MutableJsonLong;
import com.ibm.jaql.json.type.MutableJsonString;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.util.Bool3;
import com.ibm.jaql.util.hadoop.HadoopShim;

/**
 * ls(glob) 
 * 
 */
public class LsFn extends IterExpr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11
  {
    public Descriptor()
    {
      super("ls", LsFn.class); // TODO: add options, like recursive, etc to define filter
    }
  }
  
  public static enum LsField implements JsonEnum
  {
    ACCESS_TIME("accessTime"),
    MODIFY_TIME("modifyTime"),
    LENGTH("length"),
    BLOCK_SIZE("blockSize"),
    REPLICATION("replication"),
    PATH("path"),
    OWNER("owner"),
    GROUP("group"),
    PERMISSION("permission");
    
    public static final JsonString[] names =
      JsonUtil.jsonStrings(LsField.values());
    
    protected final JsonString name;
    
    private LsField(String name) 
    {
      this.name = new JsonString(name);
    }

    @Override
    public JsonString jsonString() 
    {
      return name; 
    }
  }

  
  public LsFn(Expr... exprs)
  {
    super(exprs);
  }

  @Override
  public Bool3 evaluatesChildOnce(int i)
  {
    return Bool3.TRUE;
  }

  @Override
  public boolean isMappable(int i)
  {
    return i == 0;
  }

  @Override
  protected JsonIterator iterRaw(final Context context) throws Exception
  {
    JsonString glob = (JsonString)exprs[0].eval(context);
    // Configuration conf = context.getConfiguration();
    Configuration conf = new Configuration(); // TODO: get from context, incl options
    //URI uri;
    //FileSystem fs = FileSystem.get(uri, conf);
    Path inpath = new Path(glob.toString());
    FileSystem fs = inpath.getFileSystem(conf);
    //final FileStatus[] stats = fs.listStatus(path, filter);
    final FileStatus[] stats = fs.globStatus(inpath);
    
    if( stats == null || stats.length == 0 )
    {
      return JsonIterator.EMPTY;
    }
    
    final MutableJsonDate accessTime = new MutableJsonDate();
    final MutableJsonDate modifyTime = new MutableJsonDate();
    final MutableJsonLong length = new MutableJsonLong();
    final MutableJsonLong blockSize = new MutableJsonLong();
    final MutableJsonLong replication = new MutableJsonLong();
    final MutableJsonString path = new MutableJsonString();
    final MutableJsonString owner = new MutableJsonString();
    final MutableJsonString group = new MutableJsonString();
    final MutableJsonString permission = new MutableJsonString();
    final JsonValue[] values = new JsonValue[] {
        accessTime, modifyTime, length, blockSize,
        replication, path, owner, group, permission
    };
    final BufferedJsonRecord rec = new BufferedJsonRecord();
    rec.set(LsField.names, values, values.length, false);
    
    return new JsonIterator(rec)
    {
      int i = 0;
      
      @Override
      protected boolean moveNextRaw() throws Exception
      {
        if( i >= stats.length )
        {
          return false;
        }
        
        FileStatus stat = stats[i++];
        // fs.getUri().toString();
        long x = HadoopShim.getAccessTime(stat);
        if( x <= 0 )
        {
          values[LsField.ACCESS_TIME.ordinal()] = null;
        }
        else
        {
          accessTime.set(x);
          values[LsField.ACCESS_TIME.ordinal()] = accessTime;
        }
        modifyTime.set(stat.getModificationTime());
        length.set(stat.getLen());
        blockSize.set(stat.getBlockSize());
        replication.set(stat.getReplication());
        path.setCopy(stat.getPath().toString());
        owner.setCopy(stat.getOwner());
        group.setCopy(stat.getGroup());
        permission.setCopy(stat.getPermission().toString());
        return true;
      }
    };
  }

}
