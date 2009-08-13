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
package com.ibm.jaql.io.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class Db2DpfTableInputFormat extends AbstractDb2InputFormat
{
  public static final String SCHEMA_KEY              = "com.ibm.db2.input.schema";
  public static final String TABLE_KEY               = "com.ibm.db2.input.table";
  public static final String COLUMNS_KEY             = "com.ibm.db2.input.columns";
  public static final String WHERE_KEY               = "com.ibm.db2.input.where";


  public InputSplit[] getSplits(JobConf conf, int numSplits) throws IOException
  {
    try
    {
      init(conf);
      String schema    = conf.get(SCHEMA_KEY);
      String table     = conf.get(TABLE_KEY);
      String columns   = conf.get(COLUMNS_KEY, "*");
      String where     = conf.get(WHERE_KEY, "");

      String schemaTable = "\""+schema+"\".\""+table+"\"";
      String tablePred = "TABSCHEMA='"+schema+"' and TABNAME='"+table+"'";
      
      String keyColQuery =
        "select COLNAME from syscat.columns where "+tablePred+" and PARTKEYSEQ = 1";
      
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery(keyColQuery);
      if( ! rs.next() )
      {
        // TODO: we could revert to primary key partitioning instead of raising an error
        throw new IOException("partitioning key not found for "+schemaTable);
      }
      String keyCol = rs.getString(1);
      rs.close();
      stmt.close();

      String partQuery = 
        " select p.dbpartitionnum "+
        " from syscat.tables t, syscat.tablespaces ts, syscat.dbpartitiongroupdef p "+
        " where t.tbspaceid = ts.tbspaceid and ts.dbpgname = p.dbpgname "+
        "   and t.partition_mode = 'H' and "+
        tablePred;
      
      stmt = conn.createStatement();
      rs = stmt.executeQuery(partQuery);

      ArrayList<InputSplit> splits = new ArrayList<InputSplit>();

      if( ! rs.next() )
      {
        throw new IOException("no partitions found for table \""+schema+"\".\""+table+"\"");
      }
      
      String query = "select "+columns+" from "+schemaTable+
                     " where SYSIBM.DBPARTITIONNUM(\""+keyCol+"\") = CURRENT DBPARTITIONNUM\n";
      if( !where.equals("") )
      {
        query += " and ("+where+")\n";
      }

      do
      {
        int partitionId = rs.getInt(1);
        splits.add(new DpfSplit(query, partitionId));
      }
      while( rs.next() );

      rs.close();
      stmt.close();
      conn.close();
      
      return splits.toArray(new InputSplit[splits.size()]);
    }
    catch( SQLException e )
    {
      throw new UndeclaredThrowableException(e); // IOException(e);
    }
  }

  

  public RecordReader<JsonHolder, JsonHolder> getRecordReader(
      InputSplit split, 
      JobConf conf, 
      Reporter reporter)
    throws IOException
  {
    try
    {
      DpfSplit dpfSplit = (DpfSplit)split;
      Properties props = new Properties();
      props.setProperty("CONNECTNODE", Integer.toString(dpfSplit.partitionId));
      init(conf, props);
      
      Statement s = conn.createStatement();
      ResultSet rs = s.executeQuery("values current dbpartitionnum");
      if( ! rs.next() ) 
      {
        throw new IOException("couldn't get dbpartitionnum");
      }
      int p = rs.getInt(1);
      if( p != dpfSplit.partitionId )
      {
        throw new IOException("didn't connect to the right dbpartitionnum.  Expected "+dpfSplit.partitionId+" got "+p);
      }
      
      PreparedStatement stmt = conn.prepareStatement(dpfSplit.query);
      return new JdbcRecordReader(conn, stmt);
    }
    catch( SQLException e )
    {
      throw new UndeclaredThrowableException(e); //IOException(e);
    }
  }
  
  protected static class DpfSplit implements InputSplit
  {
    protected String query;
    protected int partitionId;
    
    public DpfSplit(String query, int partitionId)
    {
      this.query = query;
      this.partitionId = partitionId;
    }
    
    public long getLength() throws IOException
    {
      return 1000000; // TODO: get partition size from catalog
    }

    public String[] getLocations() throws IOException
    {
      return null; // TODO: DPF affinity
    }

    public void readFields(DataInput in) throws IOException
    {
      query = in.readUTF();
      partitionId = in.readInt();
    }

    public void write(DataOutput out) throws IOException
    {
      out.writeUTF(query);
      out.writeInt(partitionId);
    }
  }

  @Deprecated
  public void validateInput(JobConf conf) throws IOException
  {
  }
}
