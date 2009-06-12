package com.ibm.jaql.io.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.UndeclaredThrowableException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import com.ibm.jaql.json.parser.JsonParser;
import com.ibm.jaql.json.parser.ParseException;
import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JBinary;
import com.ibm.jaql.json.type.JDate;
import com.ibm.jaql.json.type.JDecimal;
import com.ibm.jaql.json.type.JLong;
import com.ibm.jaql.json.type.JRecord;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.json.type.JValue;
import com.ibm.jaql.json.type.MemoryJRecord;

public class DB2InputFormat implements InputFormat<Item,Item>
{
  public static final String DRIVER_KEY              = "com.ibm.db2.input.driver";
  public static final String URL_KEY                 = "com.ibm.db2.input.url";
  public static final String PROPERTIES_KEY          = "com.ibm.db2.input.properties"; // json record key/value into JDBC Properties
  public static final String QUERY_KEY               = "com.ibm.db2.input.query";
  public static final String SPLIT_QUERY_KEY         = "com.ibm.db2.input.split.query";
  // public static final String SPLIT_COLUMN_KEY        = "com.ibm.db2.input.split.column";
  // public static final String SPLIT_SAMPLING_RATE_KEY = "com.ibm.db2.input.split.sampling.rate";


  protected Connection conn;


  protected void init(JobConf conf) throws IOException, SQLException
  {
    String url        = conf.get(URL_KEY);
    String jsonRec    = conf.get(PROPERTIES_KEY);
    Class<? extends Driver> driverClass = 
      conf.getClass(DRIVER_KEY, Driver.class).asSubclass(Driver.class);

    Driver driver;
    try 
    {
      driver = driverClass.newInstance();
    }
    catch (Exception e)
    {
      throw new UndeclaredThrowableException(e);// IOException("Error constructing jdbc driver", e);
    }

    Properties props = new Properties();
    
    if( jsonRec != null && ! "".equals(jsonRec))
    {
      try
      {
        JsonParser parser = new JsonParser(new StringReader(jsonRec));
        JRecord jrec = (JRecord)parser.JsonVal().get();
        int n = jrec.arity();
        for( int i = 0 ; i < n ; i++ )
        {
          JString key = jrec.getName(i);
          Item value = jrec.getValue(i);
          props.setProperty(key.toString(), value.toString());
        }
      }
      catch(ParseException pe)
      {
        throw new UndeclaredThrowableException(pe); // IOException("couldn't parse "+PROPERTIES_KEY+" = "+jsonRec, pe);
      }
    }
    
    conn = driver.connect(url, props);
  }
  
  public InputSplit[] getSplits(JobConf conf, int numSplits) throws IOException
  {
    try
    {
      String dataQuery  = conf.get(QUERY_KEY);
      String splitQuery = conf.get(SPLIT_QUERY_KEY);
      // String col        = conf.get(SPLIT_COLUMN_KEY);
      // String rate       = conf.get(SPLIT_SAMPLING_RATE_KEY, "0.0001");

      init(conf);

//      String sample = "";
//      if( ! BigDecimal.ONE.equals(new BigDecimal(rate)) )
//      {
//        sample = " tablesample system(100*decimal('"+rate+"'))";
//      }

      // Make sure that the data query is executable and get the key column type.
      ResultSetMetaData meta = conn.prepareStatement(dataQuery).getMetaData();
      int dataColCount = meta.getColumnCount();
      // int keyType = meta.getColumnType(1);
      // KeyConverter converter = makeKeyConverter(keyType);

      String query = "with T";
      String sep = "(";
      for(int i = 1 ; i <= dataColCount ; i++)
      {
        query += sep + meta.getColumnName(i);
        sep = ",";
      }
      query +=
        ") as ("+ dataQuery + ") "+
        " select * from T ";
      String keycol = meta.getColumnName(1);
      dataQuery = query;
      
      query = 
        "with S(c) as ("+splitQuery+") "+
        "select distinct c "+
        " from S "+
        " where c is not null "+
        " order by c";

      Statement stmt = conn.createStatement();
      final ResultSet rs = stmt.executeQuery(query);

      ArrayList<InputSplit> splits = new ArrayList<InputSplit>();

      if( ! rs.next() )
      {
        splits.add(new JdbcSplit(dataQuery, null, null));
      }
      else
      {
        String prevKey = rs.getString(1);
        // prevKey = converter.convert(prevKey);
        // query = dataQuery + " where "+keycol+" <= "+prevKey;
        query = dataQuery + " where "+keycol+" <= ?";
        splits.add(new JdbcSplit(query, null, prevKey));

        while( rs.next() )
        {
          String key = rs.getString(1);
          // key = converter.convert(key);
          // query = dataQuery + " where "+keycol+" > "+prevKey+" and "+keycol+" <= "+key;
          query = dataQuery + " where "+keycol+" > ? and "+keycol+" <= ?";
          splits.add(new JdbcSplit(query, prevKey, key));
          prevKey = key;
        }

        // query = dataQuery + " where "+keycol+" > "+prevKey;
        query = dataQuery + " where "+keycol+" > ?";
        splits.add(new JdbcSplit(query, prevKey, null));
      }

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
  
//  private static abstract class KeyConverter
//  {
//    abstract String convert(String inKey);
//  }
//  
//  private KeyConverter makeKeyConverter(int columnType) throws SQLException
//  {
//    switch( columnType )
//    {
//      case Types.BIGINT:
//      case Types.INTEGER:
//      case Types.TINYINT:
//      case Types.SMALLINT:
//      case Types.DECIMAL: 
//      case Types.DOUBLE: 
//      case Types.FLOAT: 
//        return new KeyConverter() { 
//          String convert(String inKey) { 
//            return inKey; 
//          }
//        };
//
//      case Types.CHAR:
//      case Types.VARCHAR:
//        return new KeyConverter() { 
//          String convert(String inKey) { 
//            return "'" + inKey.replace("'", "''") + "'"; 
//          }
//        };
//
//      case Types.DATE:
//        return new KeyConverter() { 
//          String convert(String inKey) { 
//            return "date('"+ inKey +"')";
//          }
//        };
//        
//      case Types.TIME:
//        return new KeyConverter() { 
//          String convert(String inKey) { 
//            return "time('"+ inKey +"')";
//          }
//        };
//
//      case Types.TIMESTAMP:
//        return new KeyConverter() { 
//          String convert(String inKey) { 
//            return "timestamp('"+ inKey +"')";
//          }
//        };
//        
//      case Types.BINARY:
//        return new KeyConverter() { 
//          String convert(String inKey) { 
//            return "x'"+ inKey +"'"; // TODO: right?
//          }
//        };
//
//      default:
//        throw new RuntimeException("Unsupported column type: " + columnType);
//    }
//  }
  
  public RecordReader<Item,Item> getRecordReader(InputSplit split, JobConf conf, Reporter reporter)
    throws IOException
  {
    try
    {
      init(conf);
      return new JdbcRecordReader(conn, (JdbcSplit)split);
    }
    catch( SQLException e )
    {
      throw new UndeclaredThrowableException(e); //IOException(e);
    }
  }
  
  protected static class JdbcSplit implements InputSplit
  {
    protected String query;
    protected String lowKey;
    protected String highKey;
    
    public JdbcSplit(String query, String lowKey, String highKey)
    {
      this.query = query;
      this.lowKey = lowKey;
      this.highKey = highKey;
    }
    
    public long getLength() throws IOException
    {
      return 1000000; // no clue...
    }

    public String[] getLocations() throws IOException
    {
      return null; // TODO: DPF affinity
    }

    public void readFields(DataInput in) throws IOException
    {
      query = in.readUTF();
      if( in.readByte() == 1 )
      {
        lowKey = in.readUTF();
      }
      else
      {
        lowKey = null;
      }
      if( in.readByte() == 1 )
      {
        highKey = in.readUTF();
      }
      else
      {
        highKey = null;
      }
    }

    public void write(DataOutput out) throws IOException
    {
      out.writeUTF(query);
      if( lowKey != null )
      {
        out.writeByte(1);
        out.writeUTF(lowKey);
      }
      else
      {
        out.writeByte(0);
      }
      if( highKey != null )
      {
        out.writeByte(1);
        out.writeUTF(highKey);
      }
      else
      {
        out.writeByte(0);
      }
    }
  }

  protected static class JdbcRecordReader implements RecordReader<Item, Item>
  {
    protected final Connection conn;
    protected final PreparedStatement stmt;
    protected final ResultSet resultSet;
    protected final ResultSetMetaData meta;
    protected final int ncols;
    protected final JString[] names;
    protected final JValue[] values;
    protected long numRecs;
    
    public JdbcRecordReader(Connection conn, JdbcSplit split) throws SQLException
    {
      this.conn = conn;
      stmt = conn.prepareStatement(split.query);
      if( split.lowKey != null )
      {
        stmt.setObject(1, split.lowKey);
        if( split.highKey != null )
        {
          stmt.setObject(2, split.highKey);
        }
      }
      else if( split.highKey != null )
      {
        stmt.setObject(1, split.highKey);
      }
      resultSet = stmt.executeQuery();
      meta = resultSet.getMetaData();
      ncols = meta.getColumnCount();
      names = new JString[ncols];
      values = new JValue[ncols];
      
      for(int i = 0 ; i < ncols ; i++)
      {
        names[i] = new JString(meta.getColumnName(i+1).toLowerCase());

        switch( meta.getColumnType(i+1) )
        {
          case Types.BIGINT:
          case Types.INTEGER:
          case Types.TINYINT:
          case Types.SMALLINT:
            values[i] = new JLong();
            break;
          case Types.DECIMAL: 
          case Types.DOUBLE: 
          case Types.FLOAT: 
            values[i] = new JDecimal();
            break;
          case Types.CHAR:
          case Types.VARCHAR:
          case Types.OTHER:   // TODO: Types.XML, when jdbc gets there...
            values[i] = new JString();
            break;
          case Types.DATE:
          case Types.TIME:
          case Types.TIMESTAMP:
            values[i] = new JDate();
            break;
          case Types.BINARY:          
            values[i] = new JBinary();
            break;
          default:
            throw new RuntimeException("Unsupported column type: " + meta.getColumnTypeName(i+1));
        }
      }
    }
    
    public void close() throws IOException
    {
      try
      {
        resultSet.close();
        stmt.close();
        conn.close();
      }
      catch( SQLException e )
      {
        throw new UndeclaredThrowableException(e); // IOException(e);
      }
    }

    public Item createKey()
    {
      return null;
    }

    /**
     * Warning: For efficiency, this class does not support more than one value to be created (no concurrent readers) 
     */
    public Item createValue()
    {
      return new Item(new MemoryJRecord(ncols));
    }

    public long getPos() throws IOException
    {
      return numRecs;
    }

    public float getProgress() throws IOException
    {
      return (float)(numRecs / 1000000.0); // hack
    }

    public boolean next(Item key, Item value) throws IOException
    {
      try
      {
        if( ! resultSet.next() )
        {
          return false;
        }

        MemoryJRecord jrec = (MemoryJRecord)value.get();
        jrec.clear();

        for(int i = 0 ; i < ncols ; i++)
        {
          switch( meta.getColumnType(i+1) )
          {
            case Types.BIGINT:
            case Types.INTEGER:
            case Types.TINYINT:
            case Types.SMALLINT:
              ((JLong)values[i]).value = resultSet.getLong(i+1);
              break;
            case Types.DECIMAL: 
            case Types.DOUBLE: 
            case Types.FLOAT: 
              ((JDecimal)values[i]).value = resultSet.getBigDecimal(i+1);
              break;
            case Types.CHAR:
            case Types.VARCHAR:
              String s = resultSet.getString(i+1);
              if( s != null )
              {
                ((JString)values[i]).set(s);
              }
              break;
            case Types.DATE:
              // TODO: all these need null handling...
              ((JDate)values[i]).millis = resultSet.getDate(i+1).getTime();
              break;
            case Types.TIME:
              ((JDate)values[i]).millis = resultSet.getTime(i+1).getTime();
              break;
            case Types.TIMESTAMP:
              ((JDate)values[i]).millis = resultSet.getTimestamp(i+1).getTime();
              break;
            case Types.BINARY:
              ((JBinary)values[i]).setBytes(resultSet.getBytes(i+1));
              break;
            default:
              throw new RuntimeException("Unsupported column type: " + meta.getColumnTypeName(i+1));
          } // end switch

          if( ! resultSet.wasNull() )
          {
            jrec.add(names[i], values[i]);
          }
        }

        return true;
      }
      catch( SQLException e )
      {
        throw new UndeclaredThrowableException(e); // IOException(e);
      }
    }
  }


  @Deprecated
  public void validateInput(JobConf conf) throws IOException
  {
  }
}
