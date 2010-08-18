package com.ibm.jaql.jdbc;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;

import com.ibm.jaql.json.schema.OrSchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.type.JsonBinary;
import com.ibm.jaql.json.type.JsonBool;
import com.ibm.jaql.json.type.JsonDate;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonType;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.expr.function.Function;

public class JaqlSqlType
{
  protected final Schema jtype;
  protected final int stype;
  protected final boolean nullable;
  protected final Class<?> rclass;
  
  
  protected JaqlSqlType(Schema jtype, int stype, boolean nullable, Class<?> rclass)
  {
    this.jtype = jtype;
    this.stype = stype;
    this.nullable = nullable;
    this.rclass = rclass;
  }
  
  public Schema getJsonSchema()
  {
    return jtype;
  }
  
  public int getSqlType()
  {
    return stype;
  }

  public boolean isNullable()
  {
    return nullable;
  }

  public Class<?> getObjectClass()
  {
    return rclass;
  }

  
  public static JaqlSqlType make(Schema jtype, boolean nullable)
  {
    int stype;
    Class<?> rclass;

    nullable = nullable || jtype.is(JsonType.NULL).maybe();
    
    while( true )
    {
      switch( jtype.getSchemaType() )
      {
        // atoms
        case BOOLEAN:
          stype = Types.BOOLEAN;
          rclass = Boolean.class;
          break;
        case LONG:
          stype = Types.BIGINT;
          rclass = Long.class;
          break;
        case DOUBLE:
          stype = Types.DOUBLE;
          rclass = Double.class;
          break;
        case DECFLOAT: 
          stype = Types.DECIMAL;
          rclass = BigDecimal.class;
          break;
        case STRING:
          stype = Types.VARCHAR;
          rclass = String.class;
          break;
        case BINARY:
          stype = Types.VARBINARY;
          rclass = byte[].class;
          break;
        case DATE:
          stype = Types.TIMESTAMP;
          rclass = Date.class;
          break;
        case FUNCTION:
          stype = Types.OTHER;
          rclass = Function.class;
          break;
        case SCHEMATYPE:
          stype = Types.OTHER;
          rclass = Schema.class;
          break;

          // null
        case NULL: 
          stype = Types.NULL;
          rclass = Object.class;
          break;

          // compound types
        case ARRAY:
          stype = Types.ARRAY;
          rclass = Object[].class;
          break;

        case RECORD: 
          stype = Types.OTHER;
          rclass = JsonRecord.class;
          break;

          // union types
        case OR: {
          OrSchema or = (OrSchema)jtype;
          List<Schema> alts = or.get();
          if( alts.size() < 2 )
          {
            throw new IllegalArgumentException("OrSchema should have at least two alternatives!");
          }
          if( alts.size() == 2 )
          {
            if( alts.get(0).is(JsonType.NULL).always() )
            {
              jtype = alts.get(1);
              continue;
            }
            else if( alts.get(1).is(JsonType.NULL).always() )
            {
              jtype = alts.get(0);
              continue;
            }
          }
          stype = Types.OTHER;
          rclass = JsonValue.class;
          break;
        }

        case NON_NULL: 
          stype = Types.OTHER;
          rclass = JsonValue.class;
          break;

          // container for the rest
        case GENERIC:
          stype = Types.OTHER;
          rclass = JsonValue.class;
          break;

        default:
          throw new IllegalArgumentException("unknown schema type:"+jtype);
      }

      return new JaqlSqlType(jtype, stype, nullable, rclass);
    }
  }
  

  public Array getArray(JsonValue value) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("array NYI");
  }
  
  public String getString(JsonValue value) throws SQLException
  {
    return value.toString();
  }

  public BigDecimal getBigDecimal(JsonValue value) throws SQLException
  {
    if( value instanceof JsonNumber )
    {
      return ((JsonNumber)value).decimalValue();
    }
    throw new SQLException("invalid decimal: "+value);
  }

  public boolean getBoolean(JsonValue value) throws SQLException
  {
    if( value instanceof JsonBool )
    {
      return ((JsonBool)value).get();
    }
    throw new SQLException("invalid boolean: "+value);
  }

  public byte getByte(JsonValue value) throws SQLException
  {
    return (byte)getLong(value);
  }

  public byte[] getBytes(JsonValue value) throws SQLException
  {
    if( value instanceof JsonBinary )
    {
      return ((JsonBinary)value).getCopy();
    }
    throw new SQLException("invalid binary: "+value);
  }

  public Date getDate(JsonValue value) throws SQLException
  {
    if( value instanceof JsonDate )
    {
      JsonDate d = (JsonDate)value;
      return new Date(d.get());
    }
    throw new SQLException("invalid time: "+value);
  }

  public double getDouble(JsonValue value) throws SQLException
  {
    if( value instanceof JsonNumber )
    {
      return ((JsonNumber)value).doubleValue();
    }
    throw new SQLException("invalid double: "+value);
  }

  public float getFloat(JsonValue value) throws SQLException
  {
    return (float)getDouble(value);
  }

  public int getInt(JsonValue value) throws SQLException
  {
    return (int)getLong(value);
  }

  public long getLong(JsonValue value) throws SQLException
  {
    if( value instanceof JsonNumber )
    {
      return ((JsonNumber)value).longValue();
    }
    throw new SQLException("invalid long: "+value);
  }

  public short getShort(JsonValue value) throws SQLException
  {
    return (short)getLong(value);
  }

  public Time getTime(JsonValue value) throws SQLException
  {
    if( value instanceof JsonDate )
    {
      JsonDate d = (JsonDate)value;
      return new Time(d.get());
    }
    throw new SQLException("invalid time: "+value);
  }

  public Timestamp getTimestamp(JsonValue value) throws SQLException
  {
    if( value instanceof JsonDate )
    {
      JsonDate d = (JsonDate)value;
      return new Timestamp(d.get());
    }
    throw new SQLException("invalid timestamp: "+value);
  }
  
}
