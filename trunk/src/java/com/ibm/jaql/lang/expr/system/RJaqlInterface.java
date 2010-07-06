/**
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
package com.ibm.jaql.lang.expr.system; // TODO: find a better home
// package com.ibm.jaql.rjaql;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapred.JobConf;

import com.ibm.jaql.io.converter.AbstractFromDelConverter;
import com.ibm.jaql.io.hadoop.ConfSetter;
import com.ibm.jaql.io.hadoop.converter.FromDelConverter;
import com.ibm.jaql.io.hadoop.FromLinesConverter;
import com.ibm.jaql.io.hadoop.HadoopSerializationDefault;
import com.ibm.jaql.io.hadoop.HadoopSerializationTemp;
import com.ibm.jaql.io.hadoop.JsonHolder;
import com.ibm.jaql.io.hadoop.JsonHolderDefault;
import com.ibm.jaql.io.hadoop.JsonHolderTempKey;
import com.ibm.jaql.io.hadoop.JsonHolderTempValue;
import com.ibm.jaql.io.hadoop.converter.KeyValueImport;
import com.ibm.jaql.json.schema.RecordSchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.schema.SchemaTransformation;
import com.ibm.jaql.json.schema.RecordSchema.Field;
import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonAtom;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonSchema;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonType;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.Jaql;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.del.DelOptionParser;

/**
 * This class provides the interface to drive Jaql from within R.
 * TODO: A list of features that need to be supported.
 * <ul> 
 *  <li>Support for Binary objects to be returned from Jaql to R.</li>
 *  <li>Support for Jaql Date type. </li>
 *  <li>Add more options for the existing interface methods.</li>
 * </ul>
 */
public class RJaqlInterface extends Jaql {

  private static final Log LOG = LogFactory.getLog(RJaqlInterface.class);
  
  public static final String FORMAT_DELIM = "csv";
  public static final String FORMAT_DEFAULT = "default";
  public static final String FORMAT_TEMP = "temp";
  
  private static Pattern splitPattern = Pattern.compile(",");
  
  public RJaqlInterface() {
    super();
    stopOnException(true);
  }
  
  public RJaqlInterface(String input) {
    super(input);
    stopOnException(true);
  }
  
  @Override
  public void setInput (String jaqlQuery) {
    if (jaqlQuery.endsWith(";")) super.setInput(jaqlQuery);
    else super.setInput(jaqlQuery + ";");
  }
  
  /**
   * This method takes a Jaql query and executes it in the present context.
   * The returned value is an array of two elements. The first element is always
   * a JsonLong and is used to interpret the meaning of the next element. The goal
   * of this function is to transfer huge amount of data from Jaql to R either as
   * a table or a vector. As a result, this function will handle only simple data types
   * and will fail for complicated and fancy structures.
   * 
   * <ul>
   * <li> If the query evaluates to an array of records such that it can be formatted as a table,
   * then the first element is set to 2, and the second
   * element is the name of the file where the contents of the table are written in CSV
   * format.</li>
   * <li> If the query evaluates to an array of primitive types, it is formatted as a data
   * frame with a single column such that it can be converted to a vector in R. The value
   * of the first element is set to 3 and the formatted contents are put in a file, whose
   * name is passed as the second argument.</li>
   * <li> If the query evaluates to an array of arrays, this method tries to format it as a
   * data frame where the individual constituents array become a row. Again, only atomic types
   * are allowed. The value of the first element is set to 4, and the formatted contents are
   * put in a file whose name is passed as the second argument. </li>
   * <li> Fails for all other cases.</li>
   * </ul> 
   * 
   * @param input the Jaql query string.
   * @param schemaString the expected schema of the result of the query. If not null, then this
   *        argument overrides the schema inferred by Jaql.
   * @return and array of two items, which are interpreted as described earlier. Returns null
   *        either if the expression does not need evaluation, or if there is some error.
   */
  public JsonArray jaqlTableRowwise (String input, String schemaString) {
    setInput(input);
    Expr expr;
    try {
      expr = prepareNext();
      if( expr == null ) {
        LOG.info("Nothing to evaluate for query: " + input);
        return null;
      }
      JsonIterator iter;
      Schema schema;
      if (schemaString != null) {
        schema = SchemaFactory.parse(schemaString);
      } else {
        schema = expr.getSchema();
      }
      if (schema.is(JsonType.ARRAY, JsonType.NULL).always()) {
        iter = expr.iter(context);
      } else {
        throw new IOException("Serialization Failed: Expression not an array.");
      }
      return serializeIterator(iter, schema, new RUtil.Config());
    } catch (Exception e) {
      LOG.error("Error in evaluation of query: " + input, e);
      return null;
    }
  }
  
  /**
   * This method takes a Jaql query and executes it in the present context.
   * The returned value is record specifying how to interpret the result. 
   * The field named "mode" specifies how to interpret the returned value. The goal
   * of this function is to transfer huge amount of data from Jaql to R either as
   * a table or a vector. As a result, this function will handle only simple data types
   * and will fail for complicated and fancy structures.
   * 
   * <ul>
   * <li> If the query evaluates to an array of records such that it can be formatted as a table,
   * then mode is set to 2, and the "path" field is the directory containing files 1,2...,nColumns
   * which contains the contents of the columns of the data.</li>
   * <li> If the query evaluates to an array of primitive types, it is formatted as a
   * single column such that it can be converted to a vector in R. The value
   * of mode is set to 3 and the formatted contents are put in a file, whose
   * name is passed as the field "path".</li>
   * <li> If the query evaluates to an array of arrays, this method tries to format it as a
   * data frame where the individual constituents array become a row. Again, only atomic types
   * are allowed. The value of mode is set to 4, and the "path" field is the directory containing files 1,2...,nColumns
   * which contains the contents of the columns of the data.</li>
   * <li> Fails for all other cases.</li>
   * </ul> 
   * 
   * @param input the Jaql query string.
   * @param schemaString the expected schema of the result of the query. If not null, then this
   *        argument overrides the schema inferred by Jaql.
   * @return a record containing information on how to interpret the result. Returns null
   *        either if the expression does not need evaluation, or if there is some error.
   */
  public JsonRecord jaqlTable (String input, String schemaString) {
    setInput(input);
    Expr expr;
    try {
      expr = prepareNext();
      if( expr == null ) {
        LOG.info("Nothing to evaluate for query: " + input);
        return null;
      }
      JsonIterator iter;
      Schema schema;
      if (schemaString != null) {
        schema = SchemaFactory.parse(schemaString);
      } else {
        schema = expr.getSchema();
      }
      if (schema.is(JsonType.ARRAY, JsonType.NULL).always()) {
        iter = expr.iter(context);
      } else {
        throw new IOException("Serialization Failed: Expression not an array.");
      }
      return RUtil.serializeIterator(iter, schema, new RUtil.Config());
    } catch (Exception e) {
      LOG.error("Error in evaluation of query: " + input, e);
      return null;
    }
  }
  
  /**
   * This method takes a Jaql query and executes it in the present context.
   * The returned value is an array of two elements. The first element is always
   * a JsonLong and is used to interpret the meaning of the next element. The goal 
   * of this function is to allow passing flexible and fancy Json objects into R.
   * This is geared to flexibility, and not performance. This function can handle data
   * with arbitrary nesting and structure as allowed by Json, and generates R-Code
   * to create lists.
   * 
   * <ul>
   * <li> If the query evaluates to a primitive type, then it is passed directly as the
   * primitive type which is then converted in R into the appropriate R type. In this case,
   * the first value is set to 0, and the second item is the actual value.</li>
   * <li> Otherwise, it generates R code for creating nested lists. The first argument
   * is set to 1 and the 2nd argument is a JsonString that contains the RCode generated.</li>
   * <li> Not expected to fail for any case.</li>
   * </ul> 
   * 
   * @param input the Jaql query string.
   * @return and array of two items, which are interpreted as described earlier. Returns null
   *        either if the expression does not need evaluation, or if there is some error.
   */
  public JsonArray jaqlValue (String input) {
    try {
      setInput(input);
      Expr expr = prepareNext();
      if (expr == null) {
        return null;
      }
      return evaluateAsValue(expr);
    } catch (Exception e) {
      LOG.error("Error in evaluation of query: " + input, e);
      return null;
    }
  }
  
  /**
   * This method provides the functionality of saving simple R objects into HDFS in one of
   * the formats supported by Jaql so that it can be directly read into Jaql.
   * @param localPath
   * @param hdfsPath
   * @param schemaString
   * @param format
   * @param header
   * @param vector
   * @return
   */
  public boolean jaqlSave(String localPath, String hdfsPath, 
      String schemaString, String format, boolean header, boolean vector) {
    if (format.equalsIgnoreCase(FORMAT_DELIM)) {
      LOG.info("Format: " + FORMAT_DELIM + ", saving to HDFS loc: " + hdfsPath);
      return RUtil.saveToHDFS(localPath, hdfsPath);
    }
    try {
      JobConf conf = new JobConf();
      int DEFAULT_BUFFER_SIZE = 64*1024;
      int bufferSize = conf.getInt("io.file.buffer.size", DEFAULT_BUFFER_SIZE);
      BufferedReader reader = new BufferedReader(new FileReader(localPath), bufferSize);
      LongWritable key = new LongWritable(0);
      long count = 0;
      Text value = new Text();
      BufferedJsonRecord options = new BufferedJsonRecord(2);
      BufferedJsonArray headerArray = null;
      if (header) {
        String headerString = reader.readLine();
        String[] headers = splitPattern.split(headerString);
        headerArray = new BufferedJsonArray(headers.length);
        for (int i = 0; i < headers.length; i++) {
          headerArray.set(i, new JsonString(StringUtils.strip(headers[i], "\"")));
        }
        count++;
      }
      
      Schema schema = null;
      if (schemaString != null) {
        schema = SchemaFactory.parse(schemaString);
      }
      
      if (headerArray != null) {
        RecordSchema recordSchema = (RecordSchema)schema;
       
        // construct new matching schema
        List<Field> fields = new LinkedList<Field>();
        for (JsonValue fieldName : headerArray) {
          Field field;
          if (recordSchema == null) {
            field = new Field((JsonString)fieldName, SchemaFactory.stringSchema(), false);
          }
          else
          {
            field = recordSchema.getField((JsonString)fieldName);
            if (field == null) throw new NullPointerException("header field not in schema: " + fieldName);
            // FIXME: schema fields that are not in the header are currently consider OK
          }
          fields.add(field);          
        }
        
        // and set it
        schema = new RecordSchema(fields, null); 
      }
      if (schema != null)
        options.add(DelOptionParser.SCHEMA_NAME, new JsonSchema(schema));
      KeyValueImport<LongWritable, Text> converter = null;
      if (vector) {
        converter = new FromLinesConverter();
      } else {
        converter = new FromDelConverter();
      }
      LOG.info("Initializing Converter with options: " + options);
      converter.init(options);
      Schema tmpSchema = converter.getSchema();
      tmpSchema = SchemaTransformation.removeNullability(tmpSchema);
      if (!tmpSchema.is(JsonType.ARRAY, JsonType.RECORD, JsonType.BOOLEAN, JsonType.DECFLOAT, 
          JsonType.DOUBLE, JsonType.LONG, JsonType.STRING).always()) {
        throw new IOException ("Unrecognized schema type: " + schema.getSchemaType());
      }
      JsonValue outValue = converter.createTarget();
      JsonHolder outKeyHolder;
      JsonHolder outValueHolder;
      if (format.equalsIgnoreCase(FORMAT_DEFAULT)) {
        HadoopSerializationDefault.register(conf);
        outKeyHolder = new JsonHolderDefault();
        outValueHolder = new JsonHolderDefault(outValue);
        LOG.info("Registered serializer for Default format.");
      } else if (format.equalsIgnoreCase(FORMAT_TEMP)) {
        // TODO: There should be a better way of doing this. HadoopSerializationTemp
        // now does it in an ugly way.
        BufferedJsonRecord tmpOptions = new BufferedJsonRecord();
        BufferedJsonRecord outOptions = new BufferedJsonRecord();
        outOptions.add(new JsonString("schema"), new JsonSchema(schema));
        tmpOptions.add(new JsonString("options"), outOptions);
        conf.set(ConfSetter.CONFOUTOPTIONS_NAME, tmpOptions.toString());
        HadoopSerializationTemp.register(conf);
        outKeyHolder = new JsonHolderTempKey(null);
        outValueHolder = new JsonHolderTempValue();
        LOG.info("Registered serializer for HadoopTemp format.");
      } else {
        throw new IOException ("Unrecognized serialization format requested: " + format);
      }
      FileSystem fs = FileSystem.get(conf);
      Path outputPath = new Path(hdfsPath);
      Writer writer = SequenceFile.createWriter(fs, conf, outputPath, 
          outKeyHolder.getClass(), outValueHolder.getClass());
      String line;
      while ((line = reader.readLine()) != null) {
        key.set(count++);
        value.set(line);
        outValue = converter.convert(key, value, outValue);
        outValueHolder.value = outValue;
        writer.append(outKeyHolder, outValueHolder);
      }
      LOG.info("Transferred " + count + " line(s).");
      reader.close();
      writer.close();
    } catch (IOException e) {
      LOG.info("Error in saving object." , e);
      return false;
    }
    return true;
  }
  
  private JsonArray evaluateAsValue(Expr expr) throws Exception {
    JsonArray returnVal = null;
    JsonValue[] returnValues = new JsonValue[2];
    JsonValue val = expr.eval(context);
    if (val instanceof JsonRecord) {
      returnValues[0] = new JsonLong(1);
      returnValues[1] = new JsonString(RUtil.serializeRecord((JsonRecord)val));
    } else if (val instanceof JsonArray) {
      returnValues[0] = new JsonLong(1);
      returnValues[1] = new JsonString(RUtil.serializeArray((JsonArray)val));
    } else {
      returnValues[0] = new JsonLong(0);
      returnValues[1] = val;
    }
    returnVal = new BufferedJsonArray(returnValues, false);
    return returnVal;
  }
  
  private JsonArray serializeIteratorAsArray(JsonIterator iter, 
      RUtil.Config config, String file) throws IOException {
    PrintWriter writer = new PrintWriter(new BufferedWriter(
        new FileWriter(file), 4*1024));
    boolean firstRow = true;
    JsonType expect = null;
    long count = 0L;
    for (JsonValue cur : iter) {
      String line;
      if (cur instanceof JsonAtom) {
        if (firstRow) {
          firstRow = false;
          expect = cur.getType();
        }
        JsonType obtained = cur.getType();
        if (config.strict) {
          if ((expect.isNumber() && !obtained.isNumber()) 
              || (!expect.isNumber() && !expect.equals(obtained))) {
            RUtil.doCleanup(writer, file);
            throw new IOException("Serialization failed: At row index " + (count+1)
                + ", cannot serialize " + obtained + ", expecting " + expect);
          }
          line = RUtil.convertToRString(cur);
        } else {
          if ((expect.isNumber() && !obtained.isNumber()) 
              || (!expect.isNumber() && !expect.equals(obtained))) {
            line = RUtil.rNullString;
          } else line = RUtil.convertToRString(cur);
        }
      } else {
        if (config.strict) {
          RUtil.doCleanup(writer, file);
          throw new IOException("Serialization failed: Array is a mix of JsonAtom " +
              "and other JsonValue instances. Found: " + cur.getClass().getCanonicalName());
        } else line = RUtil.rNullString;
      }
      count++;
      writer.println(line.toString());
    }
    LOG.info(count + " entries written to file: " + file);
    writer.close();
    JsonValue[] values = new JsonValue[2];
    values[0] = new JsonLong(3);
    values[1] = new JsonString(file);
    JsonArray returnVal = new BufferedJsonArray(values, false);
    return returnVal;
  }
  
  private JsonArray serializeIteratorAsRecords(JsonIterator iter, 
      RecordSchema recordSchema, RUtil.Config config) throws IOException {
    String filename = RUtil.getTempFileName();
    PrintWriter writer = new PrintWriter(new BufferedWriter(
        new FileWriter(filename), 4*1024));
    boolean firstRow = false;
    if (config.inferSchema || config.strict) firstRow = true;
    JsonString[] fieldNames = new JsonString[0];
    List<JsonString> names = new ArrayList<JsonString>();
    Map<JsonString, JsonType> typeMap = new HashMap<JsonString, JsonType>();
    StringBuilder header = new StringBuilder();
    for (Field field : recordSchema.getFieldsByName()) {
      names.add(field.getName());
      if (header.length() != 0) header.append(RUtil.fieldSeparator);
      header.append(field.getName().toString());
    }
    if (!firstRow) {
      fieldNames = names.toArray(fieldNames);
      writer.println(header.toString());
      LOG.info("Header: " + header.toString());
    }
    long count = 0L;
    for (JsonValue cur : iter) {
      if (cur instanceof JsonRecord) {
        JsonRecord curRecord = (JsonRecord) cur;
        if (firstRow) {
          firstRow = false;
          for (Entry<JsonString, JsonValue> entry : curRecord) {
            JsonString fieldName = entry.getKey().getImmutableCopy();
            if (config.inferSchema && !names.contains(fieldName)) {
              names.add(fieldName);
              if (header.length() != 0) header.append(RUtil.fieldSeparator);
              header.append(entry.getKey().toString());
            }
            if (entry.getValue() != null)
              typeMap.put(fieldName, entry.getValue().getType());
            //LOG.info("Field: " + fieldName + ", Type: " + entry.getValue().getType());
          }
          fieldNames = names.toArray(fieldNames);
          writer.println(header.toString());
          LOG.info("Header: " + header.toString());
        }
        StringBuilder line = new StringBuilder();
        for (JsonString field : fieldNames) {
          JsonValue value = curRecord.get(field, null);
          if (line.length() != 0) line.append(RUtil.fieldSeparator);
          if (value == null) {
            line.append(RUtil.rNullString);
          } else if (value instanceof JsonAtom) {
            JsonType expect = typeMap.get(field);
            JsonType obtained = value.getType();
            if (expect == null) {
              typeMap.put(field, obtained);
              expect = obtained;
            }
            if (config.strict) {
              if ((expect.isNumber() && !obtained.isNumber()) 
                  || (!expect.isNumber() && !expect.equals(obtained))) {
                RUtil.doCleanup(writer, filename);
                throw new IOException("Serialization failed: At row index " + (count+1)
                    + ", cannot serialize " + obtained + ", expecting " + expect);
              }
              line.append(RUtil.convertToRString(value));
            } else {
              if ((expect.isNumber() && !obtained.isNumber()) 
                  || (!expect.isNumber() && !expect.equals(obtained))) {
                line.append(RUtil.rNullString);
              } else line.append(RUtil.convertToRString(value));
            }
          } else {
            if (config.strict) {
              RUtil.doCleanup(writer, filename);
              throw new IOException("Serialization failed: Cannot handle non-atomic types.");
            } else {
              line.append(RUtil.rNullString);
            }
          }
        }
        writer.println(line.toString());
        count++;
      } else {
        RUtil.doCleanup(writer, filename);
        throw new IOException("Serialization failed: Array is a mix of JsonRecord " +
            "and other JsonValue instances.Found: " + cur.getClass().getCanonicalName());
      }
    }
    writer.close();
    LOG.info(count + " record(s) written to file " + filename);
    JsonValue[] values = new JsonValue[2];
    values[0] = new JsonLong(2);
    values[1] = new JsonString(filename);
    JsonArray returnVal = new BufferedJsonArray(values, false);
    return returnVal;
  }
  
  private JsonArray serializeIteratorArrayAsFrame (JsonIterator iter, 
      RUtil.Config config) throws IOException {
    String filename = RUtil.getTempFileName();
    PrintWriter writer = new PrintWriter(new BufferedWriter(
        new FileWriter(filename), 4*1024));
    boolean firstRow = true;
    Map<Integer, JsonType> fieldTypes = new HashMap<Integer, JsonType>();
    long count = 0L;
    JsonType expect;
    int expectedLength = 0;
    int index;
    for (JsonValue val : iter) {
      if (val instanceof JsonArray) {
        StringBuffer line = new StringBuffer();
        JsonArray arr = (JsonArray) val;
        Iterator<JsonValue> arrayIter = arr.iterator();
        JsonValue cur;
        for (index = 0; arrayIter.hasNext(); index++) {
          if (line.length() > 0) line.append(RUtil.fieldSeparator);
          cur = arrayIter.next();
          if (cur == null) {
            line.append(RUtil.rNullString);
          } else if (cur instanceof JsonAtom) {
            JsonType obtained = cur.getType();
            if (firstRow) {
              fieldTypes.put(index, obtained);
            }
            expect = fieldTypes.get(index);
            if (expect == null) {
              fieldTypes.put(index, obtained);
              expect = fieldTypes.get(index);
            }
            if (config.strict) {
              if ((expect.isNumber() && !obtained.isNumber()) 
                  || (!expect.isNumber() && !expect.equals(obtained))) {
                RUtil.doCleanup(writer, filename);
                throw new IOException("Serialization failed: At row index " + (count+1)
                    + ", cannot serialize " + obtained + ", expecting " + expect);
              }
              line.append(RUtil.convertToRString(cur));
            } else {
              if ((expect.isNumber() && !obtained.isNumber()) 
                  || (!expect.isNumber() && !expect.equals(obtained))) {
                line.append(RUtil.rNullString);
              } else line.append(RUtil.convertToRString(cur));
            }
          } else {
            if (config.strict) {
              RUtil.doCleanup(writer, filename);
              throw new IOException("Serialization failed: Array is a mix of JsonAtom " +
                  "and other JsonValue instances. Found: " + cur.getClass().getCanonicalName());
            } else line.append(RUtil.rNullString);
          }
        }
        count++;
        writer.println(line.toString());
        if (firstRow) expectedLength = index;
        else if (expectedLength != index) {
          RUtil.doCleanup(writer, filename);
          throw new IOException("Serialization failed: Found rows of varying " +
                "length. Expecting: " + expectedLength + ", found: " + index);
        }
        firstRow = false;
      } else {
        RUtil.doCleanup(writer, filename);
        throw new IOException("Serialization failed: Found a non array type: " + 
            val.getClass().getCanonicalName());
      }
    }
    writer.close();
    LOG.info(count + " row(s) written to file " + filename);
    JsonValue[] values = new JsonValue[2];
    values[0] = new JsonLong(4);
    values[1] = new JsonString(filename);
    JsonArray returnVal = new BufferedJsonArray(values, false);
    return returnVal;
  }
  
  private JsonArray serializeIterator(JsonIterator iter, 
      Schema schema, RUtil.Config config) throws IOException {
    schema = SchemaTransformation.restrictToArray(schema);
    if (schema == null) {
      throw new IOException("Serialization failed: Expression is not an array.");
    }
    Schema elements = SchemaTransformation.compact(schema.elements());
    elements = SchemaTransformation.removeNullability(elements);
    switch (elements.getSchemaType()) {
      case RECORD:
        return serializeIteratorAsRecords(iter, (RecordSchema) elements, config);
      case ARRAY:
        return serializeIteratorArrayAsFrame(iter, config);
      case BOOLEAN:
      case LONG:
      case DOUBLE:
      case DECFLOAT:
      case STRING:
        return serializeIteratorAsArray(iter, config, RUtil.getTempFileName());  
      default: 
        throw new IOException ("Serialization failed: Unsupported schema type: " + schema);
    }
  }
}
