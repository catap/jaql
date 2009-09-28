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
package com.ibm.jaql.lang.expr.system;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.ibm.jaql.json.schema.RecordSchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaTransformation;
import com.ibm.jaql.json.schema.RecordSchema.Field;
import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonAtom;
import com.ibm.jaql.json.type.JsonBinary;
import com.ibm.jaql.json.type.JsonBool;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonType;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;

/**
 * This class provides some utilities to interface jaql with R.
 */
public class RUtil {

  /**
   * This class is used as a configuration for augmentation of functionality in future.
   */
  public static class Config {
    public boolean strict;
    public boolean inferSchema;
    
    public Config() {
      strict = true;
      inferSchema = true;
    }
  }
  
  private static final Log LOG = LogFactory.getLog(RUtil.class);
  private static final AtomicLong uniqueId = 
    new AtomicLong(System.currentTimeMillis());
  private static final String extension = ".csv";
  public static final String fileSeparator = System.getProperty("file.separator");
  public static final String rNullString = "NA";
  public static final String fieldSeparator = ",";
  
  private static String tmpDirPath;
  
  // -------------------------------------------------------------------------
  // The numbers used as mode in the descriptors created after serialization.
  // -------------------------------------------------------------------------
  
  public static final int MODE_ATOMIC = 1;
  public static final int MODE_ARRAY_OF_RECORDS = 2;
  public static final int MODE_ARRAY_OF_ATOMICS = 3;
  public static final int MODE_ARRAY_OF_ARRAYS = 4;
  public static final int MODE_ARRAY_OF_BINARIES = 5;
  
  static {
    tmpDirPath = System.getProperty("java.io.tmpdir");
    if (!tmpDirPath.endsWith(fileSeparator)) tmpDirPath += fileSeparator;
    tmpDirPath += "rJaql" + fileSeparator;
    File tmpDir = new File(tmpDirPath);
    if (!tmpDir.exists()) {
      tmpDir.mkdirs();
    } else if (!tmpDir.isDirectory()) {
      tmpDir.delete();
      tmpDir.mkdirs();
    }
  }

  public static JsonString convertJsonTypeToR (JsonType type) {
    switch (type) {
      case LONG:
      case DOUBLE:
      case DECFLOAT:
        return new JsonString("numeric()");
      case BOOLEAN:
        return new JsonString("logical()");
      case STRING:
        return new JsonString("character()");
      default: throw new IllegalArgumentException("Cannot convert " + type + 
          " to a R type");
    }
  }
  
  public static String convertToRString (JsonValue val) {
    if (val == null) {
      return rNullString;
    } else if (val instanceof JsonNumber) {
      if (val instanceof JsonLong) return String.valueOf(((JsonLong)val).longValue());
      return String.valueOf(((JsonNumber)val).doubleValue());
    } else if (val instanceof JsonBool) {
      return String.valueOf(((JsonBool)val).get()).toUpperCase();
    } else if (val instanceof JsonString) {
      String str = val.toString();
      str = StringEscapeUtils.escapeJava(str);
      str = StringEscapeUtils.escapeJava(str);
      str = "\"" + str + "\"";
      return str;
    } else if (val instanceof JsonArray) {
      return serializeArray((JsonArray)val);
    } else if (val instanceof JsonRecord) {
      return serializeRecord((JsonRecord)val);
    } else {
      throw new IllegalArgumentException("Invalid Json type " + val.getType() + 
          " for conversion in convertToRString.");
    }
  }
  
  public static String convertToRString (JsonValue val, boolean escape) {
    if (val == null) {
      return rNullString;
    } else if (val instanceof JsonNumber) {
      if (val instanceof JsonLong) return String.valueOf(((JsonLong)val).longValue());
      return String.valueOf(((JsonNumber)val).doubleValue());
    } else if (val instanceof JsonBool) {
      return String.valueOf(((JsonBool)val).get()).toUpperCase();
    } else if (val instanceof JsonString) {
      String str = val.toString();
      if (escape) {
        str = StringEscapeUtils.escapeJava(str);
        str = StringEscapeUtils.escapeJava(str);
      }
      str = "\"" + str + "\"";
      return str;
    } else if (val instanceof JsonArray) {
      return serializeArray((JsonArray)val, escape);
    } else if (val instanceof JsonRecord) {
      return serializeRecord((JsonRecord)val, escape);
    } else {
      throw new IllegalArgumentException("Invalid Json type " + val.getType() + 
          " for conversion in convertToRString.");
    }
  }
  
  public static void doCleanup (java.io.Writer writer, 
      String file) throws IOException {
    writer.close();
    File path = new File(file);
    if (path.exists()) path.delete();
  }
  
  public static void doCleanup (java.io.Writer[] writers, 
      String dirname) throws IOException {
    for (int i = 0; i < writers.length; i++) {
      writers[i].close();
      String filename = dirname + fileSeparator + (i+1);
      File path = new File(filename);
      if (path.exists()) path.delete();
    }
    File dir = new File(dirname);
    if(!dir.delete()) LOG.warn("Could not delete directory " + dirname);
  }
  
  /**
   * Return a string which can be used to create a directory in the
   * temporary workspace of whatever is the meaning of the temp space in the
   * present execution environment. Under linux it will return a file name 
   * under the /tmp directory, when run inside map-reduce it will return
   * a file in the temp working space of the task.
   * 
   * <br>
   * This call itself does not create the dir or ensure that the dir is deleted
   * on completion. It is the caller's responsibility to create the directory
   * and make sure that it is deleted on exit.
   * 
   * @return
   */
  public static String getTempDirName() {
    return tmpDirPath + uniqueId.incrementAndGet();
  }
  
  /**
   * This function returns a filename which is set to a file in the
   * temporary workspace of whatever is the meaning of the temp space in the
   * present execution environment. Under linux it will return a file name 
   * under the /tmp directory, when run inside map-reduce it will return
   * a file in the temp working space of the task.
   * 
   * <br>
   * This call itself does not create a file or ensure that the file is deleted
   * on completion. It is the caller's responsibility to create the file
   * and make sure that it is deleted on exit.
   *   
   * @return a string representing a path to a temp file.
   */
  public static String getTempFileName() {
    return tmpDirPath + uniqueId.incrementAndGet() + extension;
  }
  
  /**
   * Function that puts a local file into HDFS.
   * @param localPath
   * @param hdfsPath
   * @return
   */
  public static boolean saveToHDFS(String localPath, String hdfsPath) {
    try {
      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.get(conf);
      int bufferSize = 4*1024;
      byte[] buffer = new byte[bufferSize];
      InputStream input = new BufferedInputStream(
          new FileInputStream(localPath), bufferSize);
      
      Path outputPath = new Path(hdfsPath);
      if (fs.exists(outputPath)) {
        if (!fs.isFile(outputPath)) {
          throw new IOException("Output path is a directory that already exists.");
        }
        LOG.info("Output path" + outputPath + 
            " already exists. Overwriting it.");
      }
      FSDataOutputStream output = fs.create(outputPath, true);
      
      int numBytesRead;
      while ((numBytesRead = input.read(buffer)) > 0) {
        output.write(buffer, 0, numBytesRead);
      }
      input.close();
      output.close();
      return true;
    } catch (IOException e) {
      LOG.info("Error in writing file to HDFS." , e);
      return false;
    }
  }
  
  public static String serializeArray(JsonArray array) {
    StringBuilder contents = new StringBuilder();
    contents.append("list(");
    boolean first = true;
    for (JsonValue value : array) {
      if (!first) {
        contents.append(",");
      }
      contents.append(convertToRString(value));
      first = false;
    }
    contents.append(")");
    return contents.toString();
  }
  
  public static String serializeArray(JsonArray array, boolean escape) {
    StringBuilder contents = new StringBuilder();
    contents.append("list(");
    boolean first = true;
    for (JsonValue value : array) {
      if (!first) {
        contents.append(",");
      }
      contents.append(convertToRString(value,escape));
      first = false;
    }
    contents.append(")");
    return contents.toString();
  }
  
  public static JsonRecord serializeIterator(JsonIterator iter, 
      Schema schema, Config config) throws IOException {
    schema = SchemaTransformation.restrictToArray(schema);
    if (schema == null) {
      throw new IOException("Serialization failed: Expression is not an array.");
    }
    Schema elements = SchemaTransformation.compact(schema.elements());
    Schema originalElemSchema = elements;
    elements = SchemaTransformation.removeNullability(elements);
    switch (elements.getSchemaType()) {
      case RECORD:
        return serializeIteratorAsRecords(iter, 
            (RecordSchema) elements, config, getTempDirName());
      case ARRAY:
        return serializeIteratorArrayAsFrame(iter, config, 
            getTempDirName());
      case BOOLEAN:
      case LONG:
      case DOUBLE:
      case DECFLOAT:
      case STRING:
        return serializeIteratorAsArray(iter, 
            config, originalElemSchema, getTempFileName());
      case BINARY:
        return serializeIteratorAsBinary(iter, getTempDirName());
      default: 
        throw new IOException ("Serialization failed: Unsupported schema type: " + 
            schema);
    }
  }
  
  public static JsonRecord serializeIteratorAsBinary(JsonIterator iter, 
      String dirname) throws IOException {
    if (!(new File(dirname)).mkdir()) {
      throw new IOException("Failed to create directory " + dirname + " for serialization.");
    }
    int count = 0;
    for (JsonValue value : iter) {
      JsonBinary bin = (JsonBinary)value;
      count++;
      String filename = dirname + fileSeparator + count;
      FileOutputStream output = new FileOutputStream(filename);
      output.write(bin.getCopy());
      output.close();
    }
    LOG.info(count + " binaries written to directory: " + dirname);
    BufferedJsonRecord returnVal = new BufferedJsonRecord();
    returnVal.add(new JsonString("mode"), new JsonLong(MODE_ARRAY_OF_BINARIES));
    returnVal.add(new JsonString("path"), new JsonString(dirname));
    returnVal.add(new JsonString("nfiles"), new JsonLong(count));
    return returnVal;
  }
  
  public static JsonRecord serializeIteratorArrayAsFrame (JsonIterator iter, 
      Config config, String dirname) throws IOException {
    if (!(new File(dirname)).mkdir()) {
      throw new IOException("Failed to create directory " + dirname + " for serialization.");
    }
    PrintWriter[] writers = null;
    boolean firstRow = true;
    Map<Integer, JsonType> fieldTypes = new HashMap<Integer, JsonType>();
    long count = 0L;
    JsonType expect;
    int expectedLength = 0;
    int index;
    for (JsonValue val : iter) {
      if (val instanceof JsonArray) {
        String line;
        JsonArray arr = (JsonArray) val;
        if (firstRow) {
          int length = (int) arr.count();
          writers = new PrintWriter[length];
          for (int i = 1; i <= writers.length; i++) {
            String filename = dirname + fileSeparator + i;
            writers[i-1] = new PrintWriter(new BufferedWriter(
                new FileWriter(filename), 4*1024));
          }
        }
        Iterator<JsonValue> arrayIter = arr.iterator();
        JsonValue cur;
        for (index = 0; arrayIter.hasNext(); index++) {
          cur = arrayIter.next();
          if (cur == null) {
            line = rNullString;
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
                doCleanup(writers, dirname);
                throw new IOException("Serialization failed: At row index " + (count+1)
                    + ", cannot serialize " + obtained + ", expecting " + expect);
              }
              line = convertToRString(cur);
            } else {
              if ((expect.isNumber() && !obtained.isNumber()) 
                  || (!expect.isNumber() && !expect.equals(obtained))) {
                line = rNullString;
              } else line = convertToRString(cur);
            }
          } else {
            if (config.strict) {
              doCleanup(writers, dirname);
              throw new IOException("Serialization failed: Array is a mix of JsonAtom " +
                  "and other JsonValue instances. Found: " + 
                  cur.getClass().getCanonicalName());
            } else line = rNullString;
          }
          writers[index].println(line.toString());
        }
        count++;
        if (firstRow) expectedLength = index;
        else if (expectedLength != index) {
          doCleanup(writers, dirname);
          throw new IOException("Serialization failed: Found rows of varying " +
              "length. Expecting: " + expectedLength + ", found: " + index);
        }
        firstRow = false;
      } else {
        doCleanup(writers, dirname);
        throw new IOException("Serialization failed: Found a non array type: " + 
            val.getClass().getCanonicalName());
      }
    }
    for (int i = 0; i < writers.length; i++) {
      writers[i].close();
    }
    LOG.info(count + " row(s) written to file(s) in directory " + dirname);
    BufferedJsonRecord returnVal = new BufferedJsonRecord();
    returnVal.add(new JsonString("mode"), new JsonLong(MODE_ARRAY_OF_ARRAYS));
    returnVal.add(new JsonString("path"), new JsonString(dirname));
    BufferedJsonArray types = new BufferedJsonArray();
    for (int i = 0 ; i < expectedLength; i++) {
      JsonType type = fieldTypes.get(new Integer(i));
      types.add(convertJsonTypeToR(type));
    }
    returnVal.add(new JsonString("type"), new JsonString(serializeArray(types)));
    returnVal.add(new JsonString("ncols"), new JsonLong(expectedLength));
    return returnVal;
  }
  
  public static JsonRecord serializeIteratorAsArray (JsonIterator iter, 
      Config config, Schema schema, String file) throws IOException {
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
            doCleanup(writer, file);
            throw new IOException("Serialization failed: At row index " + (count+1)
                + ", cannot serialize " + obtained + ", expecting " + expect);
          }
          line = convertToRString(cur);
        } else {
          if ((expect.isNumber() && !obtained.isNumber()) 
              || (!expect.isNumber() && !expect.equals(obtained))) {
            line = rNullString;
          } else line = convertToRString(cur);
        }
      } else {
        if (config.strict) {
          if (cur == null && schema.is(JsonType.NULL).maybe()) {
            line = rNullString;
          } else {
            doCleanup(writer, file);
            throw new IOException("Serialization failed: Array is a mix of JsonAtom " +
                "and other JsonValue instances. Found: " + cur.getClass().getCanonicalName());
          }
        } else line = rNullString;
      }
      count++;
      writer.println(line.toString());
    }
    LOG.info(count + " entries written to file: " + file);
    writer.close();
    BufferedJsonRecord returnVal = new BufferedJsonRecord();
    returnVal.add(new JsonString("mode"), new JsonLong(MODE_ARRAY_OF_ATOMICS));
    returnVal.add(new JsonString("path"), new JsonString(file));
    returnVal.add(new JsonString("type"), new JsonString(convertJsonTypeToR(expect)));
    return returnVal;
  }
  
  public static JsonRecord serializeIteratorAsRecords (JsonIterator iter, 
      RecordSchema recordSchema, Config config, 
      String dirname) throws IOException {
    if (!(new File(dirname)).mkdir()) {
      throw new IOException("Failed to create directory " + dirname + " for serialization.");
    }
    PrintWriter[] writers = null;
    boolean firstRow = false;
    if (config.inferSchema || config.strict) firstRow = true;
    JsonString[] fieldNames = new JsonString[0];
    List<JsonString> names = new ArrayList<JsonString>();
    Map<JsonString, JsonType> typeMap = new HashMap<JsonString, JsonType>();
    //StringBuilder header = new StringBuilder();
    for (Field field : recordSchema.getFields()) {
      names.add(field.getName());
      //if (header.length() != 0) header.append(fieldSeparator);
      //header.append(field.getName().toString());
    }
    if (!firstRow) {
      fieldNames = names.toArray(fieldNames);
      writers = new PrintWriter[fieldNames.length];
      for (int i = 1; i <= writers.length; i++) {
        String filename = dirname + fileSeparator + i;
        writers[i-1] = new PrintWriter(new BufferedWriter(new FileWriter(filename), 4*1024));
      }
      //LOG.info("Header: " + header.toString());
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
              //if (header.length() != 0) header.append(fieldSeparator);
              //header.append(entry.getKey().toString());
            }
            if (entry.getValue() != null)
              typeMap.put(fieldName, entry.getValue().getType());
            //LOG.info("Field: " + fieldName + ", Type: " + entry.getValue().getType());
          }
          fieldNames = names.toArray(fieldNames);
          writers = new PrintWriter[fieldNames.length];
          for (int i = 1; i <= writers.length; i++) {
            String filename = dirname + fileSeparator + i;
            writers[i-1] = new PrintWriter(new BufferedWriter(
                new FileWriter(filename), 4*1024));
          }
          //LOG.debug("Header: " + header.toString());
        }
        for (int i = 0; i < fieldNames.length; i++) {
          JsonString field = fieldNames[i];
          JsonValue value = curRecord.get(field, null);
          if (value == null) {
            writers[i].println(rNullString);
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
                doCleanup(writers, dirname);
                throw new IOException("Serialization failed: At row index " + (count+1)
                    + ", cannot serialize " + obtained + ", expecting " + expect);
              }
              writers[i].println(convertToRString(value));
            } else {
              if ((expect.isNumber() && !obtained.isNumber()) 
                  || (!expect.isNumber() && !expect.equals(obtained))) {
                writers[i].println(rNullString);
              } else writers[i].println(convertToRString(value));
            }
          } else {
            if (config.strict) {
              doCleanup(writers, dirname);
              throw new IOException("Serialization failed: Cannot handle non-atomic types.");
            } else {
              writers[i].println(rNullString);
            }
          }
        }
        count++;
      } else {
        doCleanup(writers, dirname);
        throw new IOException("Serialization failed: Array is a mix of JsonRecord " +
            "and other JsonValue instances.Found: " + cur.getClass().getCanonicalName());
      }
    }
    for (int i = 0; i < writers.length; i++) {
      writers[i].close();
    }
    LOG.info(count + " record(s) written to files in directory " + dirname);
    BufferedJsonRecord returnVal = new BufferedJsonRecord();
    returnVal.add(new JsonString("mode"), new JsonLong(MODE_ARRAY_OF_RECORDS));
    returnVal.add(new JsonString("path"), new JsonString(dirname));
    BufferedJsonArray types = new BufferedJsonArray();
    for (JsonString field : fieldNames) {
      JsonType type = typeMap.get(field);
      if (type == null) {
        type = JsonType.LONG;
      }
      types.add(convertJsonTypeToR(type));
    }
    returnVal.add(new JsonString("type"), new JsonString(serializeArray(types)));
    returnVal.add(new JsonString("name"), new BufferedJsonArray(fieldNames, false));
    return returnVal;
  }
  
  public static String serializeRecord(JsonRecord rec) {
    StringBuilder contents = new StringBuilder();
    contents.append("list(");
    boolean first = true;
    for (Entry<JsonString, JsonValue> entry : rec) {
      if (!first) {
        contents.append(",");
      }
      contents.append(entry.getKey().toString());
      contents.append("=");
      contents.append(convertToRString(entry.getValue()));
      first = false;
    }
    contents.append(")");
    return contents.toString();
  }
  
  public static String serializeRecord(JsonRecord rec, boolean escape) {
    StringBuilder contents = new StringBuilder();
    contents.append("list(");
    boolean first = true;
    for (Entry<JsonString, JsonValue> entry : rec) {
      if (!first) {
        contents.append(",");
      }
      contents.append(entry.getKey().toString());
      contents.append("=");
      contents.append(convertToRString(entry.getValue(),escape));
      first = false;
    }
    contents.append(")");
    return contents.toString();
  }
}
