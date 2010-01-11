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
package com.ibm.jaql;

import java.io.File;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import com.ibm.jaql.json.parser.JsonParser;
import com.ibm.jaql.json.type.FixedJArray;
import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JArray;
import com.ibm.jaql.json.type.JRecord;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.json.type.JValue;
import com.ibm.jaql.json.type.MemoryJRecord;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.string.SerializeFn;
import com.ibm.jaql.lang.util.JaqlUtil;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import org.junit.Test;

public class Testi18n {
  
  private static final String GOOD_ENC  = "UTF-8";
  private static final String BAD_ENC   = "cp1252";
  
  public Testi18n() { }
  
  public abstract class TypeTester {
    
    private String sampleString;
    private Item testData;
    private String testString;
    
    public TypeTester() { }
    
    void evaluate(String sampleFile, String testFile) throws Exception {
      sampleString = getSampleData(sampleFile);
      Object sampleTest = hideSampleData(sampleString);
      writeTestData(sampleTest, testFile);
      testData = readTestData(testFile);
      testString = extractTestString(testData);
    }
    
    void expectMatch() throws Exception {
      assertTrue("No match: " + sampleString + " != " + testString, sampleString.equals(testString));
    }
    
    void expectNoMatch() throws Exception {
      assertFalse("Match: " + sampleString + " == " + testString, sampleString.equals(testString));
    }
    
    private String getSampleData(String fileName) throws Exception {
      File file = new File( System.getProperty("test.cache.data") + File.separator + fileName);
      
      BufferedReader bufferedReader = new BufferedReader( new InputStreamReader( new FileInputStream( file ), "UTF-8" ) );
      String readline = bufferedReader.readLine();
      bufferedReader.close();
      
      return readline;
    }
      
    private void writeTestData(Object val, String fileName) throws Exception {
      File fileOutput = new File( System.getProperty("test.cache.data") + File.separator + fileName );
      BufferedWriter bufferedWriter = new BufferedWriter( new OutputStreamWriter( new FileOutputStream( fileOutput ), "UTF-8" ) );
      if( val instanceof JValue )
        bufferedWriter.write( ((JValue) val).toJSON() );
      else
        bufferedWriter.write( ((Item) val).toJSON() );
      bufferedWriter.flush();
      bufferedWriter.close();
    }
    
    private Item readTestData(String fileName) throws Exception {
      File fileOutput = new File( System.getProperty("test.cache.data") + File.separator + fileName );
      BufferedReader bufferedReader = new BufferedReader( new InputStreamReader( new FileInputStream( fileOutput ), "UTF-8" ) );
      JsonParser parser = new JsonParser(bufferedReader);
      Item i = parser.JsonVal();
      bufferedReader.close();
      
      return i;
    }
    
    protected abstract Object hideSampleData(String value) throws Exception;
    
    protected abstract String extractTestString(Item val);
  };
  
  @Test
  public void testJString() throws Exception {
  
      TypeTester tester = new TypeTester() {
        protected Object hideSampleData(String value) throws Exception {
          return new JString(value);
        }
        
        protected String extractTestString(Item val) {
          return ((JString)val.get()).toString();
        }
      };
      JaqlUtil.setEncoding(GOOD_ENC);
      tester.evaluate("i18nSample1.txt", "i18nSample1Out.txt");
      tester.expectMatch();
  }
  
  @Test
  public void testJRecordString() throws Exception {
    
    TypeTester tester = new TypeTester() {
      private String fieldName = "test";
      protected Object hideSampleData(String value) throws Exception {
        MemoryJRecord memoryJRecord = new MemoryJRecord();
        memoryJRecord.add( fieldName, new JString( value ) );
        
        return memoryJRecord;
      }
      
      protected String extractTestString(Item val) {
        JRecord r = (JRecord)val.get();
        return ((JString)r.getValue(fieldName).get()).toString();
      }
    };
    JaqlUtil.setEncoding(GOOD_ENC);
    tester.evaluate("i18nSample1.txt", "i18nSample1Out.txt");
    tester.expectMatch();
    
    JaqlUtil.setEncoding(BAD_ENC);
    tester.evaluate("i18nSample1.txt", "i18nSample1Out.txt");
    tester.expectNoMatch();
  }
  
  @Test
  public void testJRecordBytes() throws Exception {
    TypeTester tester = new TypeTester() {
      private String fieldName = "test";
      protected Object hideSampleData(String value) throws Exception {
        MemoryJRecord memoryJRecord = new MemoryJRecord();
        byte[] utf8 = value.getBytes( "UTF-8" );
        memoryJRecord.add( fieldName, new JString( utf8 ) );
        
        return memoryJRecord;
      }
      
      protected String extractTestString(Item val) {
        JRecord r = (JRecord)val.get();
        return ((JString)r.getValue(fieldName).get()).toString();
      }
    };
    JaqlUtil.setEncoding(GOOD_ENC);
    tester.evaluate("i18nSample1.txt", "i18nSample1Out.txt");
    tester.expectMatch();
    
    JaqlUtil.setEncoding(BAD_ENC);
    tester.evaluate("i18nSample1.txt", "i18nSample1Out.txt");
    tester.expectNoMatch();
  }
  
  @Test
  public void testItemJString() throws Exception {
    TypeTester tester = new TypeTester() {
      protected Object hideSampleData(String value) throws Exception {
        return new Item(new JString(value));
      }
      
      protected String extractTestString(Item val) {
        return ((JString)val.get()).toString();
      }
    };
    JaqlUtil.setEncoding(GOOD_ENC);
    tester.evaluate("i18nSample1.txt", "i18nSample1Out.txt");
    tester.expectMatch();
  }
  
  @Test
  public void testFixedJArray() throws Exception {
    TypeTester tester = new TypeTester() {
      protected Object hideSampleData(String value) throws Exception {
        FixedJArray arr = new FixedJArray();
        arr.add(new JString(value));
        return arr;
      }
      
      protected String extractTestString(Item val) {
        String r = null;
        try {
          r = ((JArray)val.get()).nth(0).get().toString();
        } catch(Exception e) {
          throw new RuntimeException(e);
        }
        return r;
      }
    };
    JaqlUtil.setEncoding(GOOD_ENC);
    tester.evaluate("i18nSample1.txt", "i18nSample1Out.txt");
    tester.expectMatch();
    
    JaqlUtil.setEncoding(BAD_ENC);
    tester.evaluate("i18nSample1.txt", "i18nSample1Out.txt");
    tester.expectNoMatch();
  }
  
  @Test
  public void testFunctionCall() throws Exception {
    TypeTester tester = new TypeTester() {
      protected Object hideSampleData(String value) throws Exception {
        JString s = new JString(value);
        ConstExpr c = new ConstExpr(s);
        SerializeFn fn = new SerializeFn(new Expr[] {c});
        Context ctx = new Context();
        
        return fn.eval(ctx);
      }
      
      protected String extractTestString(Item val) {
        String s = ((JString)val.get()).toString();
        return s.substring(1, s.length() - 1); // get rid of the quotes from serialize
      }
    };
    JaqlUtil.setEncoding(GOOD_ENC);
    tester.evaluate("i18nSample1.txt", "i18nSample1Out.txt");
    tester.expectMatch();
  }
  
  // test that map-reduce works
  @Test
  public void testMapReduce() {
    // 1. read string value, pass through job-conf in MR, pass back, validate
    // 2. read string value from MR (tests JaqlUtil setting)
  }
  
  // test that the shell works
  @Test
  public void testJaql() {
    
  }
  
  @Test
  public void testJRecordOriginal() throws Exception {
    String valName = "val";
    String valByteName = "valByte";
    BufferedReader bufferedReader = null;
    BufferedWriter bufferedWriter = null;
    JaqlUtil.setEncoding(GOOD_ENC);
    try
    {
      File file = new File( System.getProperty("test.cache.data") + File.separator + "i18nSample1.txt");
      File fileOutput = new File( System.getProperty("test.cache.data") + File.separator + "i18nSample1Out.txt" );
      
      bufferedReader = new BufferedReader( new InputStreamReader( new FileInputStream( file ), "UTF-8" ) );
      String readline = bufferedReader.readLine();
      
      // write it out in a record
      MemoryJRecord memoryJRecord = null;
      if ( readline != null )
      {
        memoryJRecord = new MemoryJRecord();
        byte[] utf8 = readline.getBytes( "UTF-8" );
        memoryJRecord.add( valName, new JString( readline ) );
        memoryJRecord.add( valByteName, new JString( utf8 ) );
        bufferedWriter = new BufferedWriter( new OutputStreamWriter( new FileOutputStream( fileOutput ), "UTF-8" ) );
        bufferedWriter.write( memoryJRecord.toString() );
        bufferedWriter.flush();
      }
      
      // read it back in
      bufferedReader.close();
      bufferedReader = new BufferedReader( new InputStreamReader( new FileInputStream( fileOutput ), "UTF-8" ) );
      JsonParser parser = new JsonParser(bufferedReader);
      Item i = parser.JsonVal();
      JRecord rr = (JRecord)i.get();
      String testVal = ((JString) rr.getValue(valName).get()).toString();
      String testValByte = ((JString) rr.getValue(valByteName).get()).toString();
      
      assertTrue("vals not equal: " + readline + " != " + testVal, readline.equals(testVal));
      assertTrue("btye vals not equal: " + readline + " != " + testValByte, readline.equals(testValByte));
    }
    catch ( FileNotFoundException e )
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    catch ( IOException e )
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    catch ( Exception e )
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    finally
    {
      try
      {
        if ( bufferedReader != null )
        {

          bufferedReader.close();

        }
        if ( bufferedWriter != null )
        {
          bufferedWriter.close();
        }
      }
      catch ( IOException e )
      {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }

  }    
}