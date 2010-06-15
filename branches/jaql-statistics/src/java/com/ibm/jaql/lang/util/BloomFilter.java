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
package com.ibm.jaql.lang.util;

import java.util.Vector;
import java.util.Enumeration;

import com.ibm.jaql.json.type.JsonValue;

/**
 * 
 * This class creates a BloomFilter for a given set of values.
 */
public class BloomFilter 
{	
  /* bloom filter parameters */
  private static final int byteSize = 0x08;  /* eight bits per byte */
  private static int hashFunctionNo = 0x03;  /* number of hash functions to apply is set to 3*/
  private int signatureLength;               /* number of bytes to hold signature */
  private byte [] signature = null;
  
  /**
   * Creates an empty BloomFilter of length 'len' in bytes.
   */
  public BloomFilter(int len) 
  {
    signatureLength = len;
    signature = new byte[signatureLength];
    for (int index = 0; index < signatureLength; ++index) 
    	signature[index] = 0;
  }

  /**
   * Creates a BloomFilter from the given byte array.
   */
  public BloomFilter(byte [] input_signature) 
  {
    signatureLength = input_signature.length;
    signature = input_signature;
  }
   
  /**
   *  re-initializes the signature 
   */
  public void clearSignature() 
  {
	  for (int index = 0; index < signatureLength; ++index) 
		  signature[index] = 0;
  }
   
  /**
   * Returns the BloomFilter Signature 
   */
  public byte [] getBloomSignature()
  {
	  return signature;
  }
  
  /**
   * Returns the length of the BloomFilter Signature 
   */
  public int getSignatureLength()
  {
	  return signatureLength;
  }

  /**
   * Returns the number of hash functions used in the BloomFilter 
   */
  public int getSignatureHashCount()
  {
	  return hashFunctionNo;
  }

  /**
   * Inserts the given 'value' to the BloomFilter.
   */
  public void addValueToSignature(JsonValue json_value) 
  {
	  String value = json_value.toString();

	  /* first generate hash values */
	  Vector<Long> v = createHashValue(value);

	  /* and then add them to bloom filter */
	  for (Enumeration<Long> ven = v.elements(); ven.hasMoreElements();) 
	  {
		  Long hValue = (Long)ven.nextElement();
		  int hashValue = hValue.intValue()%(signatureLength*byteSize);
		  int index = hashValue/byteSize;
		  int hashIndex = hashValue%byteSize;
		  int exp = byteSize - hashIndex - 1;
		  int no = 1;
		  byte bno = (byte)(no << exp);
		  signature[index] |= bno;
	  }
  }
  
  /**
   * Checks if 'value' exists in the BloomFilter. It probes the BloomFilter to check if all bits corresponding to 'value' are set to 1.
   */
  public boolean containsValue(JsonValue json_value) 
  {
	  String value = json_value.toString();
	  return containsValue(value);
  }
  

  /**
   * Checks if 'value' exists in the BloomFilter. It probes the BloomFilter to check if all bits corresponding to 'value' are set to 1.
   */
  public boolean containsValue(String value) 
  {
	  /* first generate hash values */
	  Vector<Long> v = createHashValue(value);

	  /* test bloom filter */
	  for (Enumeration<Long> ven = v.elements(); ven.hasMoreElements();) 
	  {
		  Long hValue = (Long)ven.nextElement();
		  int hashValue = hValue.intValue()%(signatureLength*byteSize);
		  int index = hashValue/byteSize;
		  int hashIndex = hashValue%byteSize;
		  int exp = byteSize - hashIndex - 1;
		  int no = 1;
		  byte bno = (byte)(no << exp);
		  if ((signature[index] & bno) == 0) return false;
	  }
	  return true;
  }

  
  /**
   * Returns true if this BloomFilter contains the given 'external' BloomFilter. 
   */
  public boolean containsSignature(BloomFilter external)
  {
	  if (this.getSignatureLength() != external.getSignatureLength())
		  return false;
	  
	  for (int index = 0; index < signatureLength; ++index) 
	  {
		  byte not_sign = (byte)(~signature[index]);
		  int and_sign = (external.getBloomSignature()[index] & not_sign);
		  if (and_sign != 0) return (false);
	  }
	  return (true);
  }
  
  
  /**
   * Hash the given value 'val' to multiple values (bits) in the BloomFilter.  
   */
  private Vector<Long> createHashValue(String val)
  {
	  Vector<Long> v = new Vector<Long>();
	  v.add(new Long(elfHashValue(val)));
	  if (hashFunctionNo > 1) {
		  v.add(new Long(rsHashValue(val)));
	  }
	  if (hashFunctionNo > 2) {
		  v.add(new Long(bkdrHashValue(val)));
	  }
	  if (hashFunctionNo > 3) {
		  v.add(new Long(sdbmHashValue(val)));
	  }
	  if (hashFunctionNo > 4) {
		  v.add(new Long(djbHashValue(val)));
	  }
	  if (hashFunctionNo > 5) {
		  v.add(new Long(dekHashValue(val)));
	  }
	  if (hashFunctionNo > 6) {
		  v.add(new Long(apHashValue(val)));
	  }
	  return v;
  }
  
  /**
   * A hash function.
   */
  private long elfHashValue(String str) { /* compute the ELF hash value of a string */
	  long hashValue = 0;
	  long zero = 0;
	  for (int index = 0; index < str.length(); index++) 
	  {
		  hashValue = (hashValue << 4) + str.charAt(index);
		  if((zero = hashValue & 0xF0000000L) != 0) 
		  {
			  hashValue ^= (zero >> 24);
			  hashValue &= ~zero;
		  }
	  }
	  return (hashValue & 0x7FFFFFFF);
  }

  /**
   * A hash function.
   */
  private long rsHashValue(String str) { /* compute the RS hash value of a string */
	  int b = 378551;
	  int a = 63689;
	  long hashValue = 0;

	  for (int i = 0; i < str.length(); i++) 
	  {
		  hashValue = hashValue*a + str.charAt(i);
		  a = a*b;
	  }
	  return (hashValue & 0x7FFFFFFF);
  }

  /**
   * A hash function.
   */
  private long bkdrHashValue(String str) {
	  long seed = 131; // 31 131 1313 13131 131313 etc..
	  long hashValue = 0;

	  for (int i = 0; i < str.length(); i++) 
	  {
		  hashValue = (hashValue * seed) + str.charAt(i);
	  }
	  return (hashValue & 0x7FFFFFFF);
  }

  /**
   * A hash function.
   */
  private long sdbmHashValue(String str) {
	  long hashValue = 0;

	  for (int i = 0; i < str.length(); i++) 
	  {
		  hashValue = str.charAt(i) + (hashValue << 6) + (hashValue << 16) - hashValue;
	  }
	  return (hashValue & 0x7FFFFFFF);
  }

  /**
   * A hash function.
   */
  private long djbHashValue(String str) {
	  long hashValue = 5381;
	  for (int i = 0; i < str.length(); i++) 
	  {
		  hashValue = ((hashValue << 5) + hashValue) + str.charAt(i);
	  }
	  return (hashValue & 0x7FFFFFFF);
  }

  /**
   * A hash function.
   */
  private long dekHashValue(String str) {
	  long hashValue = str.length();
	  for (int i = 0; i < str.length(); i++) 
	  {
		  hashValue = ((hashValue << 5) ^ (hashValue >> 27)) ^ str.charAt(i);
	  }
	  return (hashValue & 0x7FFFFFFF);
  }

  /**
   * A hash function.
   */
  private long apHashValue(String str) {
	  long hashValue = 0;
	  for (int i = 0; i < str.length(); i++) 
	  {
		  if ((i & 1) == 0) 
			  hashValue ^= ((hashValue << 7) ^ str.charAt(i) ^ (hashValue >> 3));
		  else 
			  hashValue ^= (~((hashValue << 11) ^ str.charAt(i) ^ (hashValue >> 5)));
	  }
	  return (hashValue & 0x7FFFFFFF);
  }  
}