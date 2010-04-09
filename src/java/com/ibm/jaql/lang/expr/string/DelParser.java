package com.ibm.jaql.lang.expr.string;

import com.ibm.jaql.json.type.SubJsonString;

/** Parsing of delimited data */
public abstract class DelParser
{
  public static final byte QUOTE = '\"';
  
  /** Read a single input field from <code>bytes</code> of specified <code>length</code> into the 
   * provided <code>target</code>. Starts reading the field at position <code>start</code> and
   * returns the start of the next field. */
  public abstract int readField(SubJsonString target, byte[] bytes, int length, int start);
  
  
  // -- factory methods ---------------------------------------------------------------------------
  
  /** Delimited but not quoted */
  public static DelParser make(byte delimiter, boolean quoted)
  {
    return quoted ? new QuotedDelParser(delimiter) : new UnquotedDelParser(delimiter);
  }
  
  // -- implementing classes ----------------------------------------------------------------------
  
  private static final class UnquotedDelParser extends DelParser
  {
    private byte delimiter;
    
    UnquotedDelParser(byte delimiter)
    {
      this.delimiter = delimiter;
    }
    
    public int readField(SubJsonString target, byte[] bytes, int length, int start)
    {
      return readFieldUnquoted(target, bytes, length, start, delimiter);
    }
  }
  
  private static final class QuotedDelParser extends DelParser
  {
    private byte delimiter;
    
    QuotedDelParser(byte delimiter)
    {
      this.delimiter = delimiter;
    }
    
    public int readField(SubJsonString target, byte[] bytes, int length, int start)
    {
      
      if (bytes[start] == QUOTE)
      {
        return readFieldQuoted(target, bytes, length, start, delimiter);
      }
      else
      {
        return readFieldUnquoted(target, bytes, length, start, delimiter);
      }
    }
  }
  
  
  // -- different methods for reading a field -----------------------------------------------------
  
  private static int readFieldUnquoted(
      SubJsonString target, byte[] bytes, int length, int start, byte delimiter)
  {
    int end=start;
    
    // search the next delimiter
    while(end < length && bytes[end] != delimiter) 
    {
      ++end;
    };

    // process the field
    if (target != null)
    {
      target.set(bytes, start, end-start);
    }
    ++end;
    return end;
  }

  private static int readFieldQuoted(
      SubJsonString target, byte[] bytes, int length, int start, byte delimiter)
  {
    int end=start;
    
    // there is a quote but no escaping
    ++end;
    start = end;

    // search the next quote
    while(end < length && bytes[end] != QUOTE) 
    {
      ++end;
    };
      
    // check whether quote is escaped
    if (end+1 < length && bytes[end+1] == QUOTE)
    {
      // check whether we don't have an empty string ""
      if (! (end+2==length || (end+2<length && bytes[end+2]==delimiter)) )
      {
        return readFieldEscaped(target, bytes, length, start, end, delimiter);
      }
    }
      
    // no closing quote found
    if (end == length)
    {
      throw new RuntimeException("ending quote missing in field starting at position " + start);
    }
    
    // check that there is a delimiter
    ++end;
    if (end < length && bytes[end] != delimiter)
    {
      throw new RuntimeException("delimiter missing in field starting at position " + start);
    }
    
    // process the field
    if (target != null)
    {
      target.set(bytes, start, end-start-1);
    }
    ++end;
    
    return end;
  }
  
  /** Continues reading a field at a place where an escaped quote has been found. Copying is
   * necessary */
  private static int readFieldEscaped(SubJsonString target, byte[] bytes, int length, int start, 
      int end, byte delimiter)
  {
    // end points to beginning of escaped quote: "...^""..."
    ++end;
    
    // initialize byte array for output (we have to copy byte by byte to unescape quotes)
    byte[] output = new byte[length-start]; // play it safe
    int outputSize  = end-start;
    
    // copy
    System.arraycopy(bytes, start, output, 0, outputSize);
    ++end;
    // end points to after escaped quote: "...""^..."
    
    // search the next unescaped quote
    while (end < length)
    {
      // look at character
      if (bytes[end] == QUOTE)
      {
        // check whether quote is escaped
        if (end+1 < length && bytes[end+1] == QUOTE)
        {
          end+=2;
          output[outputSize] = QUOTE;
          ++outputSize;
          continue;
        }
        else
        {
          // found the ending quote
          break;
        }
      }
      output[outputSize] = bytes[end];
      ++outputSize;
      ++end;
    };
      
    // no closing quote found
    if (end == length)
    {
      throw new RuntimeException("ending quote missing in field starting at position " + start);
    }
      
    // check that there is a delimiter
    ++end;
    if (end < length && bytes[end] != delimiter)
    {
      throw new RuntimeException("delimiter missing in field starting at position " + start);
    }

    // process the field

    if (target != null)
    {
      target.set(output, 0, outputSize);
    }
    ++end;
    return end;
  }
}
