package com.ibm.jaql.lang.expr.string;

import java.io.UnsupportedEncodingException;

import org.apache.commons.lang.CharEncoding;

import com.ibm.jaql.json.type.SubJsonString;
import com.ibm.jaql.json.util.JsonUtil;

/** Parsing of delimited data */
public abstract class DelParser
{
  public static final byte DOUBLE_QUOTE = '\"';

  public static final byte SINGLE_QUOTE = '\'';
  public static final byte BACK_SLASH = '\\';

  public static final byte BACKSPACE = '\b';
  public static final byte FORM_FEED = '\f';
  public static final byte LINE_FEED = '\n';
  public static final byte CARRIAGE_RETURN = '\r';
  public static final byte TAB = '\t';
  
  
  /** Read a single input field from <code>bytes</code> of specified <code>length</code> into the 
   * provided <code>target</code>. Starts reading the field at position <code>start</code> and
   * returns the start of the next field. */
  public abstract int readField(SubJsonString target, byte[] bytes, int length, int start);
  
  
  // -- factory methods ---------------------------------------------------------------------------
  
  /** Delimited but not quoted */
  public static DelParser make(byte delimiter, boolean quoted, boolean escape)
  {
    return quoted ? new QuotedDelParser(delimiter, escape) : new UnquotedDelParser(delimiter);
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
    private boolean escape;
    
    QuotedDelParser(byte delimiter, boolean escape)
    {
      this.delimiter = delimiter;
      this.escape = escape;
    }
    
    public int readField(SubJsonString target, byte[] bytes, int length, int start)
    {
      if (bytes[start] == DOUBLE_QUOTE)
      {
        return readFieldQuoted(target, bytes, length, start, delimiter, escape);
      }
      else
      {
        return readFieldUnquoted(target, bytes, length, start, delimiter);
      }
    }
  }
  
  
  // -- different methods for reading a field -----------------------------------------------------
  
  static int readFieldUnquoted(
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

  /**
   * Escape sequence is converted into the escaped character. Escape sequence
   * is prefixed with <tt>"</tt> or <tt>\</tt>. Both <tt>""</tt> and
   * <tt>\"</tt> are converted into <tt>"</tt>. Both 2-char escape sequence and
   * 6-char unicode sequence are supported. Copying is necessary if the field
   * is escaped. It is the reverse of {@link JsonUtil#quote(String, boolean,
   * char)}.
   */
  static int readFieldQuoted(SubJsonString target,
                             byte[] bytes,
                             int length,
                             int start,
                             byte delimiter,
                             boolean escape)
 {
    // there is a quote
    ++start;
    int end = start;

    // we have to copy byte by byte to unescape quotes
    byte[] output = null;
    int capacity = length - start; // play it safe
    int outputSize = 0;
    boolean unescaped = false;
    boolean firstEscaped = true;
    
    while (end < length) {
      if (bytes[end] == DOUBLE_QUOTE) {
        if (end + 1 >= length) {
          // The double quote is the last character
          end--;
          break;
        } else if (bytes[end + 1] == DOUBLE_QUOTE) {
          // escaped double quote
          if (firstEscaped) {
            output = new byte[capacity];
            outputSize = end - start;
            System.arraycopy(bytes, start, output, 0, outputSize);
            unescaped = true;
            firstEscaped = false;
          }
          output[outputSize++] = DOUBLE_QUOTE;
          end += 2;
        } else {
          // The character following double quote is not a double quote
          end--;
          break;
        }
      } else if (escape && bytes[end] == BACK_SLASH) {
        if (firstEscaped) {
          output = new byte[capacity];
          outputSize = end - start;
          System.arraycopy(bytes, start, output, 0, outputSize);
          unescaped = true;
          firstEscaped = false;
        }
        switch (bytes[end + 1]) {
          case SINGLE_QUOTE :
            output[outputSize++] = SINGLE_QUOTE;
            end += 2;
            break;
          case BACK_SLASH :
            output[outputSize++] = BACK_SLASH;
            end += 2;
            break;
          case 'b' :
            output[outputSize++] = BACKSPACE;
            end += 2;
            break;
          case 'f' :
            output[outputSize++] = FORM_FEED;
            end += 2;
            break;
          case 'n' :
            output[outputSize++] = LINE_FEED;
            end += 2;
            break;
          case 'r' :
            output[outputSize++] = CARRIAGE_RETURN;
            end += 2;
            break;
          case 't' :
            output[outputSize++] = TAB;
            end += 2;
            break;
          case 'u' :
            try {
              byte[] utf8 = toBytes(bytes, end + 2);
              int utf8Len = utf8.length;
              System.arraycopy(utf8, 0, output, outputSize, utf8Len);
              outputSize += utf8Len;
              end += 6;
            } catch(Exception e) {
              // perhaps it was just "BAKC_SLASHublahblah" ... keep BACK_SLASH and u
              output[outputSize++] = bytes[end++];
              output[outputSize++] = bytes[end++];
            }
            break;
          default :
            output[outputSize++] = bytes[end++]; // swallow the backslash as a literal
        }
      } else {
        if (unescaped)
          output[outputSize++] = bytes[end++];
        else
          end++;
      }
    }

    // checks closing quote
    ++end;
    if ((end >= length) || (end < length && bytes[end] != DOUBLE_QUOTE)) {
      throw new RuntimeException("ending quote missing in field starting at position "
          + start);
    }

    // check that there is a delimiter
    ++end;
    if (end < length && bytes[end] != delimiter) {
      throw new RuntimeException("delimiter missing in field starting at position "
          + start);
    }

    // process the field
    if (target != null) {
      if (unescaped)
        target.set(output, 0, outputSize);
      else
        target.set(bytes, start, end - start - 1);
    }
    end++;
    return end;
  }
  
  private static byte[] toBytes(byte[] bytes, int end) {
    try {
      char ch = toChar(bytes, end);
      String s = String.valueOf(ch);
      return s.getBytes(CharEncoding.UTF_8);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  private static char toChar(byte[] bytes, int end) {
    int ch = 0;
    for (int i = 0; i < 4; i++) {
      ch = (ch << 4) + toNumber(bytes[end + i]);
    }
    return (char) ch;
  }

  private static int toNumber(byte ch) {
    if ('0' <= ch && ch <= '9')
      return ch - '0';
    else if ('a' <= ch && ch <= 'f')
      return ch - 'a' + 10;
    else if ('A' <= ch && ch <= 'F')
      return ch - 'A' + 10;
    else
      throw new RuntimeException((char) ch + " is not a hex digit");
  }
}
