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
package com.ibm.jaql.json.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.ibm.jaql.util.BaseUtil;

/**
 * 
 */
public class JRegex extends JAtom
{
  protected final static int GLOBAL           = 1; // return all matches instead of just the first
  protected final static int MULTILINE        = 2; // ^$ match any line.  Pattern.MULTILINE
  protected final static int CASE_INSENSITIVE = 4; // Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE
  // Hey! Don't use the high-order bit without changing to flags to vint 

  protected JString          regex;
  protected byte             flags;
  protected Matcher          matcher;             // FIXME: make sure java and javascript agree 

  //  // This function expects a well-formed regex as returned from the jaql lexer.
  //  public static RegexItem parse(String regex)
  //  {
  //    // TODO: support m!! regex syntax?
  //    // FIXME: this exposes most of java's regex syntax, which probably is not the same as javascripts...
  //    assert regex.charAt(0) == '/';
  //    int i = regex.lastIndexOf('/');
  //    Text r = new Text(regex.substring(1,i));
  //    byte flags = 0;
  //    for( i++ ; i < regex.length() ; i++ )
  //    {
  //      switch( regex.charAt(i) )
  //      {
  //        case 'g': flags |= GLOBAL; break;
  //        case 'm': flags |= MULTILINE; break;
  //        case 'i': flags |= CASE_INSENSITIVE; break;
  //        default: throw new IllegalArgumentException("unknown regex flag: "+regex.charAt(i));
  //      }
  //    }
  //    return new RegexItem(r, flags);
  //  }

  /**
   * 
   */
  public JRegex()
  {
    regex = new JString();
  }

  /**
   * @param regex
   * @param flags
   */
  public JRegex(JString regex, byte flags)
  {
    this.regex = regex;
    this.flags = flags;
  }

  /**
   * @param regex
   * @param flags
   */
  public JRegex(JString regex, JString flags)
  {
    this.regex = regex;
    String sflags = flags.toString();
    byte f = 0;
    for (int i = 0; i < sflags.length(); i++)
    {
      switch (sflags.charAt(i))
      {
        case 'g' :
          f |= GLOBAL;
          break;
        case 'm' :
          f |= MULTILINE;
          break;
        case 'i' :
          f |= CASE_INSENSITIVE;
          break;
        default :
          throw new IllegalArgumentException("unknown regex flag: "
              + sflags.charAt(i));
      }
    }
    this.flags = f;
  }

  /**
   * @param regex
   * @param flags
   */
  public JRegex(String regex, byte flags)
  {
    this(new JString(regex), flags);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#getEncoding()
   */
  @Override
  public Item.Encoding getEncoding()
  {
    return Item.Encoding.REGEX;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#compareTo(java.lang.Object)
   */
  @Override
  public int compareTo(Object x)
  {
    JRegex y = (JRegex) x;
    return regex.compareTo(y.regex);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#longHashCode()
   */
  @Override
  public long longHashCode()
  {
    return (regex.longHashCode() * BaseUtil.GOLDEN_RATIO_64) ^ flags;
  }

  /**
   * @return
   */
  protected Matcher compileRegex()
  {
    int flags = 0;
    if (isCaseInsensitive())
    {
      flags |= Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE;
    }
    if (isMultiline())
    {
      flags |= Pattern.MULTILINE;
    }
    Pattern pattern = Pattern.compile(regex.toString(), flags);
    return pattern.matcher("");
  }

  /**
   * If this object could have multiple concurrent users of this matcher, then
   * use takeMatcher() and returnMatcher() instead.
   * 
   * @return
   */
  public Matcher getMatcher()
  {
    if (matcher == null)
    {
      matcher = compileRegex();
    }
    return matcher;
  }

  /**
   * @return
   */
  public Matcher takeMatcher()
  {
    Matcher m = this.matcher;
    if (m != null)
    {
      this.matcher = null;
    }
    else
    {
      m = compileRegex();
    }
    return m;
  }

  /**
   * @param matcher
   */
  public void returnMatcher(Matcher matcher)
  {
    this.matcher = matcher;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#readFields(java.io.DataInput)
   */
  @Override
  public void readFields(DataInput in) throws IOException
  {
    regex.readFields(in);
    matcher = null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#write(java.io.DataOutput)
   */
  @Override
  public void write(DataOutput out) throws IOException
  {
    regex.write(out);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#toJSON()
   */
  @Override
  public String toJSON()
  {
    //    out.print('/');
    //    if( regex.find("/") < 0 )
    //    {
    //      out.print(regex);      
    //    }
    //    else
    //    {
    //      for(int i = 0 ; i < regex.getLength() ; i++)
    //      {
    //        int c = regex.charAt(i);
    //        if( c == '/' )
    //        {
    //          out.print("\\/");
    //        }
    //        else
    //        {
    //          out.print((char)c);
    //        }
    //      }
    //    }
    //    out.print('/');
    StringBuffer buf = new StringBuffer();
    buf.append("regex(");
    buf.append(regex.toJSON());
    if (flags != 0)
    {
      buf.append(",'");
      if ((flags & GLOBAL) != 0) buf.append('g');
      if ((flags & MULTILINE) != 0) buf.append('m');
      if ((flags & CASE_INSENSITIVE) != 0) buf.append('i');
      buf.append("'");
    }
    buf.append(")");
    return buf.toString();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#clone()
   */
  public JRegex clone()
  {
    return new JRegex(new JString(regex), flags);
  }

  /**
   * @return
   */
  public boolean isGlobal()
  {
    return (flags & GLOBAL) != 0;
  }

  /**
   * @return
   */
  public boolean isCaseInsensitive()
  {
    return (flags & CASE_INSENSITIVE) != 0;
  }

  /**
   * @return
   */
  public boolean isMultiline()
  {
    return (flags & MULTILINE) != 0;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#copy(com.ibm.jaql.json.type.JValue)
   */
  public void copy(JValue jvalue) throws Exception
  {
    JRegex r = (JRegex) jvalue;
    this.regex.copy(r.regex);
    this.flags = r.flags;
    this.matcher = null;
  }
}
