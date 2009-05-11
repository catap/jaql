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
package com.ibm.jaql.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 */
public class BaseUtil
{
  public static final Log     LOG             = LogFactory
                                                  .getLog(BaseUtil.class
                                                      .getName());
  public static final char    HEX_NIBBLE[]     = {'0', '1', '2', '3', '4', '5',
      '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};
  public static final int     GOLDEN_RATIO_32 = 0x09e3779b9;
  public static final long    GOLDEN_RATIO_64 = 0x9e3779b97f4a7c13L;
  public static final boolean OS_MS_WINDOWS   = System.getProperty("os.name")
                                                  .startsWith("Windows");

  //  public static void writeVUInt(DataOutput out, int v) throws IOException
  //  {
  //    if( v < 0 )
  //    {
  //      throw new IOException("vint must be positive: "+v);
  //    }
  //    else if( v < 0x80 ) // 1 byte, 7 bits, 0*
  //    {
  //      out.writeByte(v);
  //    }
  //    else if( v < 0x4000 ) // 2 bytes, 14 bits, 10*
  //    {
  //      out.writeByte((v >> 8) | 0x80);
  //      out.writeByte((v & 0xff));
  //    }
  //    else if( v < 0x200000 ) // 3 bytes, 21 bits, 110*
  //    {
  //      out.writeByte((v >> 16) | 0xC0);
  //      out.writeByte((v >> 8) & 0xff);
  //      out.writeByte((v & 0xff));
  //    }
  //    else if( v < 0x10000000 )  // 4 bytes, 28 bits, 1110*
  //    {
  //      out.writeByte((v >> 24) | 0xE0);
  //      out.writeByte((v >> 16) & 0xff);
  //      out.writeByte((v >> 8) & 0xff);
  //      out.writeByte((v & 0xff));
  //    }
  //    else // 5 bytes, 31 bits (actually 35 but top bits are zero), 1111 0*
  //    {
  //      out.writeByte(0xF0);
  //      out.writeByte((v >> 24) & 0xff);
  //      out.writeByte((v >> 16) & 0xff);
  //      out.writeByte((v >> 8) & 0xff);
  //      out.writeByte((v & 0xff));
  //    }
  //  }

  //  public static void writeVULong(DataOutput out, long v) throws IOException
  //  {
  //    if( v < 0 )
  //    {
  //      throw new IOException("vint must be positive: "+v);
  //    }
  //    else if( v < 0x80 ) // 1 byte, 7 bits, 0*
  //    {
  //      out.writeByte((int)v);
  //    }
  //    else if( v < 0x4000 ) // 2 bytes, 14 bits, 10*
  //    {
  //      out.writeByte((int)(v >> 8) | 0x80);
  //      out.writeByte((int)(v & 0xff));
  //    }
  //    else if( v < 0x200000 ) // 3 bytes, 21 bits, 110*
  //    {
  //      out.writeByte((int)(v >> 16) | 0xC0);
  //      out.writeByte((int)(v >> 8) & 0xff);
  //      out.writeByte((int)(v & 0xff));
  //    }
  //    else if( v < 0x10000000 ) // 4 bytes, 28 bits, 1110*
  //    {
  //      out.writeByte((int)(v >> 24) | 0xE0);
  //      out.writeByte((int)(v >> 16) & 0xff);
  //      out.writeByte((int)(v >> 8) & 0xff);
  //      out.writeByte((int)(v & 0xff));
  //    }
  //    else if( v < 0x800000000L ) // 5 bytes, 35 bits, 1111 0*
  //    {
  //      out.writeByte((int)(v >> 32) | 0xF0);
  //      out.writeByte((int)(v >> 24) & 0xff);
  //      out.writeByte((int)(v >> 16) & 0xff);
  //      out.writeByte((int)(v >> 8) & 0xff);
  //      out.writeByte((int)(v & 0xff));
  //    }
  //    else if( v < 0x40000000000L ) // 6 bytes, 42 bits, 1111 10*
  //    {
  //      out.writeByte((int)(v >> 40) | 0xF8);
  //      out.writeByte((int)(v >> 32) & 0xff);
  //      out.writeByte((int)(v >> 24) & 0xff);
  //      out.writeByte((int)(v >> 16) & 0xff);
  //      out.writeByte((int)(v >> 8) & 0xff);
  //      out.writeByte((int)(v & 0xff));
  //    }
  //    else if( v < 0x2000000000000L ) // 7 bytes, 49 bits, 1111 110*
  //    {
  //      out.writeByte((int)(v >> 48) | 0xFC);
  //      out.writeByte((int)(v >> 40) & 0xff);
  //      out.writeByte((int)(v >> 32) & 0xff);
  //      out.writeByte((int)(v >> 24) & 0xff);
  //      out.writeByte((int)(v >> 16) & 0xff);
  //      out.writeByte((int)(v >> 8) & 0xff);
  //      out.writeByte((int)(v & 0xff));
  //    }
  //    else if( v < 0x100000000000000L ) // 8 bytes, 56 bits, 1111 1110*
  //    {
  //      out.writeByte(0xFE);
  //      out.writeByte((int)(v >> 48) & 0xff);
  //      out.writeByte((int)(v >> 40) & 0xff);
  //      out.writeByte((int)(v >> 32) & 0xff);
  //      out.writeByte((int)(v >> 24) & 0xff);
  //      out.writeByte((int)(v >> 16) & 0xff);
  //      out.writeByte((int)(v >> 8) & 0xff);
  //      out.writeByte((int)(v & 0xff));
  //    }
  //    else // 9 bytes, 63 bits, 1111 1111 0*
  //    {
  //      out.writeByte(0xFF);
  //      out.writeByte((int)(v >> 56) & 0xff);
  //      out.writeByte((int)(v >> 48) & 0xff);
  //      out.writeByte((int)(v >> 40) & 0xff);
  //      out.writeByte((int)(v >> 32) & 0xff);
  //      out.writeByte((int)(v >> 24) & 0xff);
  //      out.writeByte((int)(v >> 16) & 0xff);
  //      out.writeByte((int)(v >> 8) & 0xff);
  //      out.writeByte((int)(v & 0xff));
  //    }
  //  }

  //  public static int readVUInt(DataInput in) throws IOException
  //  {
  //    int v = in.readUnsignedByte();
  //    int f = 0x80;
  //    while( (v & f) != 0 )
  //    {
  //      v = ((v & ~f) << 8) | in.readUnsignedByte();
  //      f <<= 7;
  //    }
  //    return v;
  //  }

  //  public static long readVULong(DataInput in) throws IOException
  //  {
  //    long v = in.readUnsignedByte();
  //    long f = 0x80;
  //    while( (v & f) != 0 )
  //    {
  //      v = ((v & ~f) << 8) | in.readUnsignedByte();
  //      f <<= 7;
  //    }
  //    return v;
  //  }

  //  public static void writeVSInt(DataOutput out, int v) throws IOException
  //  {
  //    int flip = 0x00;
  //    if( v < 0 )
  //    {
  //      flip = 0xff; // flip bits
  //      v = ~v;      // complement
  //    }
  //    if( v <= 0x3f ) // 1 byte, 6 bits, neg:01*, pos:10*
  //    {
  //      out.writeByte( ((v      ) | 0x80) ^ flip );
  //    }
  //    else if( v <= 0x1fff ) // 2 bytes, 13 bits, neg:001*, pos:110*
  //    {
  //      out.writeByte( ((v >>  8) | 0xC0) ^ flip );
  //      out.writeByte( ((v      ) & 0xff) ^ flip );
  //    }
  //    else if( v <= 0xfffff ) // 3 bytes, 20 bits, neg:0001*, pos:1110*
  //    {
  //      out.writeByte( ((v >> 16) | 0xE0) ^ flip );
  //      out.writeByte( ((v >>  8) & 0xff) ^ flip );
  //      out.writeByte( ((v      ) & 0xff) ^ flip );
  //      return;
  //    }
  //    else if( v <= 0x7ffffff ) // 4 bytes, 27 bits, neg:00001*, pos:11110*
  //    {
  //      out.writeByte( ((v >> 24) | 0xF0) ^ flip );
  //      out.writeByte( ((v >> 16) & 0xff) ^ flip );
  //      out.writeByte( ((v >>  8) & 0xff) ^ flip );
  //      out.writeByte( ((v      ) & 0xff) ^ flip );
  //    }
  //    else // 5 bytes, 32 bits (actually 34 bits, but the top two are always zero), neg:000001*, pos:111110*
  //    {
  //      out.writeByte( 0xF8 ^ flip );
  //      out.writeByte( ((v >> 24) & 0xff) ^ flip );
  //      out.writeByte( ((v >> 16) & 0xff) ^ flip );
  //      out.writeByte( ((v >>  8) & 0xff) ^ flip );
  //      out.writeByte( ((v      ) & 0xff) ^ flip );
  //    }
  //  }

  /**
   * 
   * 0x00, 0 0x01, 1 ... 0xf7 247 0xf8 1 byte pos ... 0xfe 7 byte pos 0xff 8
   * byte pos
   */
  public static void writeVULong(DataOutput out, long v) throws IOException
  {
    if (v <= 0xf7) // v <= 247
    {
      if (v < 0)
      {
        throw new IOException("positive value expected: " + v);
      }
      out.writeByte((int)v);
      return;
    }

    // 248 <= v <= maxLong
    v -= 0xf8;
    int len = 1;
    long t = v >> 8;
    while (t > 0)
    {
      t >>= 8;
      len++;
    }

    out.writeByte(0xf7 + len);

    do
    {
      len--;
      out.writeByte((int) (v >> (len << 3)));
    } while (len > 0);
  }

  /**
   * @param in
   * @return
   * @throws IOException
   */
  public static long readVULong(DataInput in) throws IOException
  {
    int b = in.readUnsignedByte();
    if (b <= 0xf7)
    {
      return b;
    }
    // 248 <= v <= maxLong

    b -= 0xf7;
    long v = 0;
    do
    {
      v = (v << 8) | in.readUnsignedByte();
      b--;
    } while (b > 0);

    v += 0xf8;
    return v;
  }

  /**
   * 
   * 0x00, 0 0x01, 1 ... 0xfB 251 0xfC 1 byte pos ... 0xfe 3 byte pos 0xff 4
   * byte pos
   */
public static void writeVUInt(DataOutput out, int v) throws IOException
  {
    if (v <= 0xfB) // v <= 251
    {
      if (v < 0)
      {
        throw new IOException("positive value expected: " + v);
      }
      out.writeByte(v);
      return;
    }

    // 252 <= v <= maxInt
    v -= 0xfC;
    int len = 1;
    long t = v >> 8;
    while (t > 0)
    {
      t >>= 8;
      len++;
    }

    out.writeByte(0xfB + len);

    do
    {
      len--;
      out.writeByte(v >> (len << 3));
    } while (len > 0);
  }

  /**
   * @param in
   * @return
   * @throws IOException
   */
  public static int readVUInt(DataInput in) throws IOException
  {
    int b = in.readUnsignedByte();
    if (b <= 0xfB)
    {
      return b;
    }
    // 252 <= v <= maxLong

    b -= 0xfB;
    int v = 0;
    do
    {
      v = (v << 8) | in.readUnsignedByte();
      b--;
    } while (b > 0);

    v += 0xFC;
    return v;
  }

  /**
   * 
   * 0x00, 8 byte neg 0x01, 7 byte neg ... 0x07 1 byte neg 0x08 -120 0x09 -119
   * ... 0x80 0 ... 0xf6 118 0xf7 119 0xf8 1 byte pos ... 0xfe 7 byte pos 0xff 8
   * byte pos
   */
  public static void writeVSLong(DataOutput out, long v) throws IOException
  {
    int flip;
    if (v >= -120)
    {
      if (v <= 119)
      {
        out.writeByte((byte) (v + 0x80));
        return;
      }
      // v >= 120
      flip = 0x00;
    }
    else
    // v <= -121
    {
      v = ~v; // take one's complement of negative numbers 
      flip = 0xff;
    }

    // 120 <= v <= maxLong
    v -= 120;
    int len = 1;
    long t = v >> 8;
    while (t > 0)
    {
      t >>= 8;
      len++;
    }

    out.writeByte((byte) ((0xf7 + len) ^ flip));

    do
    {
      len--;
      out.writeByte((byte) ((v >> (len << 3)) ^ flip));
    } while (len > 0);
  }

  /**
   * @param in
   * @return
   * @throws IOException
   */
  public static long readVSLong(DataInput in) throws IOException
  {
    int flip;
    int b = in.readUnsignedByte();
    if (b >= 0x08)
    {
      if (b <= 0xf7)
      {
        return b - 0x80;
      }
      // v >= 120
      flip = 0;
    }
    else
    // v <= -121
    {
      flip = 0xff;
    }

    b = (b ^ flip) - 0xf7;
    long v = 0;
    do
    {
      v = (v << 8) | (in.readUnsignedByte() ^ flip);
      b--;
    } while (b > 0);

    v += 120;
    v ^= (byte) flip; // 0 or -1 as a long -> one's complement for negatives
    return v;
  }

  /**
   * @param out
   * @param v
   * @throws IOException
   */
  public static void writeVSInt(DataOutput out, int v) throws IOException
  {
    writeVSLong(out, v); // TODO: own method for efficiency
  }

  /**
   * @param in
   * @return
   * @throws IOException
   */
  public static int readVSInt(DataInput in) throws IOException
  {
    return (int) readVSLong(in); // TODO: own method for efficiency
  }
  //  public static void writeVSLong(DataOutput out, long v) throws IOException
  //  {
  //    int flip = 0x00;
  //    if( v < 0 )
  //    {
  //      flip = 0xff; // flip bits
  //      v = ~v;   // complement
  //    }
  //    if( v <= 0x3f ) // 1 byte, 6 bits, neg:01*, pos:10*
  //    {
  //      out.writeByte( ((int)(v      ) | 0x80) ^ flip );
  //    }
  //    else if( v <= 0x1fff ) // 2 bytes, 13 bits, neg:001*, pos:110*
  //    {
  //      out.writeByte( ((int)(v >>  8) | 0xC0) ^ flip );
  //      out.writeByte( ((int)(v      ) & 0xff) ^ flip );
  //      return;
  //    }
  //    else if( v <= 0xfffff ) // 3 bytes, 20 bits, neg:0001*, pos:1110*
  //    {
  //      out.writeByte( ((int)(v >> 16) | 0xE0) ^ flip );
  //      out.writeByte( ((int)(v >>  8) & 0xff) ^ flip );
  //      out.writeByte( ((int)(v      ) & 0xff) ^ flip );
  //    }
  //    else if( v <= 0x7ffffff ) // 4 bytes, 27 bits, neg:00001*, pos:11110*
  //    {
  //      out.writeByte( ((int)(v >> 24) | 0xF0) ^ flip );
  //      out.writeByte( ((int)(v >> 16) & 0xff) ^ flip );
  //      out.writeByte( ((int)(v >>  8) & 0xff) ^ flip );
  //      out.writeByte( ((int)(v      ) & 0xff) ^ flip );
  //    }
  //    else if( v <= 0x3ffffffffL ) // 5 bytes, 34 bits, neg:0000 01*, pos:1111 10*
  //    {
  //      out.writeByte( ((int)(v >> 32) | 0xF8) ^ flip );
  //      out.writeByte( ((int)(v >> 24) & 0xff) ^ flip );
  //      out.writeByte( ((int)(v >> 16) & 0xff) ^ flip );
  //      out.writeByte( ((int)(v >>  8) & 0xff) ^ flip );
  //      out.writeByte( ((int)(v      ) & 0xff) ^ flip );
  //    }
  //    else if( v <= 0x1ffffffffffL ) // 6 bytes, 41 bits, neg:0000 001*, pos:1111 110*
  //    {
  //      out.writeByte( ((int)(v >> 40) | 0xFC) ^ flip );
  //      out.writeByte( ((int)(v >> 32) & 0xff) ^ flip );
  //      out.writeByte( ((int)(v >> 24) & 0xff) ^ flip );
  //      out.writeByte( ((int)(v >> 16) & 0xff) ^ flip );
  //      out.writeByte( ((int)(v >>  8) & 0xff) ^ flip );
  //      out.writeByte( ((int)(v      ) & 0xff) ^ flip );
  //    }
  //    else if( v <= 0xffffffffffffL ) // 7 bytes, 48 bits, neg:0000 0001*, pos:1111 1110*
  //    {
  //      out.writeByte( ((int)(v >> 47) | 0xFE) ^ flip );
  //      out.writeByte( ((int)(v >> 40) & 0xff) ^ flip );
  //      out.writeByte( ((int)(v >> 32) & 0xff) ^ flip );
  //      out.writeByte( ((int)(v >> 24) & 0xff) ^ flip );
  //      out.writeByte( ((int)(v >> 16) & 0xff) ^ flip );
  //      out.writeByte( ((int)(v >>  8) & 0xff) ^ flip );
  //      out.writeByte( ((int)(v      ) & 0xff) ^ flip );
  //    }
  //    else // 9 bytes, 64 bits, neg: 0000 0000*, pos: 1111 1111*
  //    {
  //      out.writeByte( 0xFF ^ flip );
  //      out.writeByte( ((int)(v >> 56) & 0xff) ^ flip );
  //      out.writeByte( ((int)(v >> 48) & 0xff) ^ flip );
  //      out.writeByte( ((int)(v >> 40) & 0xff) ^ flip );
  //      out.writeByte( ((int)(v >> 32) & 0xff) ^ flip );
  //      out.writeByte( ((int)(v >> 24) & 0xff) ^ flip );
  //      out.writeByte( ((int)(v >> 16) & 0xff) ^ flip );
  //      out.writeByte( ((int)(v >>  8) & 0xff) ^ flip );
  //      out.writeByte( ((int)(v      ) & 0xff) ^ flip );
  //    }
  //  }

  //  public static int readVSInt(DataInput in) throws IOException
  //  {
  //    int v = in.readUnsignedByte();
  //    int flip = 0x00;
  //    if( (v & 0x80) == 0 ) // negative
  //    {
  //      flip = 0xff;
  //      v = ~v;
  //    }
  //    v &= 0x7f;
  //    int f = 0x40;
  //    while( (v & f) != 0 )
  //    {
  //      v = ((v & ~f) << 8) | (in.readUnsignedByte() ^ flip);
  //      f <<= 7;
  //    }
  //    if( flip != 0 )
  //    {
  //      v = ~v; // complement
  //    }
  //    return v;
  //  }

  //  public static long readVSLong(DataInput in) throws IOException
  //  {
  //    int len = in.readUnsignedByte();
  //    int flip = 0x00;
  //    if( (len & 0x80) == 0 ) // negative
  //    {
  //      flip = 0xff;
  //      v ^= 0xff; // flip the byte
  //    }
  //    long v = len;
  //    len = len << 8
  //  }
  //
  //  public static long readVSLong1(DataInput in) throws IOException
  //  {
  //    long v = in.readUnsignedByte();
  //    int flip = 0x00;
  //    if( (v & 0x80) == 0 ) // negative
  //    {
  //      flip = 0xff;
  //      v ^= 0xff; // flip the byte
  //    }
  //    if( v == 0xff )
  //    {
  //      v = (((long)(in.readUnsignedByte() ^ flip)) << 56)
  //        | (((long)(in.readUnsignedByte() ^ flip)) << 48)
  //        | (((long)(in.readUnsignedByte() ^ flip)) << 40)
  //        | (((long)(in.readUnsignedByte() ^ flip)) << 32)
  //        | (((long)(in.readUnsignedByte() ^ flip)) << 24)
  //        | (((long)(in.readUnsignedByte() ^ flip)) << 16)
  //        | (((long)(in.readUnsignedByte() ^ flip)) <<  8)
  //        | (((long)(in.readUnsignedByte() ^ flip))      );
  //    }
  //    else
  //    {
  //      long f = 0x40;
  //      while( (v & f) != 0 )
  //      {
  //        v = ((v & ~f) << 8) | (in.readUnsignedByte() ^ flip);
  //        f <<= 7;
  //      }
  //    }
  //    if( flip != 0 )
  //    {
  //      v = ~v; // complement
  //    }
  //    return v;
  //  }

  /**
   * @param hexDigit
   * @return
   */
  public static byte parseHex(char hexDigit)
  {
    if (hexDigit >= '0' && hexDigit <= '9')
    {
      return (byte) (hexDigit - '0');
    }
    if (hexDigit >= 'a' && hexDigit <= 'f')
    {
      return (byte) (hexDigit - 'a' + 10);
    }
    if (hexDigit >= 'A' && hexDigit <= 'F')
    {
      return (byte) (hexDigit - 'A' + 10);
    }
    throw new IllegalArgumentException("invalid hex digit:" + hexDigit);
  }

  /**
   * @param h1
   * @param h2
   * @return
   */
  public static byte parseHexByte(char h1, char h2)
  {
    return (byte) ((parseHex(h1) << 4) | parseHex(h2));
  }

  /**
   * @param h1
   * @param h2
   * @param h3
   * @param h4
   * @return
   */
  public static char parseUnicode(char h1, char h2, char h3, char h4)
  {
    return (char) ((parseHex(h1) << 24) | (parseHex(h2) << 16)
        | (parseHex(h3) << 8) | (parseHex(h4)));
  }
}
