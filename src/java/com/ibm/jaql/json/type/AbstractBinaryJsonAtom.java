package com.ibm.jaql.json.type;

import java.io.DataOutput;
import java.io.IOException;

/** Base class for {@link JsonBinary} and {@link JsonString} (package private). Provides basic
 * methods for maintaining a byte array. */
abstract class AbstractBinaryJsonAtom extends JsonAtom
{
  protected static final byte[] EMPTY_BUFFER = new byte[0];
  
  protected byte[] bytes = EMPTY_BUFFER;
  protected int bytesLength = 0;

  /** Called whenever bytes are accessed. Allows subclasses to do something. */
  protected void ensureBytes()
  {    
  }
  
  // -- reading/writing ---------------------------------------------------------------------------

  /** Returns the length in number of bytes. */
  public int bytesLength()
  {
    ensureBytes();
    return bytesLength;
  }

  /** Retrieves the byte at the specified index. */
  public byte get(int i)
  {
    ensureBytes();
    if (i<0 || i>=bytesLength()) throw new ArrayIndexOutOfBoundsException();
    return bytes[i];
  }

  /** Retrieves a copy of the internal bytes. */
  public byte[] getCopy()
  {
    ensureBytes();
    byte[] b = new byte[bytesLength()];
    writeBytes(b);
    return b;
  }
  
  /** Write the internal bytes to the specified buffer. 
   *
   * Implemented as call to <code>writeBytes(0, dest, 0, length())<code>.
   */
  public final void writeBytes(byte[] dest)
  {
    writeBytes(0, dest, 0, bytesLength());
  }

  /** Write the internal bytes to the specified buffer. 
   * 
   * Implemented as call to <code>writeBytes(0, dest, 0, length)<code>. 
   */
  public final void writeBytes(byte[] dest, int length)
  {
    writeBytes(0, dest, 0, length);
  }

  /** Write the internal bytes to the specified buffer. 
   * 
   * Implemented as call to <code>writeBytes(0, dest, destpos, length)<code>. 
   */
  public final void writeBytes(byte[] dest, int destpos, int length)
  {
    writeBytes(0, dest, destpos, length);
  }

  /** Write the internal bytes to the specified buffer. */
  public void writeBytes(int srcpos, byte[] dest, int destpos, int length)
  {
    ensureBytes();
    if (srcpos<0 || srcpos+length>bytesLength()) throw new ArrayIndexOutOfBoundsException();
    System.arraycopy(this.bytes, srcpos, dest, destpos, length);
  }

  /** Writes the internal bytes to the specified output. */
  public void writeBytes(DataOutput out) throws IOException
  {
    ensureBytes();
    out.write(this.bytes, 0, bytesLength());
  }
  
  
  // -- mutation (all protected) ------------------------------------------------------------------

  /** Sets the value to the given byte array. The array is not copied but 
   * directly used as internal buffer. 
   * 
   * Implemented as call to <code>set(bytes, 0)<code>.
   * 
   * @param buffer a byte buffer
   */
  protected void set(byte[] bytes)
  {
    set(bytes, bytes.length);
  }

  /** Sets the value to the first <code>length</code> bytes of the given byte 
   * array. The array is not copied but directly used as internal buffer. 
   * 
   * @param buffer a byte buffer
   * @param length number of bytes that are valid
   */
  protected void set(byte[] bytes, int length)
  {
    this.bytes = bytes;
    this.bytesLength = length;
  }
  
  /** Copies data from a byte array into this value. 
   * 
   * Implemented as call to <code>setCopy(bytes, 0, bytes.length)<code>.
   */ 
  protected void setCopy(byte[] bytes)
  {
    setCopy(bytes, 0, bytes.length);
  }

  /** Copies data from a byte array into this value. 
   *
   * Implemented as call to <code>setCopy(bytes, 0, length)<code>.
   *
   * @param buf a byte array
   * @param length number of bytes to copy
   */ 
  protected void setCopy(byte[] bytes, int length)
  {
    setCopy(bytes, 0, length);
  }

  /** Copies data from a byte array into this value. 
   * 
   * @param buf a byte array
   * @param pos position in byte array
   * @param length number of bytes to copy
   */ 
  protected void setCopy(byte[] bytes, int offset, int length)
  {
    ensureCapacity(length);
    System.arraycopy(bytes, offset, this.bytes, 0, length);
    this.bytesLength = length;
  }
  
  /** Ensures that the internal buffer has at least the provided capacity but neither changes
   * nor increases the valid bytes (the first {@link #size()} bytes).  
   * 
   * @param capacity
   */
  protected void ensureCapacity(int capacity)
  {
    if (capacity > bytes.length)
    {
      byte[] newval = new byte[capacity];
      System.arraycopy(bytes, 0, newval, 0, bytes.length); 
      bytes = newval;
    }
  }
}
