package com.ibm.jaql.lang.expr.del;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.type.JsonBool;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonSchema;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
/**
 * Parsing options for the conversion between JSON and del (delimited) from an
 * JSON record.
 * 
 * <ul>
 * <li><code>schema</code>: JSON schema. Valid values are <code>null</code>, an
 * array schema, or a record schema.</li>
 * <li><code>delimiter</code>: An ASCII character for separating a line into
 * delimited fields. The default value is comma (<code>,<code>).</li>
 * <li><code>quoted</code>: If <code>true</code>, delimited fields are
 * surrounded with double quotes. The default value is <code>true</code>.</li>
 * <li><code>ddquote</code>: If <code>true</code>, use another double quote to
 * escape a double quote. Otherwise, use bachslash to escape a double quote.
 * Only effect if <code>quoted</code> is <code>true</code>. The default value is
 * <code>true</code>.</li>
 * <li><code>escape</code>: If <code>true</code>, handle 2-character escape
 * sequences and 6-character escape sequences. Only in effect if
 * <code>quoted</code> is true. The default value is <code>true</code>.</li>
 * </ul>
 */
public class DelOptionParser {

  public static final JsonString DELIMITER_NAME = new JsonString("delimiter");
  public static final byte DELIMITER_DEFAULT = ',';
  public static final JsonString QUOTED_NAME = new JsonString("quoted");
  public static final boolean QUOTED_DEFAULT = true;
  public static final JsonString DDQUOTE_NAME = new JsonString("ddquote");
  public static final boolean DDQUOTE_DEFAULT = true;
  public static final JsonString SCHEMA_NAME = new JsonString("schema");
  public static final JsonString ESCAPE_NAME = new JsonString("escape");
  public static final boolean ESCAPE_DEFAULT = true;

  private Schema schema;
  private byte delimiter;
  private boolean quoted;
  private boolean ddquote;
  private boolean escape;

  public void handle(JsonRecord options) {
    // TODO: better error reporting when handling arguments
    if (options == null)
      options = JsonRecord.EMPTY;

    delimiter = DELIMITER_DEFAULT;
    if (options.containsKey(DELIMITER_NAME)) {
      delimiter = getCharacter(options, DELIMITER_NAME, false);
    }

    quoted = QUOTED_DEFAULT;
    if (options.containsKey(QUOTED_NAME)) {
      JsonValue value = options.get(QUOTED_NAME);
      if (value == null) {
        throw new IllegalArgumentException("parameter \"" + QUOTED_NAME
            + "\" must not be null");
      }
      if (!(value instanceof JsonBool)) {
        throw new IllegalArgumentException("parameter \"" + QUOTED_NAME
            + "\" must be boolean");
      }
      quoted = ((JsonBool) value).get();
    }

    // / escape is valid only if quoted is true
    if (quoted) {
      ddquote = DDQUOTE_DEFAULT;
      if (options.containsKey(DDQUOTE_NAME)) {
        JsonValue value = options.get(DDQUOTE_NAME);
        if (value == null) {
          throw new IllegalArgumentException("parameter \"" + DDQUOTE_NAME
              + "\" must not be null");
        }
        if (!(value instanceof JsonBool)) {
          throw new IllegalArgumentException("parameter \"" + DDQUOTE_NAME
              + "\" must be boolean");
        }
        ddquote = ((JsonBool) value).get();
      }

      escape = ESCAPE_DEFAULT;
      if (options.containsKey(ESCAPE_NAME)) {
        JsonValue value = options.get(ESCAPE_NAME);
        if (value == null) {
          throw new IllegalArgumentException("parameter \"" + ESCAPE_NAME
              + "\" must not be null");
        }
        if (!(value instanceof JsonBool)) {
          throw new IllegalArgumentException("parameter \"" + ESCAPE_NAME
              + "\" must be boolean");
        }
        escape = ((JsonBool) value).get();
      }
    }

    JsonValue arg;
    arg = options.get(SCHEMA_NAME);
    if (arg != null && !(arg instanceof JsonSchema)) {
      throw new IllegalArgumentException("parameter \"" + SCHEMA_NAME
          + "\" must be a schema");
    }
    schema = arg != null ? ((JsonSchema) arg).get() : null;

    // TODO: remove check for deprecated options
    if (options.containsKey(new JsonString("convert"))) {
      throw new IllegalArgumentException("The \"convert\" option is deprecated. Use the \"schema\" option instead.");
    }
    if (options.containsKey(new JsonString("fields"))) {
      throw new IllegalArgumentException("The \"fields\" option is deprecated. Use the \"schema\" option instead.");
    }
  }

  /**
   * Checks whether <code>arg</code> consists of a single character and returns
   * it, if so. Otherwise, fails with an exception. If <code>allowNull</code> is
   * set, returns <code>null</code> on <code>null</code> input, otherwise fails
   * on <code>null</code> input.
   */
  private final Byte getCharacter(JsonRecord record,
                                  JsonString name,
                                  boolean allowNull) {
    JsonValue arg = record.get(name);

    // check for null
    if (arg == null) {
      if (allowNull)
        return null;
      throw new IllegalArgumentException("parameter \"" + name.toString()
          + "\" must not be null");
    }

    // check for type and length
    if (!(arg instanceof JsonString)) {
      throw new IllegalArgumentException("parameter \"" + name.toString()
          + "\" must be a string");
    }
    JsonString s = (JsonString) arg;
    if (s.bytesLength() != 1) {
      throw new RuntimeException("parameter \"" + name.toString()
          + "\" must be consist of a single character");
    }

    // return it
    return s.get(0);
  }

  public byte getDelimiter() {
    return delimiter;
  }

  public boolean getQuoted() {
    return quoted;
  }

  public boolean getEscape() {
    return escape;
  }

  public boolean getDdquote() {
    return ddquote;
  }

  public Schema getSchema() {
    return schema;
  }
}
