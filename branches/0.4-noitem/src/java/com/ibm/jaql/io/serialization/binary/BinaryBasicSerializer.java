package com.ibm.jaql.io.serialization.binary;

import java.io.DataInput;
import java.io.DataOutput;

import com.ibm.jaql.io.serialization.BasicSerializer;
import com.ibm.jaql.json.type.JsonValue;

/** Basic serializer for binary data.
 * 
 * @param <T> type of value to work on
 */
public abstract class BinaryBasicSerializer<T extends JsonValue> 
extends BasicSerializer<DataInput, DataOutput, T>
{
}
