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
package com.ibm.jaql.json.meta;

import java.lang.reflect.Field;
import java.util.Collection;

/**
 * 
 */
public abstract class MetaField extends MetaAccessor
{
  protected Field field;

  /**
   * @param name
   * @param field
   */
  protected MetaField(String name, Field field)
  {
    super(name);
    this.field = field;
  }

  /**
   * @param name
   * @param field
   * @return
   */
  public static MetaField make(String name, Field field)
  {
    field.setAccessible(true);
    Class<?> cls = field.getType();
    if (cls.isPrimitive())
    {
      String type = cls.getName();
      switch (type.charAt(0))
      {
        case 'b' : // boolean, byte
          if (type.charAt(1) == 'o')
            return new BooleanMetaField(name, field);
          else
            return new ByteMetaField(name, field);
        case 'c' :
          return new CharMetaField(name, field);
        case 'i' :
          return new IntMetaField(name, field);
        case 'l' :
          return new LongMetaField(name, field);
        case 's' :
          return new ShortMetaField(name, field);
        case 'd' :
          return new DoubleMetaField(name, field);
        case 'f' :
          return new FloatMetaField(name, field);
        default :
          throw new RuntimeException("unknown primitive type: " + name);
      }
    }
    else if (cls == String.class)
    {
      return new StringMetaField(name, field);
    }
    else if (cls.isArray())
    {
      return new ArrayMetaField(name, field);
    }
    else if (Collection.class.isAssignableFrom(cls))
    {
      throw new RuntimeException("arrays NYI");
    }
    else
    {
      // TODO: any other classes that should be arrays or atoms?
      // TODO: this should probably use a generic meta factory...
      return new RecordMetaField(name, field);
    }
  }
}
