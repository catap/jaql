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

import java.lang.reflect.Method;

/**
 * 
 */
public abstract class MetaGetter extends MetaAccessor
{
  protected Method getter;
  protected Method setter;

  /**
   * @param name
   * @param getter
   * @param setter
   */
  public MetaGetter(String name, Method getter, Method setter)
  {
    super(name);
    this.getter = getter;
    this.setter = setter;
  }

  /**
   * @param name
   * @param getter
   * @param setter
   * @return
   */
  public static MetaGetter make(String name, Method getter, Method setter)
  {
    getter.setAccessible(true);
    setter.setAccessible(true);
    Class<?> cls = getter.getReturnType();
    assert getter.getParameterTypes().length == 0;
    assert setter.getParameterTypes().length == 1;
    assert cls == setter.getParameterTypes()[0];
    if (cls.isPrimitive())
    {
      String type = cls.getName();
      switch (type.charAt(0))
      {
        case 'b' : // boolean, byte
          if (type.charAt(1) == 'o')
            return new BooleanMetaGetter(name, getter, setter);
          else
            return new ByteMetaGetter(name, getter, setter);
        case 'c' :
          return new CharMetaGetter(name, getter, setter);
        case 'i' :
          return new IntMetaGetter(name, getter, setter);
        case 'l' :
          return new LongMetaGetter(name, getter, setter);
        case 's' :
          return new ShortMetaGetter(name, getter, setter);
        case 'd' :
          return new DoubleMetaGetter(name, getter, setter);
        case 'f' :
          return new FloatMetaGetter(name, getter, setter);
        default :
          throw new RuntimeException("unknown primitive type: " + type);
      }
    }
    else if (cls.isArray())
    {
      return new ArrayMetaGetter(name, getter, setter);
    }
    else if (cls == String.class)
    {
      return new StringMetaGetter(name, getter, setter);
    }
    else
    {
      throw new RuntimeException("class values NYI");
    }

  }
}
