package com.ibm.jaql.json.util;

import java.io.Closeable;

import com.ibm.jaql.json.type.Item;

public class ClosingIter extends Iter
{
  protected Iter iter;
  protected Closeable resource;
  
  public ClosingIter(Iter iter, Closeable resource)
  {
    this.iter = iter;
    this.resource = resource;
  }

  @Override
  public Item next() throws Exception
  {
    Item item = iter.next();
    if( item == null )
    {
      if( resource != null )
      {
        resource.close();
        resource = null;
      }
    }
    return item;
  }
}
