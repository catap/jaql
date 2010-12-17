//TODO: fix or delete file
///*
// * Copyright (C) IBM Corp. 2009.
// * 
// * Licensed under the Apache License, Version 2.0 (the "License"); you may not
// * use this file except in compliance with the License. You may obtain a copy of
// * the License at
// * 
// * http://www.apache.org/licenses/LICENSE-2.0
// * 
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// * License for the specific language governing permissions and limitations under
// * the License.
// */
//package com.ibm.jaql.lang.expr.core;
//import java.util.ArrayList;
//
//import com.ibm.jaql.json.type.Item;
//import com.ibm.jaql.json.type.MemoryJRecord;
//import com.ibm.jaql.json.util.Iter;
//import com.ibm.jaql.lang.core.Context;
//
///**
// * taggedMerge $x with $k = e<$x>, $y with $k = e<$y>
// * ==
// * merge ($x -> map each $x { x: $x, k: e<$x> }),
// *       ($y -> map each $y { y: $y, k: e<$y> })
// * 
// */
//public class TaggedMergeExpr extends IterExpr
//{
//
//  /**
//   * Binding[] inputs
//   *   each input has var and expr[0]  ($x <- pipe)
//   *   and optionally var2 and expr[1] ($k = e<$x>)
//   * @param inputs
//   */
//  public TaggedMergeExpr(Expr[] inputs)
//  {
//    super(inputs);
//  }
//  
//  public TaggedMergeExpr(ArrayList<BindingExpr> inputs)
//  {
//    super(inputs);
//  }
//
//  @Override
//  public Iter iter(final Context context) throws Exception
//  {
//    return new Iter()
//    {
//      int input = 0;
//      Iter iter = Iter.empty;
//      BindingExpr binding;
//      MemoryJRecord rec = new MemoryJRecord();
//      Item result = new Item(rec);
//      int field;
//      int field2;
//      
//      @Override
//      public Item next() throws Exception
//      {
//        while( true )
//        {
//          Item item = iter.next();
//          if( item != null )
//          {
//            rec.set(field, item);
//            if( field2 >= 0 )
//            {
//              context.setVar(binding.var, item);
//              item = binding.onExpr().eval(context);
//              rec.set(field2, item);
//            }
//            return result;
//          }
//          if( input >= exprs.length )
//          {
//            return null;
//          }
//          
//          binding = (BindingExpr)exprs[input++];
//
//          rec.clear();
//          String name = binding.var.nameAsField();  
//          rec.add(name, Item.nil);
//          if( binding.var2 != null )
//          {
//            String name2 = binding.var2.nameAsField();
//            rec.add(name2, Item.nil);
//            field2 = rec.findName(name2);
//          }
//          else
//          {
//            field2 = -1;
//          }
//          field = rec.findName(name);
//          
//          iter = binding.inExpr().iter(context);
//        }
//      }
//    };
//  }
//
//}
