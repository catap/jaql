package com.ibm.jaql.lang.expr.function;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;

import com.ibm.jaql.json.type.JsonBool;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.util.ClassLoaderMgr;

/** 
 * Add to the class path with relative paths resolved using a search path.
 * 
 *    addRelativeClassPath('search1:search2:...', 'classpath1:classpath2:...');
 * 
 *  @see AddClassPathFn for a description of the class path
 *  
 *  If the class path item is an absolute path, then the search path is not used.
 *  Else, the search path is searched from first to last to find the first item
 *  that matches the class path pattern.  
 *  
 *  If the class path item cannot be found resolved, it is silently ignored.
 */
public class AddRelativeClassPathFn extends Expr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par22
  {
    public Descriptor()
    {
      super("addRelativeClassPath", AddRelativeClassPathFn.class);
    }    
  }
  
  public AddRelativeClassPathFn(Expr ... exprs)
  {
    super(exprs);
  }

  @Override
  public JsonValue eval(Context context) throws Exception 
  {
    String[] searchPath = splitPath( (JsonString)exprs[0].eval(context) );
    String[] classPath = splitPath( (JsonString)exprs[1].eval(context) );
    JarFilter jarFilter = new JarFilter();
    ArrayList<String> result = new ArrayList<String>();
    
    for( String cp: classPath )
    {
      // remove the wildcard jar marker
      String c = cp;
      boolean isWild = c.endsWith("*");
      if( isWild  )
      {
        int n = c.endsWith("/*") ? 2 : 1;
        c = c.substring(0, c.length() - n);
      }
      
      // If c is not an absolute path, find c in the search path
      File f = new File(c);
      if( ! f.isAbsolute() )
      {
        for( String sp: searchPath )
        {
          f = new File( sp + File.separator + c );
          if( f.exists() && (!isWild || f.isDirectory()) )
          {
            break;
          }
        }
      }
      
      // Resolve the file(s)
      if( f.exists() && (!isWild || f.isDirectory()) )
      {
        if( isWild )
        {
          for( File j: f.listFiles(jarFilter) )
          {
            result.add( j.getAbsolutePath() );
          }
        }
        else
        {
          result.add( f.getAbsolutePath() );
        }
      }
      else
      {
        System.err.println("ignoring unresolved class path "+cp);
      }
    }
    
    ClassLoaderMgr.addExtensionJars(result.toArray(new String[result.size()]));
    return JsonBool.TRUE;
  }
  
  public static String[] splitPath(JsonString path)
  {
    if( path == null )
    {
      return new String[0];
    }
    return path.toString().split("[:;,]");
  }

  
  static class JarFilter implements FilenameFilter
  {
    @Override
    public boolean accept(File dir, String name)
    {
      if( name.length() > 4 )
      {
        String end = name.substring(name.length() - 4);
        return end.toLowerCase().equals(".jar");
      }
      return false;
    }
  }

}
