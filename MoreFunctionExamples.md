## Grep Example ##

The following example is similar to the "grep" command in unix; it returns
the substrings that match a regular expression over a list of input strings:

```
    package com.acme.extensions.fn;

    import java.util.regex.Matcher;
    import java.util.regex.Pattern;

    import com.ibm.jaql.json.util.JIterator;
    import com.ibm.jaql.json.type.JString;


    public class Grep
    {
1     public JIterator eval(JString regex, JIterator jstrs) throws Exception
      {
2       return eval(regex, null, jstrs);
      }

3     public JIterator eval(JString regex, JString flags, final JIterator jstrs) throws Exception
      {
        if( regex == null || jstrs == null )
        {
          return null;
        }

        int f = 0;
        boolean global1 = false;
        if( flags != null )
        {
          for( int i = 0 ; i < flags.getLength() ; i++ )
          {
            switch( flags.charAt(i) )
            {
              case 'g': global1 = true; break;
              case 'm': f |= Pattern.MULTILINE; break;
              case 'i': f |= Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE; break;
              default: throw new IllegalArgumentException("unknown regex flag: "+(char)flags.charAt(i));
            }
          }
        }
        Pattern pattern = Pattern.compile(regex.toString(), f);

        final Matcher matcher = pattern.matcher("");    
        final boolean global = global1;

        final JString resultStr = new JString();

        return new JIterator(resultStr)
        {
          private boolean needInput = true;

          public boolean moveNext() throws Exception
          {
            while( true )
            {
              if( needInput )
              {
                if( ! jstrs.moveNextNonNull() )
                {
                  return false;
                }
                JString jstr = (JString)jstrs.current(); // could raise a cast error
                matcher.reset(jstr.toString());
              }
              if( matcher.find() )
              {
                resultStr.set(matcher.group());
                needInput = ! global;
                return true;
              }
              needInput = true;
            }
          }
        };
      }
    }
```

This example shows that a class may have multiple `eval()`
methods (1,3).  The current implementation supports overloading only
on the number of arguments, not based upon the types of the arguments.
In this case, the two argument function (1) is supplying default
`flags` for the three argument function (3).  The example
also illustrates that the function can take array values using an
`JIterator`.  This allows a function to process a large
array effeciently.  The function is not required to process the entire
array, which might allow the system to avoid computing the entire
array.

```
    registerFunction("grep", "com.acme.extensions.fn.Grep");
    $data = [ "a1bxa2b", "a3bxa4b", "a5bxa6b", null, "a7bxa8b" ];

    grep("a\\d*b", $data);
    // [ "a1b", "a3b", "a5b", "a7b" ]

    grep("a\\d*b", null, $data );
    // [ "a1b", "a3b", "a5b", "a7b" ]

    grep("a\\d*b", "g", $data );
    // [ "a1b", "a2b", "a3b", "a4b", "a5b", "a6b", "a7b", "a8b" ]
```

The first call is to the two argument `eval()` method,
which supplies the a default `null` value, and therefore,
the second call is identical to the first.