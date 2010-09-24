package com.ibm.jaql.lang.expr.core;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import com.ibm.jaql.io.hadoop.Globals;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

/** return the absolute path in HDFS*/
public class GetHdfsPathFn extends Expr {

    public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11 {

        public Descriptor() {
            super("getHdfsPath", GetHdfsPathFn.class);
        }    
    }
    
    public GetHdfsPathFn(Expr...exprs) {
        super(exprs);
    }
    
//    @Override
//    public Map<ExprProperty, Boolean> getProperties() {
//        Map<ExprProperty, Boolean> result = super.getProperties();
//        result.put(ExprProperty.READS_EXTERNAL_DATA, true);
//        return result;
//    }
    
    @Override
    public JsonString eval(Context context) throws Exception {
        JsonString inpath = (JsonString)exprs[0].eval(context);
        String outpath;
       
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        
        Path path = new Path(inpath.toString());
        
        if (path.isAbsolute()) {       
            String uri = fs.getUri().toString();
            outpath = uri + inpath;
        } else {
            String home = fs.getHomeDirectory().toString();           
            outpath = home + "/" + inpath;
        }
        if (!fs.exists(new Path(outpath))) {
            throw new IllegalArgumentException ("The input path doesn't exist in HDFS");
        }
                
        return new JsonString(outpath); 
    }

}
