/*
 * Copyright (C) IBM Corp. 2010.
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
package com.ibm.jaql.lang.expr.system;

import com.ibm.jaql.io.ClosableJsonIterator;
import com.ibm.jaql.io.ClosableJsonWriter;
import com.ibm.jaql.io.InputAdapter;
import com.ibm.jaql.io.OutputAdapter;
import com.ibm.jaql.io.serialization.text.TextFullSerializer;
import com.ibm.jaql.io.stream.StreamInputAdapter;
import com.ibm.jaql.io.stream.StreamOutputAdapter;
import com.ibm.jaql.json.parser.JsonParser;
import com.ibm.jaql.json.parser.ParseException;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonBool;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.core.VarMap;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ExprProperty;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.util.JaqlUtil;
import com.ibm.jaql.util.Bool3;
import com.ibm.jaql.util.FastPrinter;
import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class ExternalFunctionCallExpr extends IterExpr {

    private static final JsonString CMD = new JsonString("cmd");
    private static final JsonString WRITEOPTS = new JsonString("writeOpts");
    private static final JsonString READOPTS = new JsonString("readOpts");

    private Process process = null;
    private InputStream stdin = null;
    private OutputStream stdout = null;
    private Throwable error;
    private ClosableJsonWriter writer = null;
    private ClosableJsonIterator reader;

    private JsonValue cmd = null;
    private JsonValue writeOpts = null;
    private JsonValue readOpts = null;
    private JsonValue args = null;
    private JsonIterator data = null;

    private JsonRecord rec = null;
    private JsonString mode = null;

    public ExternalFunctionCallExpr(JsonRecord rec, ArrayList<Expr> exprList) {
        super(exprList);
        this.rec = rec;
        this.mode = (JsonString) rec.get(new JsonString("mode"));

        if (mode == null) {
            throw new IllegalArgumentException(
                    "mode should be specified: push or streaming");
        }

        initParams(rec);
    }

    public ExternalFunctionCallExpr(JsonRecord rec, Expr[] exprs)
            throws InstantiationException, IllegalAccessException {
        super(exprs);
        this.rec = rec;
        this.mode = (JsonString) rec.get(new JsonString("mode"));

        if (mode == null) {
            throw new IllegalArgumentException(
                    "mode should be specified: push or streaming");
        }

        initParams(rec);
    }

    private void initParams(JsonRecord rec) {
        for (Iterator iter = rec.iterator(); iter.hasNext();) {
            Entry tmp = (Entry) iter.next();
            if (tmp.getKey().equals(CMD)) {
                cmd = (JsonValue) tmp.getValue();
            } else if (tmp.getKey().equals(WRITEOPTS)) {
                writeOpts = (JsonValue) tmp.getValue();
            } else if (tmp.getKey().equals(READOPTS)) {
                readOpts = (JsonValue) tmp.getValue();
            }
        }
        if (cmd == null)
            throw new IllegalArgumentException(
                    "cmd cannot be null in externalfn()");
        try {
            if (writeOpts == null) {
                String in = "{outoptions: {adapter: 'com.ibm.jaql.io.stream.StreamOutputAdapter',";
                if (mode.equals(new JsonString("push")))
                    in += "format: 'com.ibm.jaql.io.stream.converter.ArgumentsOutputStream'}}";
                else
                    in += "format: 'com.ibm.jaql.io.stream.converter.LineTextOutputStream'}}";
                writeOpts = new JsonParser().parse(in);
            }
            if (readOpts == null) {
                String out = "{inoptions: {adapter: 'com.ibm.jaql.io.stream.StreamInputAdapter',"
                        + "format: 'com.ibm.jaql.io.stream.converter.LineTextInputStream'}}";
                readOpts = new JsonParser().parse(out);
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.ibm.jaql.lang.expr.core.Expr#decompile(java.io.PrintStream,
     * java.util.HashSet)
     */
    @Override
    public void decompile(FastPrinter exprText, HashSet<Var> capturedVars)
            throws Exception {
        exprText.print("system::externalfn(");
        TextFullSerializer.getDefault().write(exprText, rec);
        exprText.print(")");
        String sep = "( ";
        for (Expr e : exprs) {
            exprText.print(sep);
            e.decompile(exprText, capturedVars);
            sep = ", ";
        }
        exprText.print(" )");
    }

    @Override
    public boolean isMappable(int i) {
        return i == 0
                && ((JsonBool) rec.get(new JsonString("perPartition"))).get() == true;
    }

    /**
     * This expression evaluates all input arguments only once
     */
    @Override
    public Bool3 evaluatesChildOnce(int i) {
        return Bool3.TRUE;
    }

    public Map<ExprProperty, Boolean> getProperties() {
        // NOTE: we should at least be as liberal as built-ins.
        return ExprProperty.createUnsafeDefaults();
    }

    @Override
    public Schema getSchema() {
        return SchemaFactory.anySchema();
    }

    @Override
    public Expr clone(VarMap varMap) {
        try {
            return new ExternalFunctionCallExpr(rec, cloneChildren(varMap));
        } catch (Exception ex) {
            throw new UndeclaredThrowableException(ex);
        }
    }

    @Override
    public JsonIterator iter(Context context) throws Exception {
        try {
            initProcess(context);

            if (mode.equals(new JsonString("push"))) {
                args = exprs[0].eval(context);
                writer.write(args);
                writer.flush();
                return reader;
            } else if (mode.equals(new JsonString("streaming"))) {
                data = exprs[0].iter(context);

                return new ClosableJsonIterator() {
                    boolean firsttime = true;
                    InputHelper helper = new InputHelper();

                    @Override
                    public boolean moveNext() throws Exception {
                        if (firsttime) {
                            helper.start();
                            firsttime = false;
                        } else {
                            helper.stop = false;
                        }
                        if (reader.moveNext()) {
                            helper.stop = true;
                            currentValue = reader.current();
                            return true;
                        }
                        return false;
                    }

                    class InputHelper extends Thread {
                        public boolean stop = false;

                        public void run() {

                            try {
                                while (!stop) {
                                    if (data.moveNext()) {
                                        writer.write(data.current());
                                    } else {
                                        stop = true;
                                        writer.close();
                                        writer = null;
                                    }
                                }
                            } 
                            catch (Throwable e)
                            {
                              e.printStackTrace(); // TODO: move to log
                              if( error == null )
                              {
                                error = e;
                              }
                            }
                        }
                    }
                };
            } else {
              throw new RuntimeException("unsupported mode: "+mode);
            }
        } catch (Throwable e) {
            if (error == null) {
                error = e;
            }
            if (stdin != null) {
                try {
                    stdin.close();
                } catch (Throwable t) { // FIXME: More lost exceptions!
                }
                stdin = null;
            }
            if (process != null) {
              try {
                  process.destroy();
              } catch (Throwable t) {
              }
              process = null;
            }
            if (stdout != null) {
                try {
                    stdout.close();
                } catch (Throwable t) {
                }
                stdout = null;
            }
            if (error instanceof Exception) {
                throw (Exception) error;
            }
            throw new UndeclaredThrowableException(error);
        }

    }

    private void initProcess(Context context) throws Exception {
        ProcessBuilder pb = new ProcessBuilder();

        if (cmd instanceof JsonString) {
            String tmp = ((JsonString) cmd).toString();
            String[] cmdArray = tmp.split(" ");
            ArrayList<String> array = new ArrayList<String>();
            for (String s : cmdArray) {
                if (s.length() > 0)
                    array.add(s.trim());
            }
            pb.command(array);
        }

        Configuration cfg = new Configuration();

        File directory = new File(cfg.get("mapred.local.dir", "."));
        pb.directory(directory);

        // unset environment variables that jvm dump to stderr
        pb.environment().remove("_JAVA_OPTIONS");
        pb.environment().remove("JAVA_TOOL_OPTIONS");

        process = pb.start();

        ErrorThread errorThread = new ErrorThread();
        errorThread.start();
        stdin = process.getInputStream();
        stdout = process.getOutputStream();

        OutputAdapter outAdapter = (OutputAdapter) JaqlUtil.getAdapterStore().output
                .getAdapter(writeOpts);
        if (!(outAdapter instanceof StreamOutputAdapter))
            throw new IllegalArgumentException(
                    "The adapter of writeOpts must be an instance of com.ibm.jaql.io.stream.StreamOutputAdapter");
        ((StreamOutputAdapter) outAdapter).setDefaultOutput(stdout);
        outAdapter.open();
        writer = outAdapter.getWriter();

        InputAdapter inAdapter = (InputAdapter) JaqlUtil.getAdapterStore().input
                .getAdapter(readOpts);
        if (!(inAdapter instanceof StreamInputAdapter))
            throw new IllegalArgumentException(
                    "The adapter of readOpts must be an instance of com.ibm.jaql.io.stream.ExternalCallStreamInputAdapter");
        ((StreamInputAdapter) inAdapter).setInputStream(stdin);
        inAdapter.open();
        reader = inAdapter.iter();

    }

    private class ErrorThread extends Thread {
        @Override
        public void run() {
            try {
                InputStream is = process.getErrorStream();
                byte[] buffer = new byte[1024];
                int n;
                while ((n = is.read(buffer)) >= 0) {
                    System.err.write(buffer, 0, n);
                }
                System.err.flush();
                is.close();
            } catch (Throwable e) {
                if (error == null) {
                    error = e;
                }
                process.destroy();
            }
        }

    }

}