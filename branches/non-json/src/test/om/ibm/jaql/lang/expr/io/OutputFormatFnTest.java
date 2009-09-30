package om.ibm.jaql.lang.expr.io;

import org.junit.Test;

import com.ibm.jaql.AbstractLoggableTest;
import com.ibm.jaql.io.stream.converter.ArrayJsonTextOutputStream;
import com.ibm.jaql.io.stream.converter.LinesJsonTextOutputStream;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.Jaql;

public class OutputFormatFnTest extends AbstractLoggableTest {
  private static final String JSON = "{format: '"
      + ArrayJsonTextOutputStream.class.getName()
      + "', converter: 'com.ibm.jaql.io.stream.converter.ToDelConverter'}";
  private static final String CSV = "{format: '"
      + LinesJsonTextOutputStream.class.getName()
      + "', converter: 'com.ibm.jaql.io.stream.converter.ToDelConverter'}";
  private static final String XML = "{format: '"
      + LinesJsonTextOutputStream.class.getName()
      + "', converter: 'com.ibm.jaql.io.stream.converter.ToXmlConverter'}";

  @Test
  public void xml() throws Exception {
    work("{root: {content: [1,2,3]}}->write(stdout(" + XML + "));");
    work("{root: {content: [1, 2, 3]}}->write(stdout(xmlFormat()));");
  }

  @Test
  public void csv() throws Exception {
    work("[[1,2],[3,4]]->write(stdout(" + CSV + "));");
    work("[[100,200], [300,400]]->write(stdout(csvFormat()));");
  }

  @Test
  public void stdout() throws Exception {
    work("[1,2,3]->write(stdout());");
  }

  @Test
  public void descriptor() throws Exception {
    work("stdout();");
    work("stdout(" + JSON + ");");
    work("stdout(" + CSV + ");");
  }

  @Test
  public void predefined() throws Exception {
    work("csvFormat();");
    work("xmlFormat();");
  }

  private void work(String s) throws Exception {
    System.out.println(s);
    Jaql jaql = new Jaql(s);
    JsonValue jv = jaql.evalNext();
    debug(jv);
  }
}
