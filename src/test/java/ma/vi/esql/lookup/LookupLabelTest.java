package ma.vi.esql.lookup;

import ma.vi.esql.exec.EsqlConnection;
import ma.vi.esql.exec.Result;
import ma.vi.esql.syntax.Parser;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.util.stream.Stream;

import static org.junit.jupiter.api.DynamicTest.dynamicTest;

/**
 * @author Vikash Madhow (vikash.madhow@gmail.com)
 */
class LookupLabelTest extends DataTest {
  @TestFactory
  Stream<DynamicTest> lookupDirectLabel() {
    return Stream.of(databases)
                 .map(db -> dynamicTest(db.target().toString(), () -> {
                   System.out.println(db.target());
                   Parser p = new Parser(db.structure());
                   try (EsqlConnection con = db.esql(db.pooledConnection())) {
                     con.exec("delete t from t:a.b.LkT");
                     con.exec("delete s from s:LkS");
                     con.exec("insert into LkS(_id, a, b, i) values "
                                  + "(newid(), 1, 0, '0115'),"
                                  + "(newid(), 2, 9, '0164'),"
                                  + "(newid(), 3, 8, '0992'),"
                                  + "(newid(), 4, 7, '1063'),"
                                  + "(newid(), 5, 6, '1511'),"
                                  + "(newid(), 6, 5, '2219'),"
                                  + "(newid(), 7, 4, '2434'),"
                                  + "(newid(), 8, 3, '3211'),"
                                  + "(newid(), 9, 2, '4532'),"
                                  + "(newid(), 0, 1, '5811')");

                     Result rs = con.exec("select i, lookuplabel(i, 'TestClass') from LkS order by a");
                     printResult(rs, 20);
                   }
                 }));
  }

  @TestFactory
  Stream<DynamicTest> lookupLinkedLabel() {
    return Stream.of(databases)
                 .map(db -> dynamicTest(db.target().toString(), () -> {
                   System.out.println(db.target());
                   Parser p = new Parser(db.structure());
                   try (EsqlConnection con = db.esql(db.pooledConnection())) {
                     con.exec("delete t from t:a.b.LkT");
                     con.exec("delete s from s:LkS");
                     con.exec("insert into LkS(_id, a, b, i) values "
                                  + "(newid(), 1, 0, '0115'),"
                                  + "(newid(), 2, 9, '0164'),"
                                  + "(newid(), 3, 8, '0992'),"
                                  + "(newid(), 4, 7, '1063'),"
                                  + "(newid(), 5, 6, '1511'),"
                                  + "(newid(), 6, 5, '2219'),"
                                  + "(newid(), 7, 4, '2434'),"
                                  + "(newid(), 8, 3, '3211'),"
                                  + "(newid(), 9, 2, '4532'),"
                                  + "(newid(), 0, 1, '5811')");

                     Result rs = con.exec("select i, lookuplabel(i, 'TestClass', 'TestGroup') from LkS order by a");
                     printResult(rs, 20);
                   }
                 }));
  }

  @TestFactory
  Stream<DynamicTest> lookupMultipleLinkedLabel() {
    return Stream.of(databases)
                 .map(db -> dynamicTest(db.target().toString(), () -> {
                   System.out.println(db.target());
                   Parser p = new Parser(db.structure());
                   try (EsqlConnection con = db.esql(db.pooledConnection())) {
                     con.exec("delete t from t:a.b.LkT");
                     con.exec("delete s from s:LkS");
                     con.exec("insert into LkS(_id, a, b, i) values "
                                  + "(newid(), 1, 0, '0115'),"
                                  + "(newid(), 2, 9, '0164'),"
                                  + "(newid(), 3, 8, '0992'),"
                                  + "(newid(), 4, 7, '1063'),"
                                  + "(newid(), 5, 6, '1511'),"
                                  + "(newid(), 6, 5, '2219'),"
                                  + "(newid(), 7, 4, '2434'),"
                                  + "(newid(), 8, 3, '3211'),"
                                  + "(newid(), 9, 2, '4532'),"
                                  + "(newid(), 0, 1, '5811')");

                     Result rs = con.exec("select i, lookuplabel(i, 'TestClass', 'TestGroup', 'TestDivision', 'TestSection') from LkS order by a");
                     printResult(rs, 20);
                   }
                 }));
  }

  @TestFactory
  Stream<DynamicTest> lookupMultipleLinkedLabelAllLabels() {
    return Stream.of(databases)
                 .map(db -> dynamicTest(db.target().toString(), () -> {
                   System.out.println(db.target());
                   Parser p = new Parser(db.structure());
                   try (EsqlConnection con = db.esql(db.pooledConnection())) {
                     con.exec("delete t from t:a.b.LkT");
                     con.exec("delete s from s:LkS");
                     con.exec("insert into LkS(_id, a, b, i) values "
                                  + "(newid(), 1, 0, '0115'),"
                                  + "(newid(), 2, 9, '0164'),"
                                  + "(newid(), 3, 8, '0992'),"
                                  + "(newid(), 4, 7, '1063'),"
                                  + "(newid(), 5, 6, '1511'),"
                                  + "(newid(), 6, 5, '2219'),"
                                  + "(newid(), 7, 4, '2434'),"
                                  + "(newid(), 8, 3, '3211'),"
                                  + "(newid(), 9, 2, '4532'),"
                                  + "(newid(), 0, 1, '5811')");

                     Result rs = con.exec("select i, lookuplabel(show_last_only:=false, i, 'TestClass', 'TestGroup', 'TestDivision', 'TestSection') from LkS order by a");
                     printResult(rs, 20);
                   }
                 }));
  }

  @TestFactory
  Stream<DynamicTest> lookupMultipleLinkedLabelAllLabelsFirstToLast() {
    return Stream.of(databases)
                 .map(db -> dynamicTest(db.target().toString(), () -> {
                   System.out.println(db.target());
                   Parser p = new Parser(db.structure());
                   try (EsqlConnection con = db.esql(db.pooledConnection())) {
                     con.exec("delete t from t:a.b.LkT");
                     con.exec("delete s from s:LkS");
                     con.exec("insert into LkS(_id, a, b, i) values "
                                  + "(newid(), 1, 0, '0115'),"
                                  + "(newid(), 2, 9, '0164'),"
                                  + "(newid(), 3, 8, '0992'),"
                                  + "(newid(), 4, 7, '1063'),"
                                  + "(newid(), 5, 6, '1511'),"
                                  + "(newid(), 6, 5, '2219'),"
                                  + "(newid(), 7, 4, '2434'),"
                                  + "(newid(), 8, 3, '3211'),"
                                  + "(newid(), 9, 2, '4532'),"
                                  + "(newid(), 0, 1, '5811')");

                     Result rs = con.exec(
                         "select i, " +
                             "lookuplabel(show_last_only:=false, last_to_first:=false, show_code:=true, show_text:=false," +
                             "            i, 'TestClass', 'TestGroup', 'TestDivision', 'TestSection') " +
                             "from LkS order by a");
                     printResult(rs, 20);
                   }
                 }));
  }
}