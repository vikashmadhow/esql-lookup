package ma.vi.esql.lookup;

import ma.vi.esql.exec.EsqlConnection;
import ma.vi.esql.exec.Result;
import ma.vi.esql.syntax.Parser;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.util.Arrays;
import java.util.Map;
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

                     Result rs = con.exec("select i, label:lookuplabel(i, 'TestClass') from LkS order by i");
                     matchResult(rs,
                                 Arrays.asList(
                                     Map.of("i", "0115", "label", "0115 - Growing of tobacco"),
                                     Map.of("i", "0164", "label", "0164 - Seed processing for propagation"),
                                     Map.of("i", "0992", "label", "0992 - Research & training and service activities incidental to mining of minerals"),
                                     Map.of("i", "1063", "label", "1063 - Manufacture of prepared animal feeds"),
                                     Map.of("i", "1511", "label", "1511 - Tanning and dressing of leather; dressing and dyeing of fur"),
                                     Map.of("i", "2219", "label", "2219 - Manufacture of other rubber products e.g. balloons, pipes, transmission belts"),
                                     Map.of("i", "2434", "label", "2434 - Other casting of metals"),
                                     Map.of("i", "3211", "label", "3211 - Diamond cutting and processing"),
                                     Map.of("i", "4532", "label", "4532 - Sale of new parts and accessories"),
                                     Map.of("i", "5811", "label", "5811 - Publishing of books")));
//                     printResult(rs, 20);
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

                     Result rs = con.exec("select i, label:lookuplabel(i, 'TestClass', 'TestGroup') from LkS order by i");
                     matchResult(rs,
                                 Arrays.asList(
                                    Map.of("i", "0115", "label", "011 - Growing of non-perennial crops"),
                                    Map.of("i", "0164", "label", "016 - Support activities to agriculture"),
                                    Map.of("i", "0992", "label", "099 - Support activities for other mining and quarrying"),
                                    Map.of("i", "1063", "label", "106 - Manufacture of grain mill products, starches and starch products, and prepared animal feeds"),
                                    Map.of("i", "1511", "label", "151 - Tanning and dressing of leather; manufacture of luggage, handbags, saddlery and harness; dressing and dyeing of fur"),
                                    Map.of("i", "2219", "label", "221 - Manufacture of rubber products"),
                                    Map.of("i", "2434", "label", "243 - Casting of metals"),
                                    Map.of("i", "3211", "label", "321 - Manufacture of jewellery, bijouterie and related articles"),
                                    Map.of("i", "4532", "label", "453 - Sale of motor vehicles parts and accessories"),
                                    Map.of("i", "5811", "label", "581 - Publishing of books, periodicals and other publishing activities")));
//                     printResult(rs, 20);
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

                     Result rs = con.exec("select i, label:lookuplabel(i, 'TestClass', 'TestGroup', 'TestDivision', 'TestSection') from LkS order by i");
                     matchResult(rs,
                                 Arrays.asList(
                                    Map.of("i", "0115", "label", "A - Agriculture forestry and fishing"),
                                    Map.of("i", "0164", "label", "A - Agriculture forestry and fishing"),
                                    Map.of("i", "0992", "label", "B - Mining and quarrying"),
                                    Map.of("i", "1063", "label", "C - Manufacturing"),
                                    Map.of("i", "1511", "label", "C - Manufacturing"),
                                    Map.of("i", "2219", "label", "C - Manufacturing"),
                                    Map.of("i", "2434", "label", "C - Manufacturing"),
                                    Map.of("i", "3211", "label", "C - Manufacturing"),
                                    Map.of("i", "4532", "label", "G - Wholesale and retail trade; repair of motor vehicles and motorcycles"),
                                    Map.of("i", "5811", "label", "J - Information and communication")));
//                     printResult(rs, 20);
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

                     Result rs = con.exec("select i, label:lookuplabel(show_last_only=false, i, 'TestClass', 'TestGroup', 'TestDivision', 'TestSection') from LkS order by i");
                     matchResult(rs,
                                 Arrays.asList(
                                    Map.of("i", "0115", "label", "A - Agriculture forestry and fishing / 01 - Crop and animal production, hunting and related service activities / 011 - Growing of non-perennial crops / 0115 - Growing of tobacco"),
                                    Map.of("i", "0164", "label", "A - Agriculture forestry and fishing / 01 - Crop and animal production, hunting and related service activities / 016 - Support activities to agriculture / 0164 - Seed processing for propagation"),
                                    Map.of("i", "0992", "label", "B - Mining and quarrying / 09 - Mining support service activities / 099 - Support activities for other mining and quarrying / 0992 - Research & training and service activities incidental to mining of minerals"),
                                    Map.of("i", "1063", "label", "C - Manufacturing / 10 - Manufacture of food products / 106 - Manufacture of grain mill products, starches and starch products, and prepared animal feeds / 1063 - Manufacture of prepared animal feeds"),
                                    Map.of("i", "1511", "label", "C - Manufacturing / 15 - Manufacture of leather and related products / 151 - Tanning and dressing of leather; manufacture of luggage, handbags, saddlery and harness; dressing and dyeing of fur / 1511 - Tanning and dressing of leather; dressing and dyeing of fur"),
                                    Map.of("i", "2219", "label", "C - Manufacturing / 22 - Manufacture of rubber and plastic products / 221 - Manufacture of rubber products / 2219 - Manufacture of other rubber products e.g. balloons, pipes, transmission belts"),
                                    Map.of("i", "2434", "label", "C - Manufacturing / 24 - Manufacturing of basic metals / 243 - Casting of metals / 2434 - Other casting of metals"),
                                    Map.of("i", "3211", "label", "C - Manufacturing / 32 - Other manufacturing / 321 - Manufacture of jewellery, bijouterie and related articles / 3211 - Diamond cutting and processing"),
                                    Map.of("i", "4532", "label", "G - Wholesale and retail trade; repair of motor vehicles and motorcycles / 45 - Wholesale and retail trade and repair of motor vehicles and motorcycles / 453 - Sale of motor vehicles parts and accessories / 4532 - Sale of new parts and accessories"),
                                    Map.of("i", "5811", "label", "J - Information and communication / 58 - Publishing activities / 581 - Publishing of books, periodicals and other publishing activities / 5811 - Publishing of books")));
//                     printResult(rs, 20);
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
                             "label:lookuplabel(show_last_only=false, last_to_first=false, show_code=true, show_text=false," +
                             "            i, 'TestClass', 'TestGroup', 'TestDivision', 'TestSection') " +
                             "from LkS order by i");
                     matchResult(rs,
                                 Arrays.asList(
                                    Map.of("i", "0115", "label", "0115 / 011 / 01 / A"),
                                    Map.of("i", "0164", "label", "0164 / 016 / 01 / A"),
                                    Map.of("i", "0992", "label", "0992 / 099 / 09 / B"),
                                    Map.of("i", "1063", "label", "1063 / 106 / 10 / C"),
                                    Map.of("i", "1511", "label", "1511 / 151 / 15 / C"),
                                    Map.of("i", "2219", "label", "2219 / 221 / 22 / C"),
                                    Map.of("i", "2434", "label", "2434 / 243 / 24 / C"),
                                    Map.of("i", "3211", "label", "3211 / 321 / 32 / C"),
                                    Map.of("i", "4532", "label", "4532 / 453 / 45 / G"),
                                    Map.of("i", "5811", "label", "5811 / 581 / 58 / J")));
//                     printResult(rs, 20);
                   }
                 }));
  }
}