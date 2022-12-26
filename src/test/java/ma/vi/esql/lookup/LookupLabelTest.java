package ma.vi.esql.lookup;

import ma.vi.esql.database.EsqlConnection;
import ma.vi.esql.exec.Result;
import ma.vi.esql.syntax.Parser;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
                   try (EsqlConnection con = db.esql()) {
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
                                     Map.of("i", "0115", "label", "Growing of tobacco"),
                                     Map.of("i", "0164", "label", "Seed processing for propagation"),
                                     Map.of("i", "0992", "label", "Research & training and service activities incidental to mining of minerals"),
                                     Map.of("i", "1063", "label", "Manufacture of prepared animal feeds"),
                                     Map.of("i", "1511", "label", "Tanning and dressing of leather; dressing and dyeing of fur"),
                                     Map.of("i", "2219", "label", "Manufacture of other rubber products e.g. balloons, pipes, transmission belts"),
                                     Map.of("i", "2434", "label", "Other casting of metals"),
                                     Map.of("i", "3211", "label", "Diamond cutting and processing"),
                                     Map.of("i", "4532", "label", "Sale of new parts and accessories"),
                                     Map.of("i", "5811", "label", "Publishing of books")));
//                     matchResult(rs,
//                                 Arrays.asList(
//                                     Map.of("i", "0115", "label", "0115 - Growing of tobacco"),
//                                     Map.of("i", "0164", "label", "0164 - Seed processing for propagation"),
//                                     Map.of("i", "0992", "label", "0992 - Research & training and service activities incidental to mining of minerals"),
//                                     Map.of("i", "1063", "label", "1063 - Manufacture of prepared animal feeds"),
//                                     Map.of("i", "1511", "label", "1511 - Tanning and dressing of leather; dressing and dyeing of fur"),
//                                     Map.of("i", "2219", "label", "2219 - Manufacture of other rubber products e.g. balloons, pipes, transmission belts"),
//                                     Map.of("i", "2434", "label", "2434 - Other casting of metals"),
//                                     Map.of("i", "3211", "label", "3211 - Diamond cutting and processing"),
//                                     Map.of("i", "4532", "label", "4532 - Sale of new parts and accessories"),
//                                     Map.of("i", "5811", "label", "5811 - Publishing of books")));
//                     printResult(rs, 20);
                   }
                 }));
  }

  @TestFactory
  Stream<DynamicTest> execLookupLabel() {
    return Stream.of(databases)
                 .map(db -> dynamicTest(db.target().toString(), () -> {
                   System.out.println(db.target());
                   Parser p = new Parser(db.structure());
                   try (EsqlConnection con = db.esql()) {
                     Result rs = con.exec("lookuplabel('0115', 'TestClass')");
                     rs.toNext();
                     assertEquals("Growing of tobacco", rs.value(1));
//                     assertEquals("0115 - Growing of tobacco", rs.value(1));
                   }
                 }));
  }

  @TestFactory
  Stream<DynamicTest> lookupLinkedLabel() {
    return Stream.of(databases)
                 .map(db -> dynamicTest(db.target().toString(), () -> {
                   System.out.println(db.target());
                   Parser p = new Parser(db.structure());
                   try (EsqlConnection con = db.esql()) {
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
                                    Map.of("i", "0115", "label", "Growing of non-perennial crops"),
                                    Map.of("i", "0164", "label", "Support activities to agriculture"),
                                    Map.of("i", "0992", "label", "Support activities for other mining and quarrying"),
                                    Map.of("i", "1063", "label", "Manufacture of grain mill products, starches and starch products, and prepared animal feeds"),
                                    Map.of("i", "1511", "label", "Tanning and dressing of leather; manufacture of luggage, handbags, saddlery and harness; dressing and dyeing of fur"),
                                    Map.of("i", "2219", "label", "Manufacture of rubber products"),
                                    Map.of("i", "2434", "label", "Casting of metals"),
                                    Map.of("i", "3211", "label", "Manufacture of jewellery, bijouterie and related articles"),
                                    Map.of("i", "4532", "label", "Sale of motor vehicles parts and accessories"),
                                    Map.of("i", "5811", "label", "Publishing of books, periodicals and other publishing activities")));
//                     matchResult(rs,
//                                 Arrays.asList(
//                                    Map.of("i", "0115", "label", "011 - Growing of non-perennial crops"),
//                                    Map.of("i", "0164", "label", "016 - Support activities to agriculture"),
//                                    Map.of("i", "0992", "label", "099 - Support activities for other mining and quarrying"),
//                                    Map.of("i", "1063", "label", "106 - Manufacture of grain mill products, starches and starch products, and prepared animal feeds"),
//                                    Map.of("i", "1511", "label", "151 - Tanning and dressing of leather; manufacture of luggage, handbags, saddlery and harness; dressing and dyeing of fur"),
//                                    Map.of("i", "2219", "label", "221 - Manufacture of rubber products"),
//                                    Map.of("i", "2434", "label", "243 - Casting of metals"),
//                                    Map.of("i", "3211", "label", "321 - Manufacture of jewellery, bijouterie and related articles"),
//                                    Map.of("i", "4532", "label", "453 - Sale of motor vehicles parts and accessories"),
//                                    Map.of("i", "5811", "label", "581 - Publishing of books, periodicals and other publishing activities")));
//                     printResult(rs, 20);
                   }
                 }));
  }

  @TestFactory
  Stream<DynamicTest> execLookupLinkedLabel() {
    return Stream.of(databases)
                 .map(db -> dynamicTest(db.target().toString(), () -> {
                   System.out.println(db.target());
                   Parser p = new Parser(db.structure());
                   try (EsqlConnection con = db.esql()) {
                     Result rs = con.exec("lookuplabel('0115', 'TestClass', 'TestGroup')");
                     rs.toNext();
                     assertEquals("Growing of non-perennial crops", rs.value(1));
//                     assertEquals("011 - Growing of non-perennial crops", rs.value(1));
                   }
                 }));
  }

  @TestFactory
  Stream<DynamicTest> execLookupMultipleLinkedLabel() {
    return Stream.of(databases)
                 .map(db -> dynamicTest(db.target().toString(), () -> {
                   System.out.println(db.target());
                   Parser p = new Parser(db.structure());
                   try (EsqlConnection con = db.esql()) {
                     Result rs = con.exec("lookuplabel('0115', 'TestClass', 'TestGroup', 'TestDivision', 'TestSection')");
                     rs.toNext();
                     assertEquals("Agriculture forestry and fishing", rs.value(1));
//                     assertEquals("A - Agriculture forestry and fishing", rs.value(1));
                   }
                 }));
  }

  @TestFactory
  Stream<DynamicTest> searchByLinkedCode() {
    return Stream.of(databases)
                 .map(db -> dynamicTest(db.target().toString(), () -> {
                   System.out.println(db.target());
                   Parser p = new Parser(db.structure());
                   try (EsqlConnection con = db.esql()) {
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

                     Result rs = con.exec("""
                                          select a, b, i
                                            from LkS
                                           where lookuplabel(i, show_code=true, show_label=false, 'TestClass', 'TestGroup', 'TestDivision', 'TestSection')='A'
                                           order by a""");
                     matchResult(rs, Arrays.asList(Map.of("a", "1", "b", 0, "i", "0115"),
                                                   Map.of("a", "2", "b", 9, "i", "0164")));
                     // printResult(rs, 20);
                     // rs.toNext();
                     // assertEquals("Agriculture forestry and fishing", rs.value(1));
//                     assertEquals("A - Agriculture forestry and fishing", rs.value(1));
                   }
                 }));
  }

  @TestFactory
  Stream<DynamicTest> lookupMultipleLinkedLabel() {
    return Stream.of(databases)
                 .map(db -> dynamicTest(db.target().toString(), () -> {
                   System.out.println(db.target());
                   Parser p = new Parser(db.structure());
                   try (EsqlConnection con = db.esql()) {
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
                                    Map.of("i", "0115", "label", "Agriculture forestry and fishing"),
                                    Map.of("i", "0164", "label", "Agriculture forestry and fishing"),
                                    Map.of("i", "0992", "label", "Mining and quarrying"),
                                    Map.of("i", "1063", "label", "Manufacturing"),
                                    Map.of("i", "1511", "label", "Manufacturing"),
                                    Map.of("i", "2219", "label", "Manufacturing"),
                                    Map.of("i", "2434", "label", "Manufacturing"),
                                    Map.of("i", "3211", "label", "Manufacturing"),
                                    Map.of("i", "4532", "label", "Wholesale and retail trade; repair of motor vehicles and motorcycles"),
                                    Map.of("i", "5811", "label", "Information and communication")));
//                     matchResult(rs,
//                                 Arrays.asList(
//                                    Map.of("i", "0115", "label", "A - Agriculture forestry and fishing"),
//                                    Map.of("i", "0164", "label", "A - Agriculture forestry and fishing"),
//                                    Map.of("i", "0992", "label", "B - Mining and quarrying"),
//                                    Map.of("i", "1063", "label", "C - Manufacturing"),
//                                    Map.of("i", "1511", "label", "C - Manufacturing"),
//                                    Map.of("i", "2219", "label", "C - Manufacturing"),
//                                    Map.of("i", "2434", "label", "C - Manufacturing"),
//                                    Map.of("i", "3211", "label", "C - Manufacturing"),
//                                    Map.of("i", "4532", "label", "G - Wholesale and retail trade; repair of motor vehicles and motorcycles"),
//                                    Map.of("i", "5811", "label", "J - Information and communication")));
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
                   try (EsqlConnection con = db.esql()) {
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
                                    Map.of("i", "0115", "label", "Agriculture forestry and fishing / Crop and animal production, hunting and related service activities / Growing of non-perennial crops / Growing of tobacco"),
                                    Map.of("i", "0164", "label", "Agriculture forestry and fishing / Crop and animal production, hunting and related service activities / Support activities to agriculture / Seed processing for propagation"),
                                    Map.of("i", "0992", "label", "Mining and quarrying / Mining support service activities / Support activities for other mining and quarrying / Research & training and service activities incidental to mining of minerals"),
                                    Map.of("i", "1063", "label", "Manufacturing / Manufacture of food products / Manufacture of grain mill products, starches and starch products, and prepared animal feeds / Manufacture of prepared animal feeds"),
                                    Map.of("i", "1511", "label", "Manufacturing / Manufacture of leather and related products / Tanning and dressing of leather; manufacture of luggage, handbags, saddlery and harness; dressing and dyeing of fur / Tanning and dressing of leather; dressing and dyeing of fur"),
                                    Map.of("i", "2219", "label", "Manufacturing / Manufacture of rubber and plastic products / Manufacture of rubber products / Manufacture of other rubber products e.g. balloons, pipes, transmission belts"),
                                    Map.of("i", "2434", "label", "Manufacturing / Manufacturing of basic metals / Casting of metals / Other casting of metals"),
                                    Map.of("i", "3211", "label", "Manufacturing / Other manufacturing / Manufacture of jewellery, bijouterie and related articles / Diamond cutting and processing"),
                                    Map.of("i", "4532", "label", "Wholesale and retail trade; repair of motor vehicles and motorcycles / Wholesale and retail trade and repair of motor vehicles and motorcycles / Sale of motor vehicles parts and accessories / Sale of new parts and accessories"),
                                    Map.of("i", "5811", "label", "Information and communication / Publishing activities / Publishing of books, periodicals and other publishing activities / Publishing of books")));
//                     matchResult(rs,
//                                 Arrays.asList(
//                                    Map.of("i", "0115", "label", "A - Agriculture forestry and fishing / 01 - Crop and animal production, hunting and related service activities / 011 - Growing of non-perennial crops / 0115 - Growing of tobacco"),
//                                    Map.of("i", "0164", "label", "A - Agriculture forestry and fishing / 01 - Crop and animal production, hunting and related service activities / 016 - Support activities to agriculture / 0164 - Seed processing for propagation"),
//                                    Map.of("i", "0992", "label", "B - Mining and quarrying / 09 - Mining support service activities / 099 - Support activities for other mining and quarrying / 0992 - Research & training and service activities incidental to mining of minerals"),
//                                    Map.of("i", "1063", "label", "C - Manufacturing / 10 - Manufacture of food products / 106 - Manufacture of grain mill products, starches and starch products, and prepared animal feeds / 1063 - Manufacture of prepared animal feeds"),
//                                    Map.of("i", "1511", "label", "C - Manufacturing / 15 - Manufacture of leather and related products / 151 - Tanning and dressing of leather; manufacture of luggage, handbags, saddlery and harness; dressing and dyeing of fur / 1511 - Tanning and dressing of leather; dressing and dyeing of fur"),
//                                    Map.of("i", "2219", "label", "C - Manufacturing / 22 - Manufacture of rubber and plastic products / 221 - Manufacture of rubber products / 2219 - Manufacture of other rubber products e.g. balloons, pipes, transmission belts"),
//                                    Map.of("i", "2434", "label", "C - Manufacturing / 24 - Manufacturing of basic metals / 243 - Casting of metals / 2434 - Other casting of metals"),
//                                    Map.of("i", "3211", "label", "C - Manufacturing / 32 - Other manufacturing / 321 - Manufacture of jewellery, bijouterie and related articles / 3211 - Diamond cutting and processing"),
//                                    Map.of("i", "4532", "label", "G - Wholesale and retail trade; repair of motor vehicles and motorcycles / 45 - Wholesale and retail trade and repair of motor vehicles and motorcycles / 453 - Sale of motor vehicles parts and accessories / 4532 - Sale of new parts and accessories"),
//                                    Map.of("i", "5811", "label", "J - Information and communication / 58 - Publishing activities / 581 - Publishing of books, periodicals and other publishing activities / 5811 - Publishing of books")));
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
                   try (EsqlConnection con = db.esql()) {
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
                             "label:lookuplabel(show_last_only=false, last_to_first=false, show_code=true, show_label=false," +
                             "                  i, 'TestClass', 'TestGroup', 'TestDivision', 'TestSection') " +
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

  @TestFactory
  Stream<DynamicTest> lookupClassCodeTable() {
    return Stream.of(databases)
                 .map(db -> dynamicTest(db.target().toString(), () -> {
                   System.out.println(db.target());
                   try (EsqlConnection con = db.esql()) {
                     Result rs = con.exec("lookuplabel(null, show_code=false, 'TestClass') ");
                     printResult(rs, 20);
                   }
                 }));
  }

  @TestFactory
  Stream<DynamicTest> lookupGroupCodeTableFromClass() {
    return Stream.of(databases)
                 .map(db -> dynamicTest(db.target().toString(), () -> {
                   System.out.println(db.target());
                   try (EsqlConnection con = db.esql()) {
                     Result rs = con.exec("lookuplabel(null, last_to_first=false, show_last_only=false, show_code=false, 'TestClass', 'TestGroup')");
                     printResult(rs, 20);
                   }
                 }));
  }

  @TestFactory
  Stream<DynamicTest> lookupSectionCodeTableFromClass() {
    return Stream.of(databases)
                 .map(db -> dynamicTest(db.target().toString(), () -> {
                   System.out.println(db.target());
                   try (EsqlConnection con = db.esql()) {
                     Result rs = con.exec("lookuplabel(null, show_code=false, 'TestClass', 'TestGroup', 'TestDivision', 'TestSection')");
                     printResult(rs, 20);
                   }
                 }));
  }

  @TestFactory
  Stream<DynamicTest> lookupByAltCode1() {
    return Stream.of(databases)
                 .map(db -> dynamicTest(db.target().toString(), () -> {
                   System.out.println(db.target());
                   try (EsqlConnection con = db.esql()) {
                     Result rs = con.exec("lookuplabel('MUS', show_code=false, match_by='alt_code1', 'Country')");
//                     printResult(rs, 20);
                     rs.toNext();
                     assertEquals("Mauritius", rs.value("label"));
                   }
                 }));
  }

  @TestFactory
  Stream<DynamicTest> lookupByAltCode2() {
    return Stream.of(databases)
                 .map(db -> dynamicTest(db.target().toString(), () -> {
                   System.out.println(db.target());
                   try (EsqlConnection con = db.esql()) {
                     Result rs = con.exec("lookuplabel('480', show_code=false, match_by='alt_code2', 'Country')");
//                     printResult(rs, 20);
                     rs.toNext();
                     assertEquals("Mauritius", rs.value("label"));
                   }
                 }));
  }

  @TestFactory
  Stream<DynamicTest> lookupCountryTableByCode() {
    return Stream.of(databases)
                 .map(db -> dynamicTest(db.target().toString(), () -> {
                   System.out.println(db.target());
                   try (EsqlConnection con = db.esql()) {
                     Result rs = con.exec("lookuplabel(null, 'Country')");
                     printResult(rs, 20);
                   }
                 }));
  }

  @TestFactory
  Stream<DynamicTest> lookupCountryTableByAltCode1() {
    return Stream.of(databases)
                 .map(db -> dynamicTest(db.target().toString(), () -> {
                   System.out.println(db.target());
                   try (EsqlConnection con = db.esql()) {
                     Result rs = con.exec("lookuplabel(null, match_by='alt_code1', 'Country')");
                     printResult(rs, 20);
                   }
                 }));
  }

  @TestFactory
  Stream<DynamicTest> lookupCountryTableByAltCode2() {
    return Stream.of(databases)
                 .map(db -> dynamicTest(db.target().toString(), () -> {
                   System.out.println(db.target());
                   try (EsqlConnection con = db.esql()) {
                     Result rs = con.exec("lookuplabel(null, match_by='alt_code2', 'Country')");
                     printResult(rs, 20);
                   }
                 }));
  }

  @TestFactory
  Stream<DynamicTest> getCodeTable() {
    return Stream.of(databases)
                 .map(db -> dynamicTest(db.target().toString(), () -> {
                   System.out.println(db.target());
                   try (EsqlConnection con = db.esql()) {
                     Result rs = con.exec("lookuplabel(null, 'TestSection')");
                     printResult(rs, 20);
//                     matchResult(rs,
//                                 Arrays.asList(
//                                     Map.of("code", "A", "alt_code1", null, "alt_code2", null, "label", "Agriculture forestry and fishing"),
//                                     Map.of("code", "B", "alt_code1", null, "alt_code2", null, "label", "Mining and quarrying"),
//                                     Map.of("code", "C", "alt_code1", null, "alt_code2", null, "label", "Manufacturing"),
//                                     Map.of("code", "D", "alt_code1", null, "alt_code2", null, "label", "Electricity, gas, steam and air conditioning supply"),
//                                     Map.of("code", "E", "alt_code1", null, "alt_code2", null, "label", "Water supply; sewerage, waste management and remediation activities"),
//                                     Map.of("code", "F", "alt_code1", null, "alt_code2", null, "label", "Construction"),
//                                     Map.of("code", "G", "alt_code1", null, "alt_code2", null, "label", "Wholesale and retail trade; repair of motor vehicles and motorcycles"),
//                                     Map.of("code", "H", "alt_code1", null, "alt_code2", null, "label", "Transportation and storage"),
//                                     Map.of("code", "I", "alt_code1", null, "alt_code2", null, "label", "Accommodation and food service activities"),
//                                     Map.of("code", "J", "alt_code1", null, "alt_code2", null, "label", "Information and communication"),
//                                     Map.of("code", "K", "alt_code1", null, "alt_code2", null, "label", "Financial and insurance activities"),
//                                     Map.of("code", "L", "alt_code1", null, "alt_code2", null, "label", "Real estate activities"),
//                                     Map.of("code", "M", "alt_code1", null, "alt_code2", null, "label", "Professional, scientific and technical activities"),
//                                     Map.of("code", "N", "alt_code1", null, "alt_code2", null, "label", "Administrative and support service activities"),
//                                     Map.of("code", "O", "alt_code1", null, "alt_code2", null, "label", "Public administration and defence; compulsory social security"),
//                                     Map.of("code", "P", "alt_code1", null, "alt_code2", null, "label", "Education"),
//                                     Map.of("code", "Q", "alt_code1", null, "alt_code2", null, "label", "Human health and social work activities"),
//                                     Map.of("code", "R", "alt_code1", null, "alt_code2", null, "label", "Arts, entertainment and recreation"),
//                                     Map.of("code", "S", "alt_code1", null, "alt_code2", null, "label", "Other service activities"),
//                                     Map.of("code", "T", "alt_code1", null, "alt_code2", null, "label", "Activities of households as employers; undifferetiated goods- and services-producing activities of households for own use"),
//                                     Map.of("code", "U", "alt_code1", null, "alt_code2", null, "label", "Activities of extraterritorial organisations and bodies"),
//                                     Map.of("code", "V", "alt_code1", null, "alt_code2", null, "label", "Other activities not adequately defined")));
                   }
                 }));
  }

  @TestFactory
  Stream<DynamicTest> getCodeTableWithAltCodes() {
    return Stream.of(databases)
                 .map(db -> dynamicTest(db.target().toString(), () -> {
                   System.out.println(db.target());
                   try (EsqlConnection con = db.esql()) {
                     Result rs = con.exec("lookuplabel(null, 'Country')");
                     printResult(rs, 20);
                   }
                 }));
  }

  @TestFactory
  Stream<DynamicTest> getLinkedCodeTable() {
    return Stream.of(databases)
                 .map(db -> dynamicTest(db.target().toString(), () -> {
                   System.out.println(db.target());
                   try (EsqlConnection con = db.esql()) {
                     Result rs = con.exec("lookuplabel(null, 'TestDivision', 'TestSection', show_code=true)");
                     printResult(rs, 20);
                   }
                 }));
  }

  @TestFactory
  Stream<DynamicTest> getLinkedCodeTableOffsetLimit() {
    return Stream.of(databases)
                 .map(db -> dynamicTest(db.target().toString(), () -> {
                   System.out.println(db.target());
                   try (EsqlConnection con = db.esql()) {
                     Result rs = con.exec("lookuplabel(null, 'TestDivision', 'TestSection', show_code=true, labels_offset=2, labels_limit=7)");
                     matchResult(rs, List.of(
                         Map.of("code", "03", "label", "A - Agriculture forestry and fishing"),
                         Map.of("code", "05", "label", "B - Mining and quarrying"),
                         Map.of("code", "06", "label", "B - Mining and quarrying"),
                         Map.of("code", "07", "label", "B - Mining and quarrying"),
                         Map.of("code", "08", "label", "B - Mining and quarrying"),
                         Map.of("code", "09", "label", "B - Mining and quarrying"),
                         Map.of("code", "10", "label", "C - Manufacturing")));
                   }
                 }));
  }
}