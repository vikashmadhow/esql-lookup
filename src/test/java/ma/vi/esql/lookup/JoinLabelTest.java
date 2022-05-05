/*
 * Copyright (c) 2020 Vikash Madhow
 */

package ma.vi.esql.lookup;

import ma.vi.esql.database.EsqlConnection;
import ma.vi.esql.exec.Result;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static java.util.UUID.randomUUID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

public class JoinLabelTest extends DataTest {
  @TestFactory
  Stream<DynamicTest> simpleJoinLabel() {
    return Stream.of(databases)
                 .map(db -> dynamicTest(db.target().toString(), () -> {
                   System.out.println(db.target());
                   try (EsqlConnection con = db.esql()) {
                     con.exec("delete LkT from a.b.LkT");
                     con.exec("delete s from s:LkS");

                     UUID id1 = randomUUID(), id2 = randomUUID();
                     con.exec("insert into LkS(_id, a, b, e, h, j) values "
                                  + "(u'" + id1 + "', 'A1', 2, true, ['Four', 'Quatre']text, [1, 2, 3]int),"
                                  + "(u'" + id2 + "', 'A2', 7, false, ['Nine', 'Neuf', 'X']text, [5, 6, 7, 8]int)");

                     con.exec("insert into a.b.LkT(_id, a, b, s_id) values"
                                  + "(newid(), 'B1', 2, u'" + id1 + "'), "
                                  + "(newid(), 'B2', 4, u'" + id2 + "')");

                     Result rs = con.exec("select a, b, label:joinlabel(s_id, '_id', 'a', 'LkS') from a.b.LkT order by a");
                     matchResult(rs, Arrays.asList(
                                        Map.of("a", "B1", "label", "A1"),
                                        Map.of("a", "B2", "label", "A2")));
//                     printResult(rs, 30);
                   }
                 }));
  }

  @TestFactory
  Stream<DynamicTest> multipleJoinLabel() {
    return Stream.of(databases)
                 .map(db -> dynamicTest(db.target().toString(), () -> {
                   System.out.println(db.target());
                   try (EsqlConnection con = db.esql()) {
                     con.exec("delete LkX from a.b.LkX");
                     con.exec("delete LkT from a.b.LkT");
                     con.exec("delete s from s:LkS");

                     UUID id1 = randomUUID(), id2 = randomUUID();
                     con.exec("insert into LkS(_id, a, b, e, h, j) values "
                                  + "(u'" + id1 + "', 'A1', 2, true, ['Four', 'Quatre']text, [1, 2, 3]int),"
                                  + "(u'" + id2 + "', 'A2', 7, false, ['Nine', 'Neuf', 'X']text, [5, 6, 7, 8]int)");

                     UUID bid1 = randomUUID(), bid2 = randomUUID();
                     con.exec("insert into a.b.LkT(_id, a, b, s_id) values"
                                  + "(u'" + bid1 + "', 'B1', 2, u'" + id1 + "'), "
                                  + "(u'" + bid2 + "', 'B2', 4, u'" + id2 + "')");

                     con.exec("insert into a.b.LkX(_id, a, b, t_id) values"
                                  + "(newid(), 'C1', 13, u'" + bid1 + "'), "
                                  + "(newid(), 'C2', 23, u'" + bid2 + "')");

                     Result rs = con.exec("select a, b, label:a || ' / ' || joinlabel(t_id, '_id', 'a', 'a.b.LkT', " +
                                                                                   "'s_id', '_id', 'a', 'LkS', " +
                                                                                   "last_to_first=false, label_separator='|') from a.b.LkX order by a");
                     matchResult(rs, Arrays.asList(
                         Map.of("a", "C1", "b", 13, "label", "C1 / B1|A1"),
                         Map.of("a", "C2", "b", 23, "label", "C2 / B2|A2")));
//                     printResult(rs, 30);
                   }
                 }));
  }
}
