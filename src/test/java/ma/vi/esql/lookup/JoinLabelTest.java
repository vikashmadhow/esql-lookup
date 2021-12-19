/*
 * Copyright (c) 2020 Vikash Madhow
 */

package ma.vi.esql.lookup;

import ma.vi.esql.exec.EsqlConnection;
import ma.vi.esql.exec.Result;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.util.UUID;
import java.util.stream.Stream;

import static java.util.UUID.randomUUID;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

public class JoinLabelTest extends DataTest {
  @TestFactory
  Stream<DynamicTest> simpleJoinLabel() {
    return Stream.of(databases)
                 .map(db -> dynamicTest(db.target().toString(), () -> {
                   System.out.println(db.target());
                   try (EsqlConnection con = db.esql(db.pooledConnection())) {
                     con.exec("delete LkT from a.b.LkT");
                     con.exec("delete s from s:LkS");

                     UUID id1 = randomUUID(), id2 = randomUUID();
                     con.exec("insert into LkS(_id, a, b, e, h, j) values "
                                  + "(u'" + id1 + "', 'A1', 2, true, text['Four', 'Quatre'], int[1, 2, 3]),"
                                  + "(u'" + id2 + "', 'A2', 7, false, text['Nine', 'Neuf', 'X'], int[5, 6, 7, 8])");

                     con.exec("insert into a.b.LkT(_id, a, b, s_id) values"
                                  + "(newid(), 'B1', 2, u'" + id1 + "'), "
                                  + "(newid(), 'B2', 4, u'" + id2 + "')");

                     Result rs = con.exec("select a, b, joinlabel(s_id, '_id', 'a', 'LkS') from a.b.LkT");
                     printResult(rs, 30);
                   }
                 }));
  }

  @TestFactory
  Stream<DynamicTest> multipleJoinLabel() {
    return Stream.of(databases)
                 .map(db -> dynamicTest(db.target().toString(), () -> {
                   System.out.println(db.target());
                   try (EsqlConnection con = db.esql(db.pooledConnection())) {
                     con.exec("delete LkX from a.b.LkX");
                     con.exec("delete LkT from a.b.LkT");
                     con.exec("delete s from s:LkS");

                     UUID id1 = randomUUID(), id2 = randomUUID();
                     con.exec("insert into LkS(_id, a, b, e, h, j) values "
                                  + "(u'" + id1 + "', 'A1', 2, true, text['Four', 'Quatre'], int[1, 2, 3]),"
                                  + "(u'" + id2 + "', 'A2', 7, false, text['Nine', 'Neuf', 'X'], int[5, 6, 7, 8])");

                     UUID bid1 = randomUUID(), bid2 = randomUUID();
                     con.exec("insert into a.b.LkT(_id, a, b, s_id) values"
                                  + "(u'" + bid1 + "', 'B1', 2, u'" + id1 + "'), "
                                  + "(u'" + bid2 + "', 'B2', 4, u'" + id2 + "')");

                     con.exec("insert into a.b.LkX(_id, a, b, t_id) values"
                                  + "(newid(), 'C1', 13, u'" + bid1 + "'), "
                                  + "(newid(), 'C2', 23, u'" + bid2 + "')");

                     Result rs = con.exec("select a, b, a || ' / ' || joinlabel(t_id, '_id', 'a', 'a.b.LkT', " +
                                                                                   "'s_id', '_id', 'a', 'LkS', " +
                                                                                   "last_to_first:=false, label_separator:='|') from a.b.LkX");
                     printResult(rs, 30);
                   }
                 }));
  }
}
