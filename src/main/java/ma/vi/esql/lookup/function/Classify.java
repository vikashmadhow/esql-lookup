/*
 * Copyright (c) 2020 Vikash Madhow
 */

package ma.vi.esql.lookup.function;

import ma.vi.esql.database.EsqlConnection;
import ma.vi.esql.exec.env.Environment;
import ma.vi.esql.exec.function.Function;
import ma.vi.esql.exec.function.FunctionCall;
import ma.vi.esql.exec.function.FunctionParam;
import ma.vi.esql.semantic.type.Types;
import ma.vi.esql.syntax.EsqlPath;
import ma.vi.esql.syntax.expression.Expression;
import org.pcollections.PMap;

import java.util.Arrays;
import java.util.List;

/**
 * `Function returning true if an element is in an array. It is translated using
 * array operations on databases that have such support or `string_split` on SQL
 * Server.
 *
 * @author Vikash Madhow (vikash.madhow@gmail.com)
 */
public class Classify extends Function {
  public Classify() {
    super("classify", Types.StringType,
          Arrays.asList(new FunctionParam("text",   Types.StringType),
                        new FunctionParam("lookup", Types.StringType)));
  }

  @Override
  public String translate(FunctionCall call, Target target, EsqlConnection esqlCon, EsqlPath path, PMap<String, Object> parameters, Environment env) {
    List<Expression<?, ?>> args = call.arguments();
    String text   = String.valueOf(args.get(0).translate(target, esqlCon, path.add(args.get(0)), env));
    String lookup = String.valueOf(args.get(1).translate(target, esqlCon, path.add(args.get(1)), env));
    return switch(target) {
      case POSTGRESQL -> """
                         (select value.code
                            from _lookup."LookupValue" value
                            join _lookup."Lookup"      lookup on lookup._id=value.lookup_id
                                                             and lookup.name=%1$s
                          where value.label %% %2$s
                          order by value.label <-> %2$s
                          limit 1)
                         """.formatted(lookup, text);

      case SQLSERVER -> """
                        (select value.code
                           from _lookup."LookupValue" value
                           join _lookup."Lookup"      lookup on lookup._id=value.lookup_id
                                                           and lookup.name=%1$s
                          where lower(%2$s) like lower(value.label)
                          order by value.code
                          fetch next 1 rows only)
                        """.formatted(lookup, text);

//      case JAVASCRIPT -> "new Set(" + args.get(1).translate(target, esqlCon, path.add(args.get(1)), env) + ").has("
//                                    + args.get(0).translate(target, esqlCon, path.add(args.get(0)), env) + ")";

      default ->  name + '(' + args.get(0).translate(target, esqlCon, path.add(args.get(0)), env) + ", "
                             + args.get(1).translate(target, esqlCon, path.add(args.get(1)), env) + ')';
    };
  }
}
