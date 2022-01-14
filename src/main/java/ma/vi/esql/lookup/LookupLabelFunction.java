/*
 * Copyright (c) 2020 Vikash Madhow
 */

package ma.vi.esql.lookup;

import ma.vi.esql.function.Function;
import ma.vi.esql.function.FunctionParameter;
import ma.vi.esql.semantic.type.Types;
import ma.vi.esql.syntax.EsqlPath;
import ma.vi.esql.syntax.expression.Expression;
import ma.vi.esql.syntax.expression.FunctionCall;

import java.util.Arrays;
import java.util.List;

import static ma.vi.esql.translation.Translatable.Target.POSTGRESQL;
import static ma.vi.esql.translation.Translatable.Target.SQLSERVER;

/**
 * Function to find the label for a code in a specified lookup, optionally
 * following links to linked lookups:
 * <p>
 * lookuplabelf(code, lookup, [show_code=true], [show_label=true], [links...])
 * <p>
 * Given that SQL Server does not support dynamic sql in functions
 * this function is resolved to a static function based on the number
 * of links on that database. Thus, there is a limit to the number of
 * links supported on SQL Server.
 * <p>
 * Other databases (Oracle, Postgresql) do not have this limitation and
 * supports any number of links using a dynamic query in the underlying
 * lookup function.
 *
 * @author Vikash Madhow (vikash.madhow@gmail.com)
 */
public class LookupLabelFunction extends Function {
  public LookupLabelFunction() {
    super("lookuplabelf", Types.TextType,
          Arrays.asList(new FunctionParameter("code", Types.TextType),
            new FunctionParameter("lookup", Types.TextType)));
  }

  @Override
  public String translate(FunctionCall call, Target target, EsqlPath path) {
    List<Expression<?, ?>> args = call.arguments();
    Expression<?, ?> code = args.get(0);
    Expression<?, ?> linkTable = args.get(1);
    if (target == POSTGRESQL) {
      StringBuilder func = new StringBuilder(
          "_core.lookup_label((" +
              code.translate(target) + ")::text, (" +
              linkTable.translate(target) + ")::text, ");
      String showCode = "true";
      String showText = "true";
      if (args.size() > 3) {
        showCode = (String)args.get(2).translate(target);
      }
      if (args.size() > 4) {
        showText = (String)args.get(3).translate(target);
      }
      func.append(showCode).append(", ").append(showText);
      for (int i = 4; i < args.size(); i++) {
        func.append(", (").append(args.get(i).translate(target)).append(")::text");
      }
      func.append(')');
      return func.toString();

    } else if (target == SQLSERVER) {
      StringBuilder func = new StringBuilder("("
          + code.translate(target) + ", "
          + linkTable.translate(target) + ", ");
      String showCode = "1";
      String showText = "1";
      if (args.size() > 3) {
        showCode = (String)args.get(2).translate(target);
      }
      if (args.size() > 4) {
        showText = (String)args.get(3).translate(target);
      }
      func.append(showCode).append(", ").append(showText);
      for (int i = 4; i < args.size(); i++) {
        func.append(", ").append(args.get(i).translate(target));
      }
      func.append(')');
      int links = args.size() > 4 ? args.size() - 4 : 0;
      return "_core.lookup_label" + links + func;

    } else {
      StringBuilder func = new StringBuilder(
          "lookuplabel(" +
              args.get(0).translate(target) + ", " +
              args.get(1).translate(target) + ", ");
      String showCode = "true";
      String showText = "true";
      if (args.size() > 3) {
        showCode = (String)args.get(2).translate(target);
      }
      if (args.size() > 4) {
        showText = (String)args.get(3).translate(target);
      }
      func.append(showCode).append(", ").append(showText);
      for (int i = 4; i < args.size(); i++) {
        func.append(", ").append(args.get(i).translate(target));
      }
      func.append(')');
      return func.toString();
    }
  }
}