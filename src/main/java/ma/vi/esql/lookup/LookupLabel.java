/*
 * Copyright (c) 2018-2021 Vikash Madhow
 */

package ma.vi.esql.lookup;

import ma.vi.base.tuple.T2;
import ma.vi.esql.builder.SelectBuilder;
import ma.vi.esql.exec.function.Function;
import ma.vi.esql.exec.function.FunctionCall;
import ma.vi.esql.exec.function.NamedArgument;
import ma.vi.esql.semantic.type.Types;
import ma.vi.esql.syntax.Context;
import ma.vi.esql.syntax.Esql;
import ma.vi.esql.syntax.EsqlPath;
import ma.vi.esql.syntax.expression.ColumnRef;
import ma.vi.esql.syntax.expression.Concatenation;
import ma.vi.esql.syntax.expression.Expression;
import ma.vi.esql.syntax.expression.SelectExpression;
import ma.vi.esql.syntax.expression.comparison.Equality;
import ma.vi.esql.syntax.expression.literal.StringLiteral;
import ma.vi.esql.syntax.expression.logical.And;
import ma.vi.esql.syntax.macro.TypedMacro;
import ma.vi.esql.syntax.query.JoinTableExpr;
import ma.vi.esql.syntax.query.QueryUpdate;
import ma.vi.esql.syntax.query.SingleTableExpr;
import ma.vi.esql.syntax.query.TableExpr;
import ma.vi.esql.translation.TranslationException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static ma.vi.esql.lookup.JoinLabel.getBooleanParam;
import static ma.vi.esql.syntax.query.ColumnList.makeUnique;
import static ma.vi.esql.translation.Translatable.Target.ESQL;

/**
 * <p>
 * A macro function which produces a label corresponding to a lookup code. It
 * can be used as follows:
 * </p>
 *
 * <p>
 * <code>lookuplabel(code, X)</code> will get the label corresponding to code
 * from a lookup table named X. A variable number of named links can be supplied
 * to find linked valued. E.g. <code>lookuplabel(code, X, Y, Z)</code> will find the
 * code in lookup X, follow its link to Y and then Z and return the label for
 * the latter.
 * </p>
 *
 * <p>
 * lookuplabel can have the following optional named arguments to control the
 * value displayed:
 * <ul>
 * <li><b>show_code:</b> whether to show the code in the label or not.
 *                       Default is true.</li>
 * <li><b>show_text:</b> whether to show the label text in the label or not.
 *                       Default is true.</li>
 * <li><b>code_separator:</b> an expression for the separator between the code
 *                            and text. Default is ' - '</li>
 * <li><b>show_last_only:</b> Show the last label element in the chain only
 *                            (a -&gt; b -&gt; c, show c only). Default is true.</li>
 * <li><b>label_separator:</b> an expression for the separator between the labels
 *                             from different lookups. Default is '/'.</li>
 * <li><b>last_to_first:</b> Shows the names from the link tables from the last
 *                           linked table to the first, if true, or otherwise,
 *                           from the first to the last. Default is true.</li>
 * <li><b>match_by:</b> the code column in the LookupValue to match the value to;
 *                      can be 'code', 'alt_code1' or 'alt_code2'. Default is 'code'.</li>
 * </ul>
 *
 * @author Vikash Madhow (vikash.madhow@gmail.com)
 */
public class LookupLabel extends Function implements TypedMacro {
  /**
   * Creates the lookuplabel macro function.
   * @param schema The database schema in which the lookup tables and functions
   *               will be created.
   */
  public LookupLabel(String schema) {
    super("lookuplabel", Types.StringType, emptyList());
    this.schema = schema;
  }

  @Override
  public Esql<?, ?> expand(Esql<?, ?> esql, EsqlPath path) {
    FunctionCall call = (FunctionCall)esql;
    Context ctx = call.context;
    List<Expression<?, ?>> arguments = call.arguments();

    if (arguments.size() < 2) {
      throw new TranslationException("lookuplabel needs at least 2 arguments: "
                                   + "the column or expression containing the code for which the label is to be found and "
                                   + "the name of the lookup table containing the label for the code. E.g., "
                                   + "lookuplabel(country_code, 'Country')");
    }

    /*
     * Load arguments included named arguments and links.
     */
    Expression<?, ?> code           = null;                                   // The code to search a label for.
    Expression<?, ?> lookup         = null;                                   // The lookup to which the code belongs to.
    List<String>     links          = new ArrayList<>();                      // lookup links.
    boolean          showCode       = true;                                   // show the code in the label or not.
    boolean          showText       = true;                                   // show the text in the label or not.
    Expression<?, ?> codeSeparator  = new StringLiteral(ctx, "' - '");  // the separator to use between code and text in the label.
    boolean          showLastOnly   = true;                                   // show only the last element (last linked foreign table) of the join.
    Expression<?, ?> labelSeparator = new StringLiteral(ctx, "' / '");  // the separator to use between labels from different table (joins).
    boolean          lastToFirst    = true;                                   // show labels last to first (or first to last if false).
    String           matchBy        = "code";                                 // Match by code, alt_code1 or alt_code2. Default is code.

    for (Expression<?, ?> arg: arguments) {
      if (arg instanceof NamedArgument namedArg) {
        switch (namedArg.name()) {
          case "show_code"       -> showCode       = getBooleanParam(namedArg, "show_code", path);
          case "show_text"       -> showText       = getBooleanParam(namedArg, "show_text", path);
          case "code_separator"  -> codeSeparator  = namedArg.arg();
          case "show_last_only"  -> showLastOnly   = getBooleanParam(namedArg, "show_last_only", path);
          case "last_to_first"   -> lastToFirst    = getBooleanParam(namedArg, "last_to_first", path);
          case "label_separator" -> labelSeparator = namedArg.arg();
          case "match_by"        -> matchBy        = namedArg.arg().translate(ESQL);
          default                -> throw new TranslationException("""
                                                                   Invalid named argument in lookuplabel: %1s
                                                                   lookuplabel recognises the following named arguments:
                                                                   show_code: whether to show the code in the label or not. Default is true.
                                                                   show_text: whether to show the label or not. Default is true.
                                                                   code_separator: the separator between the code and text. Default is ' - '
                                                                   show_last_only: Show the last label element in the chain only (a -> b -> c, show c only). Default is true.
                                                                   label_separator: the separator between the labels from different lookups. Default is '/'.
                                                                   last_to_first: Shows the names from the link tables from the last linked table to the first, if true, or otherwise, from the first to the last. Default is true.
                                                                   match_by: the code column in LookupValue to match the value to; can be 'code', 'alt_code1' or 'alt_code2'. Default is 'code'.
                                                                   """.formatted(namedArg.name()));
        }
      } else if (code == null) {
        code = arg;
      } else if (lookup == null) {
        lookup = arg;
      } else {
        links.add((String)arg.exec(ESQL, null, path, arg.context.structure));
      }
    }
    if (code == null) {
      throw new TranslationException("The code for which the label is to be found has not been provided");
    }
    if (lookup == null) {
      throw new TranslationException("The name of the lookup table containing the label for the code has not been provided");
    }

    /*
     * lookup table:
     *    lookuplabel('123', X) is transformed to (pseudo-code):
     *
     *      select v0.code || ' - ' || v0.label
     *        from LookupValue v0 join Lookup l on v.lookup_id=l.id and l.name=X
     *       where v0.code='123'
     *
     *    lookuplabel('123', X, Y) is transformed to (pseudo-code):
     *
     *      select v1.code || ' - ' || v1.label
     *        from LookupValue v1
     *        join LookupLink lk1 on v1.id=lk1.target_value_id
     *        join LookupValue v0 on (lk1.source_value_id=v0.id and lk1.name=Y)
     *        join Lookup l on v0.lookup_id=l.id and l.name=X
     *       where v0.code='123'
     */
    QueryUpdate qu = path.ancestor(QueryUpdate.class);
    Set<String> aliases = qu != null && qu.tables().exists(path)
                        ? new HashSet<>(qu.tables().computeType(path.add(qu)).aliases())
                        : new HashSet<>();
    int aliasIndex = 1;
    T2<String, Integer> uniqueName = makeUnique("value", aliases, aliasIndex, false);
    String fromValueAlias = uniqueName.a;
    aliasIndex = uniqueName.b;
    String firstFromValueAlias = fromValueAlias;

    uniqueName = makeUnique("lookup", aliases, aliasIndex, false);
    String lookupAlias = uniqueName.a;
    aliasIndex = uniqueName.b;

    Expression<?, String> value = label(ctx, showText, showCode, matchBy, fromValueAlias, codeSeparator);

    /*
     * from LookupValue v0
     * join Lookup l on v0.lookup_id=l._id and l.name=X
     */
    TableExpr from = new JoinTableExpr(ctx, null, false,
                                       new SingleTableExpr(ctx, schema + ".LookupValue", fromValueAlias),
                                       new SingleTableExpr(ctx, schema + ".Lookup", lookupAlias),
                                       new And(ctx,
                                               new Equality(ctx,
                                                            new ColumnRef(ctx, fromValueAlias, "lookup_id"),
                                                            new ColumnRef(ctx, lookupAlias, "_id")),
                                               new Equality(ctx,
                                                            new ColumnRef(ctx, lookupAlias, "name"),
                                                            lookup)));
    for (String linkName: links) {
      uniqueName = makeUnique("value", aliases, aliasIndex, false);
      String toValueAlias = uniqueName.a;
      aliasIndex = uniqueName.b;

      uniqueName = makeUnique("link", aliases, aliasIndex, false);
      String toLinkAlias = uniqueName.a;
      aliasIndex = uniqueName.b;

      /*
       * from ...
       * join LookupValueLink lk1 on lk1.source_value_id=v0._id and lk1.name=link_name
       * join LookupValue      v1 on lk1.target_value_id=v1._id
       */
      from = new JoinTableExpr(ctx, null, false, from,
                               new SingleTableExpr(ctx, schema + ".LookupValueLink", toLinkAlias),
                               new And(ctx,
                                       new Equality(ctx,
                                                    new ColumnRef(ctx, toLinkAlias, "source_value_id"),
                                                    new ColumnRef(ctx, fromValueAlias, "_id")),
                                       new Equality(ctx,
                                                    new ColumnRef(ctx, toLinkAlias, "name"),
                                                    new StringLiteral(ctx, linkName))));
      from = new JoinTableExpr(ctx, null, false, from,
                               new SingleTableExpr(ctx, schema + ".LookupValue", toValueAlias),
                               new Equality(ctx,
                                            new ColumnRef(ctx, toLinkAlias, "target_value_id"),
                                            new ColumnRef(ctx, toValueAlias, "_id")));

      Expression<?, String> label = label(ctx, showText, showCode, matchBy, toValueAlias, codeSeparator);
      value = showLastOnly ? label :
              lastToFirst  ? new Concatenation(ctx, asList(label, labelSeparator, value)) :
                             new Concatenation(ctx, asList(value, labelSeparator, label));
      fromValueAlias = toValueAlias;
    }
    return new SelectExpression(ctx, new SelectBuilder(ctx)
                                           .column(value, null)
                                           .from(from)
                                           .where(new Equality(ctx,
                                                               new ColumnRef(ctx, firstFromValueAlias, matchBy),
                                                               code))
                                           .build());
  }

  private static Expression<?, String> label(Context ctx,
                                             boolean showText,
                                             boolean showCode,
                                             String matchBy,
                                             String alias,
                                             Expression<?, ?> codeSeparator) {
    Expression<?, String> label;
    if (!showText) {
      /*
       * Show code only
       */
      label = new ColumnRef(ctx, alias, matchBy);
    } else {
      label = new ColumnRef(ctx, alias, "label");
      if (showCode) {
        /*
         * Prepend code and separator
         */
        label = new Concatenation(ctx,
                                  asList(new ColumnRef(ctx, alias, matchBy),
                                         codeSeparator,
                                         label));
      }
    }
    return label;
  }

  /**
   * Configured schema to use for the lookup tables and functions
   */
  private final String schema;
}
