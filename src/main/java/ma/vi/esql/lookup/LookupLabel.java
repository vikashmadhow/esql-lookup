/*
 * Copyright (c) 2018-2021 Vikash Madhow
 */

package ma.vi.esql.lookup;

import ma.vi.base.tuple.T2;
import ma.vi.esql.builder.SelectBuilder;
import ma.vi.esql.exec.function.Function;
import ma.vi.esql.exec.function.FunctionCall;
import ma.vi.esql.exec.function.NamedArgument;
import ma.vi.esql.semantic.type.ArrayType;
import ma.vi.esql.semantic.type.Types;
import ma.vi.esql.syntax.Context;
import ma.vi.esql.syntax.Esql;
import ma.vi.esql.syntax.EsqlPath;
import ma.vi.esql.syntax.define.Define;
import ma.vi.esql.syntax.expression.*;
import ma.vi.esql.syntax.expression.comparison.Equality;
import ma.vi.esql.syntax.expression.literal.BaseArrayLiteral;
import ma.vi.esql.syntax.expression.literal.NullLiteral;
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
import static ma.vi.esql.lookup.JoinLabel.getStringParam;
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
 * <li><b>show_code:</b> whether to show the code in the label. Default is false.</li>
 * <li><b>show_label:</b> whether to show the label. Default is true.</li>
 * <li><b>show_description:</b> whether to show the description. Default is false.</li>
 * <li><b>code_separator:</b> an expression for the separator between the code
 *                            and text. Default is ' - '</li>
 * <li><b>show_last_only:</b> show the last label element in the chain only
 *                            (a -&gt; b -&gt; c, show c only). Default is true.</li>
 * <li><b>label_separator:</b> an expression for the separator between the labels
 *                             from different lookups. Default is '/'.</li>
 * <li><b>last_to_first:</b> shows the names from the link tables from the last
 *                           linked table to the first, if true, or otherwise,
 *                           from the first to the last. Default is true.</li>
 * <li><b>match_by:</b> the code column in the LookupValue to match the value to;
 *                      can be 'code', 'alt_code1' or 'alt_code2'. Default is 'code'.</li>
 * <li><b>matching:</b> a criteria to restrict the code-label pairs to load from
 *                      the lookup. Applies only when the code searched is null,
 *                      meaning that the whole lookup is to be loaded.</li>
 * <li><b>labels_offset:</b> An offset from the start of loaded data from where
 *                           to start returning labels. Applies only when the code
 *                           searched is null; can be used to lazily load labels.</li>
 * <li><b>labels_limit:</b> The number labels to return. Applies only when the
 *                          code searched is null; can be used to lazily load
 *                          labels in pages.</li>
 * </ul>
 *
 * @author Vikash Madhow (vikash.madhow@gmail.com)
 */
public class LookupLabel extends Function implements TypedMacro {
  /**
   * Creates the lookuplabel macro function.
   */
  public LookupLabel() {
    super("lookuplabel", Types.StringType, emptyList());
  }

  @Override
  public Esql<?, ?> expand(Esql<?, ?> esql, EsqlPath path) {
    if (path.hasAncestor(Define.class, UncomputedExpression.class)) {
      /*
       * Do not expand in Define statement (create/alter table/struct) or in
       * uncomputed expression as the expansion to a select will not work in most
       * cases when executed on the client-side.
       */
      return esql;
    }
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
    Expression<?, ?> code            = null;                             // The code to search a label for.
    Expression<?, ?> lookup          = null;                             // The lookup to which the code belongs to.
    List<String>     links           = new ArrayList<>();                // lookup links.
    boolean          showCode        = false;                            // show the code or not.
    boolean          showLabel       = true;                             // show the label or not.
    boolean          showDescription = false;                            // show the description or not.
    Expression<?, ?> codeSeparator   = new StringLiteral(ctx, " - ");    // the separator to use between code and text in the label.
    boolean          showLastOnly    = true;                             // show only the last element (last linked foreign table) of the join.
    Expression<?, ?> labelSeparator  = new StringLiteral(ctx, " / ");    // the separator to use between labels from different table (joins).
    boolean          lastToFirst     = true;                             // show labels last to first (or first to last if false).
    String           matchBy         = "code";                           // Match by code, alt_code1 or alt_code2. Default is code.
    String           matching        = null;                             // Criteria to restrict code-label pairs to load for whole lookup.
    Expression<?, ?> offset          = null;                             // Offset labels loading by this number.
    Expression<?, ?> limit           = null;                             // Limit labels to load to this number.

    for (Expression<?, ?> arg: arguments) {
      if (arg instanceof NamedArgument namedArg) {
        switch (namedArg.name()) {
          case "show_code"        -> showCode        = getBooleanParam(namedArg, "show_code", path);
          case "show_label"       -> showLabel       = getBooleanParam(namedArg, "show_label", path);
          case "show_description" -> showDescription = getBooleanParam(namedArg, "show_description", path);
          case "code_separator"   -> codeSeparator   = namedArg.arg();
          case "show_last_only"   -> showLastOnly    = getBooleanParam(namedArg, "show_last_only", path);
          case "last_to_first"    -> lastToFirst     = getBooleanParam(namedArg, "last_to_first", path);
          case "label_separator"  -> labelSeparator  = namedArg.arg();
          case "match_by"         -> matchBy         = getStringParam(namedArg, "match_by", path);
          case "matching"         -> matching        = getStringParam(namedArg, "matching", path);
          case "labels_offset"    -> offset         = namedArg.arg();
          case "labels_limit"     -> limit          = namedArg.arg();
          default                 -> throw new TranslationException("""
                                                                   Invalid named argument in lookuplabel: %1s
                                                                   lookuplabel recognises the following named arguments:
                                                                   show_code: whether to show the code in the label or not. Default is true.
                                                                   show_label: whether to show the label or not. Default is true.
                                                                   show_description: whether to show the description or not. Default is false.
                                                                   code_separator: the separator between the code and text. Default is ' - '
                                                                   show_last_only: chow the last label element in the chain only (a -> b -> c, show c only). Default is true.
                                                                   label_separator: the separator between the labels from different lookups. Default is '/'.
                                                                   last_to_first: shows the names from the link tables from the last linked table to the first, if true, or otherwise, from the first to the last. Default is true.
                                                                   match_by: the code column in LookupValue to match the value to; can be 'code', 'alt_code1' or 'alt_code2'. Default is 'code'.
                                                                   matching: criteria to restrict code-label pairs to load for whole lookup.
                                                                   labels_offset: offset labels loading by this number.
                                                                   labels_limit: limit labels to load to this number.
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

    boolean codeIsArray = code instanceof ColumnRef ref && ref.type() instanceof ArrayType
                       || code instanceof BaseArrayLiteral;

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
     *        join LookupValueLink lk1 on v1.id=lk1.target_value_id
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

    Expression<?, String> value = label(ctx, showCode, showLabel, showDescription,
                                        matchBy, fromValueAlias, codeSeparator);

    /*
     * from LookupValue v0
     * join Lookup l on v0.lookup_id=l._id and l.name=X
     */
    TableExpr from = new JoinTableExpr(ctx, null, false,
                                       new SingleTableExpr(ctx, "_lookup.LookupValue", fromValueAlias),
                                       new SingleTableExpr(ctx, "_lookup.Lookup", lookupAlias),
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
                               new SingleTableExpr(ctx, "_lookup.LookupValueLink", toLinkAlias),
                               new And(ctx,
                                       new Equality(ctx,
                                                    new ColumnRef(ctx, toLinkAlias, "source_value_id"),
                                                    new ColumnRef(ctx, fromValueAlias, "_id")),
                                       new Equality(ctx,
                                                    new ColumnRef(ctx, toLinkAlias, "name"),
                                                    new StringLiteral(ctx, linkName))));
      from = new JoinTableExpr(ctx, null, false, from,
                               new SingleTableExpr(ctx, "_lookup.LookupValue", toValueAlias),
                               new Equality(ctx,
                                            new ColumnRef(ctx, toLinkAlias, "target_value_id"),
                                            new ColumnRef(ctx, toValueAlias, "_id")));

      Expression<?, String> label = label(ctx, showCode, showLabel, showDescription,
                                          matchBy, toValueAlias, codeSeparator);
      value = showLastOnly ? label :
              lastToFirst  ? new Concatenation(ctx, asList(label, labelSeparator, value)) :
                             new Concatenation(ctx, asList(value, labelSeparator, label));
      fromValueAlias = toValueAlias;
    }
    if (code instanceof NullLiteral) {
      SelectBuilder builder =  new SelectBuilder(ctx);
      if (matching != null) {
        Expression<?, String> where = ColumnRef.qualify(builder.parser.parseExpression(matching), firstFromValueAlias);
        builder.where(where);
      }
      if (offset != null) {
        builder.offset((Expression<?, String>)offset);
      }
      if (limit != null) {
        builder.limit((Expression<?, String>)limit);
      }
      return builder.column (new ColumnRef(ctx, firstFromValueAlias, matchBy), "code")
                    .column (new ColumnRef(ctx, firstFromValueAlias, "alt_code1"), "alt_code1")
                    .column (new ColumnRef(ctx, firstFromValueAlias, "alt_code2"), "alt_code2")
                    .column (value, "label")
                    .from   (from)
                    .orderBy(firstFromValueAlias + '.' + matchBy)
                    .build  ();

    } else if (codeIsArray) {
      return new SelectExpression(ctx,
                                  new SelectBuilder(ctx)
                                       .column(value, "label")
                                       .from  (from)
                                       .where (new FunctionCall(ctx, "inarray",
                                                                List.of(new ColumnRef(ctx, firstFromValueAlias, matchBy), code)))
                                       .build ());
    } else {
      return new SelectExpression(ctx,
                                  new SelectBuilder(ctx)
                                       .column(value, "label")
                                       .from  (from)
                                       .where (new Equality(ctx,
                                                            new ColumnRef(ctx, firstFromValueAlias, matchBy),
                                                            code))
                                       .build ());
    }
  }

  private static Expression<?, String> label(Context ctx,
                                             boolean showCode,
                                             boolean showLabel,
                                             boolean showDescription,
                                             String matchBy,
                                             String alias,
                                             Expression<?, ?> codeSeparator) {
    Expression<?, String> label = null;
    if (showCode)         label = addToLabel(ctx, label, alias, matchBy, codeSeparator);
    if (showLabel)        label = addToLabel(ctx, label, alias, "label", codeSeparator);
    if (showDescription)  label = addToLabel(ctx, label, alias, "description", codeSeparator);
    return label;
  }

  private static Expression<?, String> addToLabel(Context               ctx,
                                                  Expression<?, String> label,
                                                  String                alias,
                                                  String                column,
                                                  Expression<?, ?>      codeSeparator) {
    ColumnRef expr = new ColumnRef(ctx, alias, column);
    return label == null
        ? expr
        : new Concatenation(ctx, asList(label, codeSeparator, expr));
  }
}
