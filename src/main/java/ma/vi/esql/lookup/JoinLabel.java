/*
 * Copyright (c) 2021 Vikash Madhow
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
import ma.vi.esql.syntax.Parser;
import ma.vi.esql.syntax.define.Define;
import ma.vi.esql.syntax.expression.*;
import ma.vi.esql.syntax.expression.comparison.Equality;
import ma.vi.esql.syntax.expression.comparison.ILike;
import ma.vi.esql.syntax.expression.comparison.IsNull;
import ma.vi.esql.syntax.expression.literal.NullLiteral;
import ma.vi.esql.syntax.expression.literal.StringLiteral;
import ma.vi.esql.syntax.macro.TypedMacro;
import ma.vi.esql.syntax.query.JoinTableExpr;
import ma.vi.esql.syntax.query.QueryUpdate;
import ma.vi.esql.syntax.query.SingleTableExpr;
import ma.vi.esql.syntax.query.TableExpr;
import ma.vi.esql.translation.TranslationException;

import java.util.*;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static ma.vi.esql.semantic.type.Type.unqualifiedName;
import static ma.vi.esql.syntax.expression.ColumnRef.qualify;
import static ma.vi.esql.syntax.query.ColumnList.makeUnique;
import static ma.vi.esql.translation.Translatable.Target.ESQL;

/**
 * <p>
 * A macro function which produces a label corresponding to a sequence of ids
 * from linked tables.
 * </p>
 *
 * <p>
 * To get the label corresponding to an id referring to another table. For
 * instance, if table A {b_id} refers to B{id, name} then
 * <code>joinlabel(b_id, 'id', 'name', 'B')</code> will return the name from B
 * corresponding to b_id. <code>joinlabel(b_id, 'id', 'name', 'B', 'c_id', 'id', 'name', 'C')</code>
 * will produce 'c_name / b_name' corresponding the b_id and following on to c_id.
 * Any number of links can be specified.
 * </p>
 *
 * <p>
 * When the code to get the label for is `null`, joinlabel loads all possible
 * labels. This can be used to populate a drop-down for selection.
 * </p>
 *
 * <p>
 * joinlabel can have the following optional named arguments to control the
 * value displayed:
 * <ul>
 * <li><b>show_code:</b> whether to show the code in the label. Default is false.</li>
 * <li><b>show_last_only:</b> Show the last label element in the chain only
 *                            (a -&gt; b -&gt; c, show c only). Default is false.</li>
 * <li><b>label_separator:</b> An expression for the separator between the
 *                             labels from different tables. Default is '/'.</li>
 * <li><b>last_to_first:</b> Shows the names from the link tables from the
 *                           last linked table to the first, if true, or otherwise,
 *                           from the first to the last. Default is true.</li>
 * <li><b>matching:</b> A criteria to restrict the code-label pairs to load from
 *                      the lookup. Applies only when the code searched is null.</li>
 * <li><b>keywords:</b> A string containing keywords that will be used to limit
 *                      the loaded labels. Applies only when the code searched
 *                      is null.</li>
 * <li><b>labels_offset:</b> An offset from the start of loaded data from where
 *                           to start returning labels. Applies only when the code
 *                           searched is null; can be used to lazily load labels.</li>
 * <li><b>labels_limit:</b> The number labels to return. Applies only when the
 *                          code searched is null; can be used to lazily load
 *                          labels in pages.</li>
 * <li><b>notfiltered:</b> Disable automatic security filtering. Default is false.</li>
 * </ul>
 * @author Vikash Madhow (vikash.madhow@gmail.com)
 */
public class JoinLabel extends Function implements TypedMacro {
  /** Creates the joinlabel macro function. */
  public JoinLabel() {
    super("joinlabel", Types.StringType, emptyList());
  }

  @Override
  public Esql<?, ?> expand(Esql<?, ?> esql, EsqlPath path) {
    if (path.hasAncestor(Define.class, UncomputedExpression.class)) {
      /*
       * Do not expand in Define statement (create/alter table/struct) or in
       * uncomputed expression as the expansion to a select will not work in
       * most cases when executed on the client-side.
       */
      return esql;
    }
    FunctionCall call = (FunctionCall)esql;
    Context ctx = call.context;
    List<Expression<?, ?>> arguments = call.arguments();

    if (arguments.size() < 4) {
      throw new TranslationException("joinlabel needs at least 4 arguments: "
                                   + "the column or expression to link to the target table containing the label, "
                                   + "the column or expression in the target table label to link to, "
                                   + "the column or expression in the target table label to generate the label and "
                                   + "the target table containing the label. E.g., "
                                   + "joinlabel(company_id, 'id', 'name', 'Company')");
    }

    /*
     * Load arguments.
     */
    List<Link>       links          = new ArrayList<>();
    boolean          showCode       = false;                            // show the code or not.
    boolean          showLastOnly   = false;                            // Show only the last element (last linked foreign table) of the join.
    Expression<?, ?> labelSeparator = new StringLiteral(ctx, " / ");    // The separator to use between labels from different table (joins).
    boolean          lastToFirst    = true;                             // Show labels last to first (or first to last if false).
    String           matching       = null;                             // Criteria to restrict code-label pairs to load for whole lookup.
    String           keywords       = null;                             // Keywords that will be used to limit the loaded labels.
    Expression<?, ?> offset         = null;                             // Offset labels loading by this number.
    Expression<?, ?> limit          = null;                             // Limit labels to load to this number.
    boolean          unfiltered     = false;                            // Enable or disable automatic security filtering. default is false.

    Iterator<Expression<?, ?>> i = arguments.iterator();
    while (i.hasNext()) {
      Expression<?, ?> arg = i.next();
      if (arg instanceof NamedArgument namedArg) {
        switch (namedArg.name()) {
          case "show_code"       -> showCode       = getBooleanParam(namedArg, "show_code", path);
          case "show_last_only"  -> showLastOnly   = getBooleanParam(namedArg, "show_last_only", path);
          case "last_to_first"   -> lastToFirst    = getBooleanParam(namedArg, "last_to_first", path);
          case "label_separator" -> labelSeparator = namedArg.arg();
          case "matching"        -> matching       = getStringParam(namedArg, "matching", path);
          case "keywords"        -> keywords       = getStringParam(namedArg, "keywords", path);
          case "labels_offset"   -> offset         = namedArg.arg();
          case "labels_limit"    -> limit          = namedArg.arg();
          case "notfiltered"     -> unfiltered     = getBooleanParam(namedArg, "notfiltered", path);
          default                -> throw new TranslationException("Invalid named argument in joinlabel: " + namedArg.name());
        }
      } else {
        /*
         * Link arguments consist of 3 parts: the id expression, the name
         * expression and the target table.
         */
        if (!i.hasNext()) {
          throw new TranslationException("joinlabel needs a source id, a target id, a label and a target table for each "
                                       + "link. Only the source id was provided for one link.");
        }
        String targetId = (String)i.next().exec(ESQL, null, path, esql.context.structure);

        if (!i.hasNext()) {
          throw new TranslationException("joinlabel needs a source id, a target id, a label and a target table for each "
                                       + "link. Only the source id and target id were provided for one link.");
        }
        String label = (String)i.next().exec(ESQL, null, path, esql.context.structure);

        if (!i.hasNext()) {
          throw new TranslationException("joinlabel needs a source id, a target id, a label and a target table for each "
                                       + "link. Only the source id, target id and label were provided for one link.");
        }
        String table = (String)i.next().exec(ESQL, null, path, esql.context.structure);
        links.add(new Link(arg, targetId, label, table));
      }
    }

    if (links.isEmpty()) {
      throw new TranslationException("No links supplied to joinlabel function");
    }

    /*
     * joinlabel(linked_id, target_link_id, name, table) is transformed to (pseudo-code):
     *
     *      select t0.name from table t0 where t0.target_link_id=linked_id
     *
     * joinlabel(bu_id, _id, bu_name, BusinessUnit,
     *           company_id, _id, company_name, Company
     *           legal_company_id, _id, legal_company_name, LegalCompany) is transformed to (pseudo-code):
     *
     *      select t3.legal_company_name || ' / ' || t2.company_name || ' / ' || t1.bu_name
     *
     *      from   LegalCompany t3
     *      join   Company      t2 on t2.legal_company_id=t3._id
     *      join   BusinessUnit t1 on t1.company_id=t2._id
     *
     *      where  t1._id=bu_id
     */
    Iterator<Link> linkIter = links.iterator();
    Link link = linkIter.next();

    Parser parser = new Parser(ctx.structure);
    Expression<?, ?> firstSourceId = link.sourceId;

    QueryUpdate qu = path.ancestor(QueryUpdate.class);
    Set<String> aliases = qu != null && qu.tables().exists(path)
                        ? new HashSet<>(qu.tables().computeType(path.add(qu)).aliases())
                        : new HashSet<>();
    int aliasIndex = 1;
    T2<String, Integer> uniqueName = makeUnique(unqualifiedName(link.targetTable),
                                                aliases, aliasIndex, false);
    String fromAlias = uniqueName.a;
    aliasIndex = uniqueName.b;
    String firstFromAlias = fromAlias;
    Expression<?, String> firstTargetId = toColumnRef(parser, link.targetId, fromAlias, path);
    Expression<?, String> value = toColumnRef(parser, link.labelColumn, fromAlias, path);
    TableExpr from = new SingleTableExpr(ctx, link.targetTable, fromAlias);

    while (linkIter.hasNext()) {
      uniqueName = makeUnique(unqualifiedName(link.targetTable),
                              aliases, aliasIndex, false);
      String toAlias = uniqueName.a;
      aliasIndex = uniqueName.b;

      link = linkIter.next();
      from = new JoinTableExpr(ctx, null, false, from,
                               new SingleTableExpr(ctx, link.targetTable, toAlias),
                               new Equality(ctx,
                                            toColumnRef(parser, link.sourceId, fromAlias, path),
                                            toColumnRef(parser, link.targetId, toAlias, path)));

      Expression<?, String> label = toColumnRef(parser, link.labelColumn, toAlias, path);
      value = showLastOnly ? label :
              lastToFirst  ? new Concatenation(ctx, asList(label, labelSeparator, value)) :
                             new Concatenation(ctx, asList(value, labelSeparator, label));
      fromAlias = toAlias;
    }
    if (firstSourceId instanceof NullLiteral) {
      SelectBuilder builder = new SelectBuilder(ctx);
      if (matching != null) {
        Expression<?, String> where = ColumnRef.qualify(builder.parser.parseExpression(matching),
                                                        firstFromAlias, false);
        builder.where(where);
      }
      if (keywords != null && !keywords.trim().isEmpty()) {
        var match = "%" + String.join("%", keywords.split("\\W+")) + "%";
        builder.and(new ILike(ctx, false, value, new StringLiteral(ctx, match)));
        if (showCode) {
          builder.or(new ILike(ctx, false, firstTargetId, new StringLiteral(ctx, match)));
        }
      }
      if (offset != null) {
        builder.offset((Expression<?, String>)offset);
      }
      if (limit != null) {
        builder.limit((Expression<?, String>)limit);
      }
      return builder.distinct(true)
                    .unfiltered(unfiltered)
                    .column  (firstTargetId, "code")
                    .column  (value,         "label")
                    .from    (from)
                    .and     (new IsNull(ctx, true, firstTargetId))
                    .and     (new IsNull(ctx, true, value))
                    .orderBy ("2")
                    .build   ();
    } else {
      return new SelectExpression(ctx,
                                  new SelectBuilder(ctx).unfiltered(unfiltered)
                                                        .column(value, "label")
                                                        .from  (from)
                                                        .where (new Equality(ctx, firstTargetId, firstSourceId))
                                                        .build ());
    }
  }

  private record Link(Expression<?, ?> sourceId,
                      String           targetId,
                      String           labelColumn,
                      String           targetTable) {}

  static boolean getBooleanParam(NamedArgument namedArg,
                                 String        argName,
                                 EsqlPath      path) {
    Object value = namedArg.arg().exec(ESQL, null, path, namedArg.context.structure);
    if (value != null && !(value instanceof Boolean)) {
      throw new TranslationException(argName + " must be a boolean value (" + namedArg.arg() + " was provided)");
    }
    return value != null && (Boolean)value;
  }

  static String getStringParam(NamedArgument namedArg,
                               String        argName,
                               EsqlPath      path) {
    Object value = namedArg.arg().exec(ESQL, null, path, namedArg.context.structure);
    if (value != null && !(value instanceof String)) {
      throw new TranslationException(argName + " must be a string value (" + namedArg.arg() + " was provided)");
    }
    return (String)value;
  }

  static Expression<?, String> toColumnRef(Parser   parser,
                                           Object   expr,
                                           String   qualifier,
                                           EsqlPath path) {
    Expression<?, String> e = expr instanceof String s        ? parser.parseExpression(s)
                            : expr instanceof StringLiteral s ? parser.parseExpression((String)s.exec(ESQL, null, path, s.context.structure))
                            : (Expression<?, String>)expr;
    return qualifier == null ? e : qualify(e, qualifier);
  }
}