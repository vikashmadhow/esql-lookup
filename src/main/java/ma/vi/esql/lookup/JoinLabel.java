/*
 * Copyright (c) 2021 Vikash Madhow
 */

package ma.vi.esql.lookup;

import ma.vi.esql.builder.SelectBuilder;
import ma.vi.esql.function.Function;
import ma.vi.esql.parser.*;
import ma.vi.esql.parser.expression.*;
import ma.vi.esql.parser.query.JoinTableExpr;
import ma.vi.esql.parser.query.SingleTableExpr;
import ma.vi.esql.parser.query.TableExpr;
import ma.vi.esql.type.Types;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static ma.vi.base.string.Strings.random;
import static ma.vi.esql.parser.Translatable.Target.ESQL;
import static ma.vi.esql.parser.expression.ColumnRef.qualify;

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
 * joinlabel can have the following optional named arguments to control the
 * value displayed:
 * <ul>
 * <li><b>show_last_only:</b> Show the last label element in the chain only
 *                            (a -&gt; b -&gt; c, show c only). Default is false.</li>
 * <li><b>label_separator:</b> an expression for the separator between the
 *                             labels from different tables. Default is '/'.</li>
 * <li><b>last_to_first:</b> Shows the names from the link tables from the
 *                           last linked table to the first, if true, or otherwise,
 *                           from the first to the last. Default is true.</li>
 * </ul>
 * @author Vikash Madhow (vikash.madhow@gmail.com)
 */
public class JoinLabel extends Function implements Macro {
  public JoinLabel() {
    super("joinlabel", Types.StringType, emptyList());
  }

  @Override
  public boolean expand(String name, Esql<?, ?> esql) {
    FunctionCall call = (FunctionCall)esql;
    Context ctx = call.context;
    List<Expression<?>> arguments = call.arguments();

    if (arguments.size() < 4) {
      throw new TranslationException("joinlabel needs at least 4 arguments: "
                                   + "the column or expression to link to the target table containing the label, "
                                   + "the column or expression in the target table label to link to, "
                                   + "the column or expression in the target table label to generate the label and "
                                   + "the target table containing the label. E.g., "
                                   + "joinlabel(company_id, 'id', 'name', 'Company')");
    }

    /*
     * Load arguments
     */
    List<Link> links = new ArrayList<>();
    boolean showLastOnly = false;                                         // show only the last element (last linked foreign table) of the join.
    Expression<?> labelSeparator = new StringLiteral(ctx, "' / '"); // the separator to use between labels from different table (joins).
    boolean lastToFirst = true;                                           // show labels last to first (or first to last if false).

    Iterator<Expression<?>> i = arguments.iterator();
    while (i.hasNext()) {
      Expression<?> arg = i.next();
      if (arg instanceof NamedArgument) {
        NamedArgument namedArg = (NamedArgument)arg;
        switch (namedArg.name()) {
          case "show_last_only"  -> showLastOnly = getBooleanParam(namedArg, "show_last_only");
          case "last_to_first"   -> lastToFirst = getBooleanParam(namedArg, "last_to_first");
          case "label_separator" -> labelSeparator = namedArg.arg();
          default                -> throw new TranslationException("Invalid named argument in joinlabel: " + namedArg.name());
        }
      } else {
        /*
         * link arguments consist of 3 parts: the id expression, the name expression and the target table
         */
        if (!i.hasNext()) {
          throw new TranslationException("joinlabel needs a source id, a target id, a label and a target table for each"
                                       + "link. Only the source id was provided for one link.");
        }
        String targetId = ((StringLiteral)i.next()).value(ESQL);

        if (!i.hasNext()) {
          throw new TranslationException("joinlabel needs a source id, a target id, a label and a target table for each "
                                       + "link. Only the source id and target id were provided for one link.");
        }
        String label = ((StringLiteral)i.next()).value(ESQL);

        if (!i.hasNext()) {
          throw new TranslationException("joinlabel needs a source id, a target id, a label and a target table for each "
                                       + "link. Only the source id, target id and label were provided for one link.");
        }
        String table = ((StringLiteral)i.next()).value(ESQL);
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
    int alias = 0;
    String unique = "t" + random(10) + "_";
    Iterator<Link> linkIter = links.iterator();
    Link link = linkIter.next();

    Parser parser = new Parser(ctx.structure);
    Expression<?> firstSourceId = link.sourceId;
    String fromAlias = unique + alias;
    Expression<?> firstTargetId = toColumnRef(parser, link.targetId, fromAlias);
    Expression<?> value = toColumnRef(parser, link.labelColumn, fromAlias);
    TableExpr from = new SingleTableExpr(ctx, link.targetTable, fromAlias);

    while (linkIter.hasNext()) {
      alias++;
      String toAlias = unique + alias;

      link = linkIter.next();
      from = new JoinTableExpr(ctx,
                               from, null,
                               new SingleTableExpr(ctx, link.targetTable, toAlias),
                               new Equality(ctx,
                                            toColumnRef(parser, link.sourceId, fromAlias),
                                            toColumnRef(parser, link.targetId, toAlias)));

      Expression<?> label = toColumnRef(parser, link.labelColumn, toAlias);
      value = showLastOnly ? label :
              lastToFirst  ? new Concatenation(ctx, asList(label, labelSeparator, value)) :
                             new Concatenation(ctx, asList(value, labelSeparator, label));
      fromAlias = toAlias;
    }
    call.parent.replaceWith(
        name,
        new SelectExpression(ctx,
                             new SelectBuilder(ctx)
                                 .column(value, null)
                                 .from(from)
                                 .where(new Equality(ctx, firstTargetId, firstSourceId))
                                 .orderBy(firstTargetId, "asc")
                                 .limit("1")
                                 .build()));
    return true;
  }

  private static class Link {
    public Link(Expression<?> sourceId, String targetId, String labelColumn, String targetTable) {
      this.sourceId = sourceId;
      this.targetId = targetId;
      this.labelColumn = labelColumn;
      this.targetTable = targetTable;
    }

    public final Expression<?> sourceId;
    public final String targetId;
    public final String labelColumn;
    public final String targetTable;
  }

  static boolean getBooleanParam(NamedArgument namedArg, String argName) {
    Object value = namedArg.arg().value(null);
    if (value != null && !(value instanceof Boolean)) {
      throw new TranslationException(argName + " must be a boolean value (" + namedArg.arg() + " was provided)");
    }
    return value != null && (Boolean)value;
  }

  static Expression<?> toColumnRef(Parser parser, Object expr, String qualifier) {
    Expression<?> e = expr instanceof String        ? parser.parseExpression((String)expr) :
                      expr instanceof StringLiteral ? parser.parseExpression(((StringLiteral)expr).value(ESQL)) : (Expression<?>)expr;
    return qualifier == null ? e : qualify(e, qualifier, null, true);
  }
}