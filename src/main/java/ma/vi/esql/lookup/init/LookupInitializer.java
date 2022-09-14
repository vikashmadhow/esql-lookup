package ma.vi.esql.lookup.init;

import ma.vi.base.collections.ArrayIterator;
import ma.vi.esql.database.Database;
import ma.vi.esql.database.init.Initializer;
import ma.vi.esql.lookup.Lookup;
import ma.vi.esql.lookup.LookupExtension;
import ma.vi.esql.lookup.LookupValue;
import ma.vi.esql.lookup.LookupValueLink;

import java.util.*;

/**
 * Create/update lookups in the  database from hierarchical definitions, such
 * as, for example, those contained in a YAML file.
 * <p>
 * An example of the expected input format:
 * <pre>
 *   Country:
 *     displayName: Country
 *     description: Countries
 *     group: Geography
 *     links:
 *       - Currency
 *       - RegionalGrouping
 *     values:
 *       -AF,AFG,4,Afghanistan|ALL|SADC
 *       -AL,ALB,8,Albania|ARS|SADC
 *       -DZ,DZA,12,Algeria|DZD|SADC
 *       -AU,AUS,36,Australia|AUD|COMESA
 *       ...
 * </pre>
 *
 * The format of lookup values are as follows:
 * <pre>
 *   code[,alt_code1][,alt_code2],label[,description][,lang][|link codes]*
 * </pre>
 * Commas (,) in the values can be escaped by preceding with a backslash (\).
 *
 * @author Vikash Madhow (vikash.madhow@gmail.com)
 */
public class LookupInitializer implements Initializer<Lookup> {
  @Override
  public Lookup add(Database db,
                    boolean  overwrite,
                    String   name,
                    Lookup   existing,
                    Map<String, Object> definition) {
    LookupExtension ext = db.extension(LookupExtension.class);
    if (overwrite || ext.findLookup(name).isEmpty()) {
      existing = new Lookup(name,
                            (String)definition.get("group"),
                            (String)definition.get("displayName"),
                            (String)definition.get("description"),
                            new ArrayList<>(),
                            new HashMap<>(),
                            new HashMap<>());
      List<String> links = (List<String>)definition.get("links");
      if (links != null) {
        for (String link: links) {
          existing.links().add(ext.loadLookup(link));
        }
      }
      List<String> values = (List<String>)definition.get("values");
      if (values != null) {
        for (String value: values) {
          String code;
          String label;
          String altCode1 = null;
          String altCode2 = null;
          String description = null;
          String lang = null;

          String suffix = null;
          int pos = value.indexOf('|');
          if (pos != -1) {
            suffix = value.substring(pos + 1);
            value = value.substring(0, pos);
          }

          value = value.replace("\\,", "\ue000");
          Iterator<String> i = new ArrayIterator<>(value.split(","));
          code = clean(i.next());

          if (!i.hasNext()) {
            throw new IllegalArgumentException("Missing label from value: " + value);
          }
          String part = clean(i.next());
          if (i.hasNext()) {
            altCode1 = part;
            part = clean(i.next());
            if (i.hasNext()) {
              altCode2 = part;
              label = clean(i.next());
            } else {
              label = part;
            }
          } else {
            label = part;
          }
          if (i.hasNext()) description = clean(i.next());
          if (i.hasNext()) lang = clean(i.next());

          LookupValue lv = new LookupValue(
              UUID.randomUUID(),
              name,
              code,
              altCode1,
              altCode2,
              label,
              description,
              lang,
              new ArrayList<>());

          if (suffix != null) {
            String[] linkValues = suffix.split("\\|");
            if (linkValues.length != existing.links().size()) {
              throw new IllegalArgumentException(linkValues.length + " linked codes "
                                              + "were provided but " + existing.links().size()
                                              + " links have been defined on the lookup "
                                              + existing.name());
            }
            for (int j = 0; j < linkValues.length; j++) {
              Lookup linkDef = existing.links().get(j);
              lv.links().add(new LookupValueLink(linkDef.name(),
                                                 ext.loadLookupValue(linkDef.name(),
                                                                     linkValues[j])));
            }
          }
          existing.values().put(lv.code(),   lv);
          existing.valuesById().put(lv.id(), lv);
        }
      }
      ext.saveLookup(existing);
    }
    return existing;
  }

  private String clean(String s) {
    return s.replace("\ue000", ",").trim();
  }

  @Override
  public Lookup get(Database db, String name) {
    return db.extension(LookupExtension.class)
             .findLookup(name)
             .orElse(null);
  }
}
