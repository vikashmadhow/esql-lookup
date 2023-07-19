package ma.vi.esql.lookup;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.util.stream.Collectors.toMap;

/**
 * @author Vikash Madhow (vikash.madhow@gmail.com)
 */
public record Lookup(UUID      id,
                     String    name,
                     String    group,
                     String    displayName,
                     String    description,
                     List<Lookup> links,
                     Map<String, LookupValue> values,
                     Map<UUID,   LookupValue> valuesById) {

  public Lookup(String    name,
                String    group,
                String    displayName,
                String    description,
                List<Lookup> links,
                Map<String, LookupValue> values,
                Map<UUID,   LookupValue> valuesById) {
    this(UUID.randomUUID(),
         name,
         group,
         displayName,
         description,
         links,
         values,
         valuesById);
  }

  Lookup() {
    this(null, null, null, null, null, null, null);
  }

  /**
   * Lookup code to match by
   */
  public enum MatchBy { code, altCode1, altCode2 }

  public Map<String, LookupValue> mapBy() {
    return mapBy(MatchBy.code);
  }

  public Map<String, LookupValue> mapBy(MatchBy matchBy) {
    return matchBy == MatchBy.code ? values
         : values.values().stream()
                 .collect(toMap(v -> matchBy == MatchBy.altCode1 ? v.altCode1() : v.altCode2(),
                                v -> v));
  }
}