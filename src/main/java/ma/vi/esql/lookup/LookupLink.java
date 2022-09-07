package ma.vi.esql.lookup;

/**
 * @author Vikash Madhow (vikash.madhow@gmail.com)
 */
public record LookupLink(String name,
                         String displayName,
                         Lookup target) {}
