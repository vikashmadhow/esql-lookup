package ma.vi.esql.lookup;

import java.util.UUID;

/**
 * @author Vikash Madhow (vikash.madhow@gmail.com)
 */
public record Lookup(UUID id,
                     String name,
                     String displayName,
                     String description,
                     LookupLink... links) {}
