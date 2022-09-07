package ma.vi.esql.lookup;

import java.util.List;
import java.util.UUID;

/**
 * @author Vikash Madhow (vikash.madhow@gmail.com)
 */
public record LookupValue(UUID   id,
                          String lookup,
                          String code,
                          String altCode1,
                          String altCode2,
                          String label,
                          String description,
                          String lang,
                          List<LookupValueLink> links) {}
