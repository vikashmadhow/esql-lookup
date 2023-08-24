package ma.vi.esql.lookup;

import ma.vi.esql.lookup.init.LookupInitializer;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

/**
 * @author Vikash Madhow (vikash.madhow@gmail.com)
 */
class InitTest extends DataTest {
  @TestFactory
  Stream<DynamicTest> initLookups() {
    return Stream.of(databases)
                 .map(db -> dynamicTest(db.target().toString(), () -> {
                   System.out.println(db.target());
                   new LookupInitializer().add(db, "/init/test_lookups.yml", InitTest.class.getResourceAsStream("/init/test_lookups.yml"));

                   LookupExtension ext = db.extension(LookupExtension.class);
                   Lookup lookup = ext.loadLookup("TestCountry");
                   assertEquals("TestCountry", lookup.name());
                   assertEquals("Country", lookup.displayName());
                   assertEquals("Countries", lookup.description());
                   assertEquals("Geography", lookup.group());

                   assertEquals(2, lookup.links().size());
                   Map<String, Lookup> linkMap = lookup.links().stream()
                                                       .collect(Collectors.toMap(Lookup::name, l -> l));

                   assertEquals(ext.loadLookup("TestCurrency"),
                                linkMap.get("TestCurrency"));
                   assertEquals(ext.loadLookup("TestRegionalGrouping"),
                                linkMap.get("TestRegionalGrouping"));

                   System.out.println(lookup.values().values());
                 }));
  }
}
