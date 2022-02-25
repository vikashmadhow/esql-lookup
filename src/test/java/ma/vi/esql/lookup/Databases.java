/*
 * Copyright (c) 2020 Vikash Madhow
 */

package ma.vi.esql.lookup;

import ma.vi.base.config.Configuration;
import ma.vi.esql.database.Database;
import ma.vi.esql.database.Postgresql;
import ma.vi.esql.database.SqlServer;

import java.util.Map;
import java.util.Set;

import static ma.vi.esql.database.Database.*;

/**
 * @author Vikash Madhow (vikash.madhow@gmail.com)
 */
public class Databases {
  public static Postgresql Postgresql() {
    if (postgresql == null) {
      postgresql = new Postgresql(Configuration.of(
          CONFIG_DB_NAME, "test",
          CONFIG_DB_USER, "test",
          CONFIG_DB_PASSWORD, "test",
          CONFIG_DB_CREATE_CORE_TABLES, true,
          CONFIG_DB_EXTENSIONS, Map.of(LookupExtension.class, Configuration.of("schema", "_platform.lookup"))));
    }
    return postgresql;
  }

  public static SqlServer SqlServer() {
    if (sqlServer == null) {
      sqlServer = new SqlServer(Configuration.of(
          CONFIG_DB_NAME, "test",
          CONFIG_DB_USER, "test",
          CONFIG_DB_PASSWORD, "test",
          CONFIG_DB_CREATE_CORE_TABLES, true,
          CONFIG_DB_EXTENSIONS, Map.of(LookupExtension.class, Configuration.EMPTY)));
    }
    return sqlServer;
  }

  public static Database[] databases() {
    return new Database[] {
        Postgresql(),
        SqlServer()
    };
  }

  private static SqlServer sqlServer;
  private static Postgresql postgresql;
}
