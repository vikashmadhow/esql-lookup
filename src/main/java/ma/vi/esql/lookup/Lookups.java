package ma.vi.esql.lookup;

import ma.vi.esql.database.Database;
import ma.vi.esql.database.Extension;
import ma.vi.esql.database.Structure;
import ma.vi.esql.exec.EsqlConnection;
import ma.vi.esql.parser.Parser;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;

import static java.lang.System.Logger.Level.INFO;
import static ma.vi.esql.parser.Translatable.Target.POSTGRESQL;
import static ma.vi.esql.parser.Translatable.Target.SQLSERVER;

/**
 * @author Vikash Madhow (vikash.madhow@gmail.com)
 */
public class Lookups implements Extension {
  @Override
  public void init(Database db) {
    log.log(INFO, "Creating lookup tables in " + db);
    try (EsqlConnection c = db.esql(db.pooledConnection())) {
      Parser p = new Parser(db.structure());

      ///////////////////////////////////////////////////////////////////////////
      // Create lookup tables
      ///////////////////////////////////////////////////////////////////////////
      c.exec(p.parse(
          "create table _platform.lookup.Lookup drop undefined({" +
          "  name: 'Lookup', " +
          "  description: 'A named table of values', " +
          "  dependents: {" +
          "    links: {" +
          "      type: '_platform.lookup.LookupLink'," +
          "      referred_by: 'source_lookup_id'," +
          "      label: 'Lookup links'" +
          "    }" +
          "  }" +
          "}," +

          "_id uuid not null, " +
          "_version long not null default 0, " +
          "_can_delete bool not null default true, " +
          "_can_edit bool not null default true, " +

          "name string not null {" +
          "  validate_unique: true, " +
          "  mask: 'Iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii'" +
          "}, " +
          "description string, " +

          "primary key(_id), " +
          "unique(name))"));

      c.exec(p.parse(
          "create table _platform.lookup.LookupLink drop undefined({" +
          "  name: 'Lookup Link', " +
          "  description: 'The definition of links between values of lookup tables which are used for searching data by associations and for aggregating data in reports'" +
          "}, " +

          "_id uuid not null, " +
          "_version long not null default 0, " +
          "_can_delete bool not null default true, " +
          "_can_edit bool not null default true, " +

          "name string not null {" +
          "  validate_unique: true, " +
          "  mask: 'Iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii', " +
          "  description: 'Start with a letter, follow by letters or digits' " +
          "}, " +

          "display_name string not null, " +

          "source_lookup_id uuid not null {" +
          "  show: false" +
          "}, " +

          "target_lookup_id uuid not null {" +
          "  label: 'Target', " +
          "  value_table: '_platform.lookup.Lookup', " +
          "  value_id: '_id', " +
          "  value_label: 'name', " +
          "  show_value_as: joinlabel(target_lookup_id, '_id', 'name', '_platform.lookup.Lookup')" +
          "}, " +

          "primary key(_id), " +
          "unique(name), " +
          "foreign key(source_lookup_id) references _platform.lookup.Lookup(_id), " +
          "foreign key(target_lookup_id) references _platform.lookup.Lookup(_id))"));

      c.exec(p.parse(
          "create table _platform.lookup.LookupValue drop undefined({" +
          "  name: 'Lookup Value', " +
          "  description: 'The values in a lookup table'," +
          "  validate_unique: [['lookup_id', 'code', 'lang']] " +
          "}, " +

          "_id uuid not null, " +
          "_version long not null default 0, " +
          "_can_delete bool not null default true, " +
          "_can_edit bool not null default true, " +

          "lookup_id uuid not null {" +
          "  show:false " +
          "}, " +
          "code string not null, " +
          "alt_code1 string, " +
          "alt_code2 string, " +
          "label string not null, " +
          "lang string not null default 'en' {" +
          "  label: 'Language', " +
          "  initial_value: 'en' " +
          "}, " +

          "primary key(_id), " +
          "unique(lookup_id, code, lang), " +
          "foreign key(lookup_id) references _platform.lookup.Lookup(_id) on delete cascade on update cascade)"));

      c.exec(p.parse(
          "create table _platform.lookup.LookupValueLink drop undefined({" +
          "  name: 'Lookup Value Link', " +
          "  description: 'Links between values of lookup tables, used primarily for searching data by associations and for aggregating data in reports'" +
          "}, " +

          "_id uuid not null, " +
          "_version long not null default 0, " +
          "_can_delete bool not null default true, " +
          "_can_edit bool not null default true, " +

          "name string not null, " +
          "source_value_id uuid not null, " +
          "target_value_id uuid not null, " +

          "primary key(_id), " +
          "foreign key(source_value_id) references _platform.lookup.LookupValue(_id), " +
          "foreign key(target_value_id) references _platform.lookup.LookupValue(_id))"));
    }

    /*
     * Lookup macros
     */

    // labels functions and macros
    ////////////////////////////////
    Structure structure = db.structure();
    structure.function(new LookupLabelFunction());
    structure.function(new LookupLabel());
    structure.function(new JoinLabel());

    /*
     * Create lookup access functions specific to each database (only
     * required when using the function-based lookup label resolution).
     */
    if (db.target() == POSTGRESQL) {
      try (Connection c = db.pooledConnection(true, -1)) {
        // lookup label with no links
        c.createStatement().executeUpdate("create or replace function _core.lookup_label(code text,\n" +
                                              "                                              lookup text,\n" +
                                              "                                              show_code boolean,\n" +
                                              "                                              show_label boolean) returns text as $$\n" +
                                              "    select case when coalesce(show_code, false)=coalesce(show_label, false)\n" +
                                              "                then v.code || ' - ' || v.label\n" +
                                              "\n" +
                                              "                when coalesce(show_code, false)=true\n" +
                                              "                then v.code\n" +
                                              "\n" +
                                              "                else v.label\n" +
                                              "           end\n" +
                                              "      from \"_platform.lookup\".\"LookupValue\" v\n" +
                                              "      join \"_platform.lookup\".\"Lookup\" l on v.lookup_id=l._id\n" +
                                              "     where l.name=$2 and v.code=$1;\n" +
                                              "$$ language sql immutable;");

        // lookup label with variable number of links
        c.createStatement().executeUpdate("create or replace function _core.lookup_label(code text,\n" +
                                              "                                              lookup text,\n" +
                                              "                                              show_code boolean,\n" +
                                              "                                              show_label boolean,\n" +
                                              "                                              variadic links text[]) returns text as $$\n" +
                                              "declare\n" +
                                              "    link_name text;\n" +
                                              "    link_index int = 0;\n" +
                                              "\n" +
                                              "    label_clause text = '';\n" +
                                              "    from_clause text = '';\n" +
                                              "    query text;\n" +
                                              "    result text;\n" +
                                              "\n" +
                                              "begin\n" +
                                              "    from_clause := '\"_platform.lookup\".\"LookupValue\" v0 '\n" +
                                              "                        || 'join \"_platform.lookup\".\"Lookup\" lookup '\n" +
                                              "                        || 'on (v0.lookup_id=lookup._id and lookup.name=''' || lookup || ''')';\n" +
                                              "\n" +
                                              "    foreach link_name in array links loop\n" +
                                              "        -- source side\n" +
                                              "        from_clause := from_clause || ' join \"_platform.lookup\".\"LookupValueLink\" lk' || link_index\n" +
                                              "                                   || ' on (v' || link_index || '._id=lk' || link_index\n" +
                                              "                                   || '.source_value_id and ' || 'lk' || link_index\n" +
                                              "                                   || '.name=''' || link_name || ''')';\n" +
                                              "\n" +
                                              "        link_index := link_index + 1;\n" +
                                              "\n" +
                                              "        -- target side\n" +
                                              "        from_clause := from_clause || ' join \"_platform.lookup\".\"LookupValue\" v' || link_index\n" +
                                              "                                   || ' on (v' || link_index || '._id=lk' || (link_index - 1)\n" +
                                              "                                   || '.target_value_id)';\n" +
                                              "    end loop;\n" +
                                              "\n" +
                                              "    if coalesce(show_code, false)=coalesce(show_label, false) then\n" +
                                              "        label_clause := 'v' || link_index || '.code || '' - '' || v' || link_index || '.label';\n" +
                                              "    elsif coalesce(show_code, false)=true then\n" +
                                              "        label_clause := 'v' || link_index || '.code';\n" +
                                              "    else\n" +
                                              "        label_clause := 'v' || link_index || '.label';\n" +
                                              "    end if;\n" +
                                              "\n" +
                                              "    query := 'select ' || label_clause\n" +
                                              "                       || ' from ' || from_clause\n" +
                                              "                       || ' where v0.code=''' || code || '''';\n" +
                                              "\n" +
                                              "    execute query into result;\n" +
                                              "    return result;\n" +
                                              "end;\n" +
                                              "$$ language plpgsql immutable;\n");
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    } else if (db.target() == SQLSERVER) {
      try (Connection c = db.pooledConnection(true, -1)) {
        // function to find value from lookups
        c.createStatement().executeUpdate(
            "create or alter function _core.lookup_label0(@Code nvarchar(max),\n" +
                "                                             @Lookup nvarchar(max),\n" +
                "                                             @ShowCode bit,\n" +
                "                                             @ShowLabel bit) returns nvarchar(max) as\n" +
                "begin\n" +
                "  declare @Result nvarchar(max);\n" +
                "  select @Result=(iif(coalesce(@ShowCode, 0)=coalesce(@ShowLabel, 0),\n" +
                "                      code + ' - '+ label,\n" +
                "                      iif(coalesce(@ShowCode, 0)=1,\n" +
                "                          code,\n" +
                "                          label)))\n" +
                "    from \"_platform.lookup\".\"LookupValue\" v\n" +
                "    join \"_platform.lookup\".\"Lookup\" l on v.lookup_id=l._id\n" +
                "  where l.name=@Lookup and v.code=@Code;\n" +
                "  return @Result;\n" +
                "end;");

        c.createStatement().executeUpdate(
            "create or alter function _core.lookup_label1(@Code nvarchar(max),\n" +
            "                                             @Lookup nvarchar(max),\n" +
            "                                             @ShowCode bit,\n" +
            "                                             @ShowLabel bit,\n" +
            "                                             @Link1 nvarchar(max)) returns nvarchar(max) as\n" +
            "begin\n" +
            "  declare @LinkCursor Cursor;\n" +
            "  declare @Result nvarchar(max);\n" +
            "\n" +
            "  select @Result=(iif(coalesce(@ShowCode, 0)=coalesce(@ShowLabel, 0),\n" +
            "                      v1.code + ' - '+ v1.label,\n" +
            "                      iif(coalesce(@ShowCode, 0)=1,\n" +
            "                          v1.code,\n" +
            "                          v1.label)))\n" +
            "\n" +
            "    from \"_platform.lookup\".\"LookupValue\" v0\n" +
            "    join \"_platform.lookup\".\"Lookup\" lookup on (v0.lookup_id=lookup._id and lookup.name=@Lookup)\n" +
            "\n" +
            "    join \"_platform.lookup\".\"LookupValueLink\" lk0 on (v0._id=lk0.source_value_id and lk0.name=@Link1)\n" +
            "    join \"_platform.lookup\".\"LookupValue\" v1 on v1._id=lk0.target_value_id\n" +
            "\n" +
            "   where v0.code=@Code;\n" +
            "  return @Result;\n" +
            "end;");

        c.createStatement().executeUpdate(
            "create or alter function _core.lookup_label2(@Code nvarchar(max),\n" +
            "                                             @Lookup nvarchar(max),\n" +
            "                                             @ShowCode bit,\n" +
            "                                             @ShowLabel bit,\n" +
            "                                             @Link1 nvarchar(max),\n" +
            "                                             @Link2 nvarchar(max)) returns nvarchar(max) as\n" +
            "begin\n" +
            "  declare @LinkCursor Cursor;\n" +
            "  declare @Result nvarchar(max);\n" +
            "\n" +
            "  select @Result=(iif(coalesce(@ShowCode, 0)=coalesce(@ShowLabel, 0),\n" +
            "                      v2.code + ' - '+ v2.label,\n" +
            "                      iif(coalesce(@ShowCode, 0)=1,\n" +
            "                          v2.code,\n" +
            "                          v2.label)))\n" +
            "\n" +
            "    from \"_platform.lookup\".\"LookupValue\" v0\n" +
            "    join \"_platform.lookup\".\"Lookup\" lookup on (v0.lookup_id=lookup._id and lookup.name=@Lookup)\n" +
            "\n" +
            "    join \"_platform.lookup\".\"LookupValueLink\" lk0 on (v0._id=lk0.source_value_id and lk0.name=@Link1)\n" +
            "    join \"_platform.lookup\".\"LookupValue\" v1 on v1._id=lk0.target_value_id\n" +
            "\n" +
            "    join \"_platform.lookup\".\"LookupValueLink\" lk1 on (v1._id=lk1.source_value_id and lk1.name=@Link2)\n" +
            "    join \"_platform.lookup\".\"LookupValue\" v2 on v2._id=lk1.target_value_id\n" +
            "\n" +
            "   where v0.code=@Code;\n" +
            "  return @Result;\n" +
            "end;");

        c.createStatement().executeUpdate(
            "create or alter function _core.lookup_label3(@Code nvarchar(max),\n" +
            "                                             @Lookup nvarchar(max),\n" +
            "                                             @ShowCode bit,\n" +
            "                                             @ShowLabel bit,\n" +
            "                                             @Link1 nvarchar(max),\n" +
            "                                             @Link2 nvarchar(max),\n" +
            "                                             @Link3 nvarchar(max)) returns nvarchar(max) as\n" +
            "begin\n" +
            "  declare @LinkCursor Cursor;\n" +
            "  declare @Result nvarchar(max);\n" +
            "\n" +
            "  select @Result=(iif(coalesce(@ShowCode, 0)=coalesce(@ShowLabel, 0),\n" +
            "                      v3.code + ' - '+ v3.label,\n" +
            "                      iif(coalesce(@ShowCode, 0)=1,\n" +
            "                          v3.code,\n" +
            "                          v3.label)))\n" +
            "\n" +
            "    from \"_platform.lookup\".\"LookupValue\" v0\n" +
            "    join \"_platform.lookup\".\"Lookup\" lookup on (v0.lookup_id=lookup._id and lookup.name=@Lookup)\n" +
            "\n" +
            "    join \"_platform.lookup\".\"LookupValueLink\" lk0 on (v0._id=lk0.source_value_id and lk0.name=@Link1)\n" +
            "    join \"_platform.lookup\".\"LookupValue\" v1 on v1._id=lk0.target_value_id\n" +
            "\n" +
            "    join \"_platform.lookup\".\"LookupValueLink\" lk1 on (v1._id=lk1.source_value_id and lk1.name=@Link2)\n" +
            "    join \"_platform.lookup\".\"LookupValue\" v2 on v2._id=lk1.target_value_id\n" +
            "\n" +
            "    join \"_platform.lookup\".\"LookupValueLink\" lk2 on (v2._id=lk2.source_value_id and lk2.name=@Link3)\n" +
            "    join \"_platform.lookup\".\"LookupValue\" v3 on v3._id=lk2.target_value_id\n" +
            "\n" +
            "   where v0.code=@Code;\n" +
            "  return @Result;\n" +
            "end;");

        c.createStatement().executeUpdate(
            "create or alter function _core.lookup_label4(@Code nvarchar(max),\n" +
            "                                             @Lookup nvarchar(max),\n" +
            "                                             @ShowCode bit,\n" +
            "                                             @ShowLabel bit,\n" +
            "                                             @Link1 nvarchar(max),\n" +
            "                                             @Link2 nvarchar(max),\n" +
            "                                             @Link3 nvarchar(max),\n" +
            "                                             @Link4 nvarchar(max)) returns nvarchar(max) as\n" +
            "begin\n" +
            "  declare @LinkCursor Cursor;\n" +
            "  declare @Result nvarchar(max);\n" +
            "\n" +
            "  select @Result=(iif(coalesce(@ShowCode, 0)=coalesce(@ShowLabel, 0),\n" +
            "                      v4.code + ' - '+ v4.label,\n" +
            "                      iif(coalesce(@ShowCode, 0)=1,\n" +
            "                          v4.code,\n" +
            "                          v4.label)))\n" +
            "\n" +
            "    from \"_platform.lookup\".\"LookupValue\" v0\n" +
            "    join \"_platform.lookup\".\"Lookup\" lookup on (v0.lookup_id=lookup._id and lookup.name=@Lookup)\n" +
            "\n" +
            "    join \"_platform.lookup\".\"LookupValueLink\" lk0 on (v0._id=lk0.source_value_id and lk0.name=@Link1)\n" +
            "    join \"_platform.lookup\".\"LookupValue\" v1 on v1._id=lk0.target_value_id\n" +
            "\n" +
            "    join \"_platform.lookup\".\"LookupValueLink\" lk1 on (v1._id=lk1.source_value_id and lk1.name=@Link2)\n" +
            "    join \"_platform.lookup\".\"LookupValue\" v2 on v2._id=lk1.target_value_id\n" +
            "\n" +
            "    join \"_platform.lookup\".\"LookupValueLink\" lk2 on (v2._id=lk2.source_value_id and lk2.name=@Link3)\n" +
            "    join \"_platform.lookup\".\"LookupValue\" v3 on v3._id=lk2.target_value_id\n" +
            "\n" +
            "    join \"_platform.lookup\".\"LookupValueLink\" lk3 on (v3._id=lk3.source_value_id and lk3.name=@Link4)\n" +
            "    join \"_platform.lookup\".\"LookupValue\" v4 on v4._id=lk3.target_value_id\n" +
            "\n" +
            "   where v0.code=@Code;\n" +
            "  return @Result;\n" +
            "end;");

        c.createStatement().executeUpdate(
            "create or alter function _core.lookup_label5(@Code nvarchar(max),\n" +
            "                                             @Lookup nvarchar(max),\n" +
            "                                             @ShowCode bit,\n" +
            "                                             @ShowLabel bit,\n" +
            "                                             @Link1 nvarchar(max),\n" +
            "                                             @Link2 nvarchar(max),\n" +
            "                                             @Link3 nvarchar(max),\n" +
            "                                             @Link4 nvarchar(max),\n" +
            "                                             @Link5 nvarchar(max)) returns nvarchar(max) as\n" +
            "begin\n" +
            "  declare @LinkCursor Cursor;\n" +
            "  declare @Result nvarchar(max);\n" +
            "\n" +
            "  select @Result=(iif(coalesce(@ShowCode, 0)=coalesce(@ShowLabel, 0),\n" +
            "                      v5.code + ' - '+ v5.label,\n" +
            "                      iif(coalesce(@ShowCode, 0)=1,\n" +
            "                          v5.code,\n" +
            "                          v5.label)))\n" +
            "\n" +
            "    from \"_platform.lookup\".\"LookupValue\" v0\n" +
            "    join \"_platform.lookup\".\"Lookup\" lookup on (v0.lookup_id=lookup._id and lookup.name=@Lookup)\n" +
            "\n" +
            "    join \"_platform.lookup\".\"LookupValueLink\" lk0 on (v0._id=lk0.source_value_id and lk0.name=@Link1)\n" +
            "    join \"_platform.lookup\".\"LookupValue\" v1 on v1._id=lk0.target_value_id\n" +
            "\n" +
            "    join \"_platform.lookup\".\"LookupValueLink\" lk1 on (v1._id=lk1.source_value_id and lk1.name=@Link2)\n" +
            "    join \"_platform.lookup\".\"LookupValue\" v2 on v2._id=lk1.target_value_id\n" +
            "\n" +
            "    join \"_platform.lookup\".\"LookupValueLink\" lk2 on (v2._id=lk2.source_value_id and lk2.name=@Link3)\n" +
            "    join \"_platform.lookup\".\"LookupValue\" v3 on v3._id=lk2.target_value_id\n" +
            "\n" +
            "    join \"_platform.lookup\".\"LookupValueLink\" lk3 on (v3._id=lk3.source_value_id and lk3.name=@Link4)\n" +
            "    join \"_platform.lookup\".\"LookupValue\" v4 on v4._id=lk3.target_value_id\n" +
            "\n" +
            "    join \"_platform.lookup\".\"LookupValueLink\" lk4 on (v4._id=lk4.source_value_id and lk4.name=@Link5)\n" +
            "    join \"_platform.lookup\".\"LookupValue\" v5 on v5._id=lk4.target_value_id\n" +
            "\n" +
            "   where v0.code=@Code;\n" +
            "  return @Result;\n" +
            "end;\n");
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public Set<Class<? extends Extension>> dependsOn() {
    return null;
  }

  private static final System.Logger log = System.getLogger(Lookups.class.getName());
}