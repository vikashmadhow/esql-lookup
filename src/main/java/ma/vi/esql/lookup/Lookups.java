package ma.vi.esql.lookup;

import ma.vi.esql.database.Database;
import ma.vi.esql.database.Extension;
import ma.vi.esql.database.Structure;
import ma.vi.esql.exec.EsqlConnection;
import ma.vi.esql.syntax.Parser;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;

import static java.lang.System.Logger.Level.INFO;
import static ma.vi.esql.syntax.Translatable.Target.POSTGRESQL;
import static ma.vi.esql.syntax.Translatable.Target.SQLSERVER;

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
          "create table _lookup.Lookup drop undefined({" +
          "  name: 'Lookup', " +
          "  description: 'A named table of values', " +
          "  dependents: {" +
          "    links: {" +
          "      type: '_lookup.LookupLink'," +
          "      referred_by: 'source_lookup_id'," +
          "      label: 'Lookup Links'" +
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
          "create table _lookup.LookupLink drop undefined({" +
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
          "  value_table: '_lookup.Lookup', " +
          "  value_id: '_id', " +
          "  value_label: 'name', " +
          "  show_value_as: joinlabel(target_lookup_id, '_id', 'name', '_lookup.Lookup')" +
          "}, " +

          "primary key(_id), " +
          "unique(name), " +
          "foreign key(source_lookup_id) references _lookup.Lookup(_id), " +
          "foreign key(target_lookup_id) references _lookup.Lookup(_id))"));

      c.exec(p.parse(
          "create table _lookup.LookupValue drop undefined({" +
          "  name: 'Lookup Value', " +
          "  description: 'The values in a lookup table'," +
          "  validate_unique: [['lookup_id', 'code', 'lang']], " +
          "  dependents: {" +
          "    links: {" +
          "      type: '_lookup.LookupValueLink'," +
          "      referred_by: 'source_value_id'," +
          "      label: 'Lookup Value Links'" +
          "    }" +
          "  }" +
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
          "foreign key(lookup_id) references _lookup.Lookup(_id) on delete cascade on update cascade)"));

      c.exec(p.parse(
          "create table _lookup.LookupValueLink drop undefined({" +
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
          "foreign key(source_value_id) references _lookup.LookupValue(_id), " +
          "foreign key(target_value_id) references _lookup.LookupValue(_id))"));
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
        c.createStatement().executeUpdate("""
                                          create or replace function _core.lookup_label(code text,
                                                                                        lookup text,
                                                                                        show_code boolean,
                                                                                        show_label boolean) returns text as $$
                                              select case when coalesce(show_code, false)=coalesce(show_label, false)
                                                          then v.code || ' - ' || v.label
                                          
                                                          when coalesce(show_code, false)=true
                                                          then v.code
                                          
                                                          else v.label
                                                     end
                                                from "_lookup"."LookupValue" v
                                                join "_lookup"."Lookup" l on v.lookup_id=l._id
                                               where l.name=$2 and v.code=$1;
                                          $$ language sql immutable;""");

        // lookup label with variable number of links
        c.createStatement().executeUpdate("""
                                              create or replace function _core.lookup_label(code text,
                                                                                            lookup text,
                                                                                            show_code boolean,
                                                                                            show_label boolean,
                                                                                            variadic links text[]) returns text as $$
                                              declare
                                                  link_name text;
                                                  link_index int = 0;

                                                  label_clause text = '';
                                                  from_clause text = '';
                                                  query text;
                                                  result text;

                                              begin
                                                  from_clause := '"_lookup"."LookupValue" v0 '
                                                                      || 'join "_lookup"."Lookup" lookup '
                                                                      || 'on (v0.lookup_id=lookup._id and lookup.name=''' || lookup || ''')';

                                                  foreach link_name in array links loop
                                                      -- source side
                                                      from_clause := from_clause || ' join "_lookup"."LookupValueLink" lk' || link_index
                                                                                 || ' on (v' || link_index || '._id=lk' || link_index
                                                                                 || '.source_value_id and ' || 'lk' || link_index
                                                                                 || '.name=''' || link_name || ''')';

                                                      link_index := link_index + 1;

                                                      -- target side
                                                      from_clause := from_clause || ' join "_lookup"."LookupValue" v' || link_index
                                                                                 || ' on (v' || link_index || '._id=lk' || (link_index - 1)
                                                                                 || '.target_value_id)';
                                                  end loop;

                                                  if coalesce(show_code, false)=coalesce(show_label, false) then
                                                      label_clause := 'v' || link_index || '.code || '' - '' || v' || link_index || '.label';
                                                  elsif coalesce(show_code, false)=true then
                                                      label_clause := 'v' || link_index || '.code';
                                                  else
                                                      label_clause := 'v' || link_index || '.label';
                                                  end if;

                                                  query := 'select ' || label_clause
                                                                     || ' from ' || from_clause
                                                                     || ' where v0.code=''' || code || '''';

                                                  execute query into result;
                                                  return result;
                                              end;
                                              $$ language plpgsql immutable;
                                              """);
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    } else if (db.target() == SQLSERVER) {
      try (Connection c = db.pooledConnection(true, -1)) {
        // function to find value from lookups
        c.createStatement().executeUpdate(
            """
                create or alter function _core.lookup_label0(@Code nvarchar(max),
                                                             @Lookup nvarchar(max),
                                                             @ShowCode bit,
                                                             @ShowLabel bit) returns nvarchar(max) as
                begin
                  declare @Result nvarchar(max);
                  select @Result=(iif(coalesce(@ShowCode, 0)=coalesce(@ShowLabel, 0),
                                      code + ' - '+ label,
                                      iif(coalesce(@ShowCode, 0)=1,
                                          code,
                                          label)))
                    from "_lookup"."LookupValue" v
                    join "_lookup"."Lookup" l on v.lookup_id=l._id
                  where l.name=@Lookup and v.code=@Code;
                  return @Result;
                end;""");

        c.createStatement().executeUpdate(
            """
                create or alter function _core.lookup_label1(@Code nvarchar(max),
                                                             @Lookup nvarchar(max),
                                                             @ShowCode bit,
                                                             @ShowLabel bit,
                                                             @Link1 nvarchar(max)) returns nvarchar(max) as
                begin
                  declare @LinkCursor Cursor;
                  declare @Result nvarchar(max);

                  select @Result=(iif(coalesce(@ShowCode, 0)=coalesce(@ShowLabel, 0),
                                      v1.code + ' - '+ v1.label,
                                      iif(coalesce(@ShowCode, 0)=1,
                                          v1.code,
                                          v1.label)))

                    from "_lookup"."LookupValue" v0
                    join "_lookup"."Lookup" lookup on (v0.lookup_id=lookup._id and lookup.name=@Lookup)

                    join "_lookup"."LookupValueLink" lk0 on (v0._id=lk0.source_value_id and lk0.name=@Link1)
                    join "_lookup"."LookupValue" v1 on v1._id=lk0.target_value_id

                   where v0.code=@Code;
                  return @Result;
                end;""");

        c.createStatement().executeUpdate(
            """
                create or alter function _core.lookup_label2(@Code nvarchar(max),
                                                             @Lookup nvarchar(max),
                                                             @ShowCode bit,
                                                             @ShowLabel bit,
                                                             @Link1 nvarchar(max),
                                                             @Link2 nvarchar(max)) returns nvarchar(max) as
                begin
                  declare @LinkCursor Cursor;
                  declare @Result nvarchar(max);

                  select @Result=(iif(coalesce(@ShowCode, 0)=coalesce(@ShowLabel, 0),
                                      v2.code + ' - '+ v2.label,
                                      iif(coalesce(@ShowCode, 0)=1,
                                          v2.code,
                                          v2.label)))

                    from "_lookup"."LookupValue" v0
                    join "_lookup"."Lookup" lookup on (v0.lookup_id=lookup._id and lookup.name=@Lookup)

                    join "_lookup"."LookupValueLink" lk0 on (v0._id=lk0.source_value_id and lk0.name=@Link1)
                    join "_lookup"."LookupValue" v1 on v1._id=lk0.target_value_id

                    join "_lookup"."LookupValueLink" lk1 on (v1._id=lk1.source_value_id and lk1.name=@Link2)
                    join "_lookup"."LookupValue" v2 on v2._id=lk1.target_value_id

                   where v0.code=@Code;
                  return @Result;
                end;""");

        c.createStatement().executeUpdate(
            """
                create or alter function _core.lookup_label3(@Code nvarchar(max),
                                                             @Lookup nvarchar(max),
                                                             @ShowCode bit,
                                                             @ShowLabel bit,
                                                             @Link1 nvarchar(max),
                                                             @Link2 nvarchar(max),
                                                             @Link3 nvarchar(max)) returns nvarchar(max) as
                begin
                  declare @LinkCursor Cursor;
                  declare @Result nvarchar(max);

                  select @Result=(iif(coalesce(@ShowCode, 0)=coalesce(@ShowLabel, 0),
                                      v3.code + ' - '+ v3.label,
                                      iif(coalesce(@ShowCode, 0)=1,
                                          v3.code,
                                          v3.label)))

                    from "_lookup"."LookupValue" v0
                    join "_lookup"."Lookup" lookup on (v0.lookup_id=lookup._id and lookup.name=@Lookup)

                    join "_lookup"."LookupValueLink" lk0 on (v0._id=lk0.source_value_id and lk0.name=@Link1)
                    join "_lookup"."LookupValue" v1 on v1._id=lk0.target_value_id

                    join "_lookup"."LookupValueLink" lk1 on (v1._id=lk1.source_value_id and lk1.name=@Link2)
                    join "_lookup"."LookupValue" v2 on v2._id=lk1.target_value_id

                    join "_lookup"."LookupValueLink" lk2 on (v2._id=lk2.source_value_id and lk2.name=@Link3)
                    join "_lookup"."LookupValue" v3 on v3._id=lk2.target_value_id

                   where v0.code=@Code;
                  return @Result;
                end;""");

        c.createStatement().executeUpdate(
            """
                create or alter function _core.lookup_label4(@Code nvarchar(max),
                                                             @Lookup nvarchar(max),
                                                             @ShowCode bit,
                                                             @ShowLabel bit,
                                                             @Link1 nvarchar(max),
                                                             @Link2 nvarchar(max),
                                                             @Link3 nvarchar(max),
                                                             @Link4 nvarchar(max)) returns nvarchar(max) as
                begin
                  declare @LinkCursor Cursor;
                  declare @Result nvarchar(max);

                  select @Result=(iif(coalesce(@ShowCode, 0)=coalesce(@ShowLabel, 0),
                                      v4.code + ' - '+ v4.label,
                                      iif(coalesce(@ShowCode, 0)=1,
                                          v4.code,
                                          v4.label)))

                    from "_lookup"."LookupValue" v0
                    join "_lookup"."Lookup" lookup on (v0.lookup_id=lookup._id and lookup.name=@Lookup)

                    join "_lookup"."LookupValueLink" lk0 on (v0._id=lk0.source_value_id and lk0.name=@Link1)
                    join "_lookup"."LookupValue" v1 on v1._id=lk0.target_value_id

                    join "_lookup"."LookupValueLink" lk1 on (v1._id=lk1.source_value_id and lk1.name=@Link2)
                    join "_lookup"."LookupValue" v2 on v2._id=lk1.target_value_id

                    join "_lookup"."LookupValueLink" lk2 on (v2._id=lk2.source_value_id and lk2.name=@Link3)
                    join "_lookup"."LookupValue" v3 on v3._id=lk2.target_value_id

                    join "_lookup"."LookupValueLink" lk3 on (v3._id=lk3.source_value_id and lk3.name=@Link4)
                    join "_lookup"."LookupValue" v4 on v4._id=lk3.target_value_id

                   where v0.code=@Code;
                  return @Result;
                end;""");

        c.createStatement().executeUpdate(
            """
                create or alter function _core.lookup_label5(@Code nvarchar(max),
                                                             @Lookup nvarchar(max),
                                                             @ShowCode bit,
                                                             @ShowLabel bit,
                                                             @Link1 nvarchar(max),
                                                             @Link2 nvarchar(max),
                                                             @Link3 nvarchar(max),
                                                             @Link4 nvarchar(max),
                                                             @Link5 nvarchar(max)) returns nvarchar(max) as
                begin
                  declare @LinkCursor Cursor;
                  declare @Result nvarchar(max);

                  select @Result=(iif(coalesce(@ShowCode, 0)=coalesce(@ShowLabel, 0),
                                      v5.code + ' - '+ v5.label,
                                      iif(coalesce(@ShowCode, 0)=1,
                                          v5.code,
                                          v5.label)))

                    from "_lookup"."LookupValue" v0
                    join "_lookup"."Lookup" lookup on (v0.lookup_id=lookup._id and lookup.name=@Lookup)

                    join "_lookup"."LookupValueLink" lk0 on (v0._id=lk0.source_value_id and lk0.name=@Link1)
                    join "_lookup"."LookupValue" v1 on v1._id=lk0.target_value_id

                    join "_lookup"."LookupValueLink" lk1 on (v1._id=lk1.source_value_id and lk1.name=@Link2)
                    join "_lookup"."LookupValue" v2 on v2._id=lk1.target_value_id

                    join "_lookup"."LookupValueLink" lk2 on (v2._id=lk2.source_value_id and lk2.name=@Link3)
                    join "_lookup"."LookupValue" v3 on v3._id=lk2.target_value_id

                    join "_lookup"."LookupValueLink" lk3 on (v3._id=lk3.source_value_id and lk3.name=@Link4)
                    join "_lookup"."LookupValue" v4 on v4._id=lk3.target_value_id

                    join "_lookup"."LookupValueLink" lk4 on (v4._id=lk4.source_value_id and lk4.name=@Link5)
                    join "_lookup"."LookupValue" v5 on v5._id=lk4.target_value_id

                   where v0.code=@Code;
                  return @Result;
                end;
                """);
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