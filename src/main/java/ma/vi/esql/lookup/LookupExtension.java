package ma.vi.esql.lookup;

import ma.vi.base.config.Configuration;
import ma.vi.base.lang.NotFoundException;
import ma.vi.esql.database.Database;
import ma.vi.esql.database.EsqlConnection;
import ma.vi.esql.database.Extension;
import ma.vi.esql.database.Structure;
import ma.vi.esql.exec.QueryParams;
import ma.vi.esql.exec.Result;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static java.lang.System.Logger.Level.INFO;
import static ma.vi.esql.translation.Translatable.Target.POSTGRESQL;
import static ma.vi.esql.translation.Translatable.Target.SQLSERVER;

/**
 * @author Vikash Madhow (vikash.madhow@gmail.com)
 */
public class LookupExtension implements Extension {
  @Override
  public void init(Database db, Configuration config) {
    this.db = db;
    log.log(INFO, "Creating lookup tables in " + db + " in schema _lookup");
    try (EsqlConnection c = db.esql()) {
      ///////////////////////////////////////////////////////////////////////////
      // Create lookup tables
      ///////////////////////////////////////////////////////////////////////////
      c.exec("""
             create table _lookup.Lookup drop undefined({
               name: 'Lookup',
               description: 'A named table of values'
             }

             _id         uuid not null,
             _version    long not null default 0,
             _can_delete bool not null default true,
             _can_edit   bool not null default true,

             name string not null {
               mask: 'Iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii'
             },
             display_name string not null,
             description  string,
             "group"      string,

             primary key(_id),
             unique(name))""");

      c.exec("""
             create table _lookup.LookupLink drop undefined({
               name: 'Lookup Link',
               description: 'The definition of links between values of lookup tables which are used for searching data by associations and for aggregating data in reports'
             }
    
             _id         uuid not null,
             _version    long not null default 0,
             _can_delete bool not null default true,
             _can_edit   bool not null default true,
    
             source_lookup_id uuid not null { show: false },
    
             target_lookup_id uuid not null {
               link_table:   '_lookup.Lookup',
               link_code:    '_id',
               link_label:   'name'
             },
    
             primary key(_id),
             foreign key(source_lookup_id) references _lookup.Lookup(_id),
             foreign key(target_lookup_id) references _lookup.Lookup(_id))""");

      c.exec("""
             create table _lookup.LookupValue drop undefined({
               name: 'Lookup Value',
               description: 'The values in a lookup table'
             }

             _id         uuid not null,
             _version    long not null default 0,
             _can_delete bool not null default true,
             _can_edit   bool not null default true,

             lookup_id   uuid not null { show: false },
             
             code        string not null,
             alt_code1   string,
             alt_code2   string,
             label       string not null,
             description text,
             
             lang string not null default 'en' {
               label: 'Language',
               initial_value: 'en'
             },

             primary key(_id),
             unique(lookup_id, code, lang),
             foreign key(lookup_id) references _lookup.Lookup(_id) on delete cascade on update cascade)""");

      c.exec("""
             create table _lookup.LookupValueLink drop undefined({
               name: 'Lookup Value Link',
               description: 'Links between values of lookup tables, used primarily for searching data by associations and for aggregating data in reports'
             }

             _id         uuid not null,
             _version    long not null default 0,
             _can_delete bool not null default true,
             _can_edit   bool not null default true,

             name            string not null,
             source_value_id uuid   not null,
             target_value_id uuid   not null,

             primary key(_id),
             foreign key(source_value_id) references _lookup.LookupValue(_id),
             foreign key(target_value_id) references _lookup.LookupValue(_id))""");

      /*
       * Indexes for optimizing primary search patterns.
       */
      c.exec("create index value_code   on _lookup.LookupValue(code)");
      c.exec("create index value_lookup on _lookup.LookupValue(lookup_id)");

      c.exec("create index link_name    on _lookup.LookupValueLink(name)");
      c.exec("create index link_source  on _lookup.LookupValueLink(source_value_id)");
      c.exec("create index link_target  on _lookup.LookupValueLink(target_value_id)");
    }

    /*
     * Lookup macros and labels functions.
     */
    Structure structure = db.structure();
    structure.function(new LookupLabelFunction());
    structure.function(new LookupLabel());
    structure.function(new JoinLabel());

    /*
     * Create lookup access functions specific to each database (only
     * required when using the function-based lookup label resolution).
     */
    if (db.target() == POSTGRESQL) {
      try (Connection c = db.pooledConnection()) {
        // lookup label with no links
        c.createStatement().executeUpdate("""
            create or replace function _lookup.lookup_label(code text,
                                                            lookup text,
                                                            show_code boolean,
                                                            show_label boolean) returns text as $$
                select case when coalesce(show_code, false)=coalesce(show_label, false)
                            then v.code || ' - ' || v.label
            
                            when coalesce(show_code, false)=true
                            then v.code
            
                            else v.label
                       end
                  from _lookup."LookupValue" v
                  join _lookup."Lookup"      l on v.lookup_id=l._id
                 where l.name=$2 and v.code=$1;
            $$ language sql immutable;""");

        // lookup label with variable number of links
        c.createStatement().executeUpdate("""
            create or replace function _lookup.lookup_label(code text,
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
                from_clause := '_lookup."LookupValue" v0 '
                            || 'join _lookup."Lookup" lookup '
                            || 'on (v0.lookup_id=lookup._id and lookup.name=''' || lookup || ''')';

                foreach link_name in array links loop
                    -- source side
                    from_clause := from_clause || ' join _lookup."LookupValueLink" lk' || link_index
                                               || ' on (v' || link_index  || '._id=lk'   || link_index
                                               || '.source_value_id and ' || 'lk'        || link_index
                                               || '.name=''' || link_name || ''')';

                    link_index := link_index + 1;

                    -- target side
                    from_clause := from_clause || ' join _lookup."LookupValue" v'   || link_index
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
            $$ language plpgsql immutable;""");

        c.commit();
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    } else if (db.target() == SQLSERVER) {
      try (Connection c = db.pooledConnection()) {
        // function to find value from lookups
        c.createStatement().executeUpdate("""
            create or alter function _lookup.lookup_label0(@Code      nvarchar(max),
                                                           @Lookup    nvarchar(max),
                                                           @ShowCode  bit,
                                                           @ShowLabel bit) returns nvarchar(max) as
            begin
              declare @Result nvarchar(max);
              select @Result=(iif(coalesce(@ShowCode, 0)=coalesce(@ShowLabel, 0),
                                  code + ' - '+ label,
                                  iif(coalesce(@ShowCode, 0)=1,
                                      code,
                                      label)))
                from _lookup."LookupValue" v
                join _lookup."Lookup"      l on v.lookup_id=l._id
              where l.name=@Lookup and v.code=@Code;
              return @Result;
            end;""");

        c.createStatement().executeUpdate("""
            create or alter function _lookup.lookup_label1(@Code      nvarchar(max),
                                                           @Lookup    nvarchar(max),
                                                           @ShowCode  bit,
                                                           @ShowLabel bit,
                                                           @Link1     nvarchar(max)) returns nvarchar(max) as
            begin
              declare @LinkCursor Cursor;
              declare @Result nvarchar(max);

              select @Result=(iif(coalesce(@ShowCode, 0)=coalesce(@ShowLabel, 0),
                                  v1.code + ' - '+ v1.label,
                                  iif(coalesce(@ShowCode, 0)=1,
                                      v1.code,
                                      v1.label)))

                from _lookup."LookupValue" v0
                join _lookup."Lookup" lookup on (v0.lookup_id=lookup._id and lookup.name=@Lookup)

                join _lookup."LookupValueLink" lk0 on (v0._id=lk0.source_value_id and lk0.name=@Link1)
                join _lookup."LookupValue" v1 on v1._id=lk0.target_value_id

               where v0.code=@Code;
              return @Result;
            end;""");

        c.createStatement().executeUpdate("""
            create or alter function _lookup.lookup_label2(@Code      nvarchar(max),
                                                           @Lookup    nvarchar(max),
                                                           @ShowCode  bit,
                                                           @ShowLabel bit,
                                                           @Link1     nvarchar(max),
                                                           @Link2     nvarchar(max)) returns nvarchar(max) as
            begin
              declare @LinkCursor Cursor;
              declare @Result nvarchar(max);

              select @Result=(iif(coalesce(@ShowCode, 0)=coalesce(@ShowLabel, 0),
                                  v2.code + ' - '+ v2.label,
                                  iif(coalesce(@ShowCode, 0)=1,
                                      v2.code,
                                      v2.label)))

                from _lookup."LookupValue" v0
                join _lookup."Lookup" lookup on (v0.lookup_id=lookup._id and lookup.name=@Lookup)

                join _lookup."LookupValueLink" lk0 on (v0._id=lk0.source_value_id and lk0.name=@Link1)
                join _lookup."LookupValue" v1 on v1._id=lk0.target_value_id

                join _lookup."LookupValueLink" lk1 on (v1._id=lk1.source_value_id and lk1.name=@Link2)
                join _lookup."LookupValue" v2 on v2._id=lk1.target_value_id

               where v0.code=@Code;
              return @Result;
            end;""");

        c.createStatement().executeUpdate("""
            create or alter function _lookup.lookup_label3(@Code      nvarchar(max),
                                                           @Lookup    nvarchar(max),
                                                           @ShowCode  bit,
                                                           @ShowLabel bit,
                                                           @Link1     nvarchar(max),
                                                           @Link2     nvarchar(max),
                                                           @Link3     nvarchar(max)) returns nvarchar(max) as
            begin
              declare @LinkCursor Cursor;
              declare @Result nvarchar(max);

              select @Result=(iif(coalesce(@ShowCode, 0)=coalesce(@ShowLabel, 0),
                                  v3.code + ' - '+ v3.label,
                                  iif(coalesce(@ShowCode, 0)=1,
                                      v3.code,
                                      v3.label)))

                from _lookup."LookupValue" v0
                join _lookup."Lookup" lookup on (v0.lookup_id=lookup._id and lookup.name=@Lookup)

                join _lookup."LookupValueLink" lk0 on (v0._id=lk0.source_value_id and lk0.name=@Link1)
                join _lookup."LookupValue" v1 on v1._id=lk0.target_value_id

                join _lookup."LookupValueLink" lk1 on (v1._id=lk1.source_value_id and lk1.name=@Link2)
                join _lookup."LookupValue" v2 on v2._id=lk1.target_value_id

                join _lookup."LookupValueLink" lk2 on (v2._id=lk2.source_value_id and lk2.name=@Link3)
                join _lookup."LookupValue" v3 on v3._id=lk2.target_value_id

               where v0.code=@Code;
              return @Result;
            end;""");

        c.createStatement().executeUpdate("""
            create or alter function _lookup.lookup_label4(@Code      nvarchar(max),
                                                           @Lookup    nvarchar(max),
                                                           @ShowCode  bit,
                                                           @ShowLabel bit,
                                                           @Link1     nvarchar(max),
                                                           @Link2     nvarchar(max),
                                                           @Link3     nvarchar(max),
                                                           @Link4     nvarchar(max)) returns nvarchar(max) as
            begin
              declare @LinkCursor Cursor;
              declare @Result nvarchar(max);

              select @Result=(iif(coalesce(@ShowCode, 0)=coalesce(@ShowLabel, 0),
                                  v4.code + ' - '+ v4.label,
                                  iif(coalesce(@ShowCode, 0)=1,
                                      v4.code,
                                      v4.label)))

                from _lookup."LookupValue" v0
                join _lookup."Lookup" lookup on (v0.lookup_id=lookup._id and lookup.name=@Lookup)

                join _lookup."LookupValueLink" lk0 on (v0._id=lk0.source_value_id and lk0.name=@Link1)
                join _lookup."LookupValue" v1 on v1._id=lk0.target_value_id

                join _lookup."LookupValueLink" lk1 on (v1._id=lk1.source_value_id and lk1.name=@Link2)
                join _lookup."LookupValue" v2 on v2._id=lk1.target_value_id

                join _lookup."LookupValueLink" lk2 on (v2._id=lk2.source_value_id and lk2.name=@Link3)
                join _lookup."LookupValue" v3 on v3._id=lk2.target_value_id

                join _lookup."LookupValueLink" lk3 on (v3._id=lk3.source_value_id and lk3.name=@Link4)
                join _lookup."LookupValue" v4 on v4._id=lk3.target_value_id

               where v0.code=@Code;
              return @Result;
            end;""");

        c.createStatement().executeUpdate("""
            create or alter function _lookup.lookup_label5(@Code      nvarchar(max),
                                                           @Lookup    nvarchar(max),
                                                           @ShowCode  bit,
                                                           @ShowLabel bit,
                                                           @Link1     nvarchar(max),
                                                           @Link2     nvarchar(max),
                                                           @Link3     nvarchar(max),
                                                           @Link4     nvarchar(max),
                                                           @Link5     nvarchar(max)) returns nvarchar(max) as
            begin
              declare @LinkCursor Cursor;
              declare @Result nvarchar(max);

              select @Result=(iif(coalesce(@ShowCode, 0)=coalesce(@ShowLabel, 0),
                                  v5.code + ' - '+ v5.label,
                                  iif(coalesce(@ShowCode, 0)=1,
                                      v5.code,
                                      v5.label)))

                from _lookup."LookupValue" v0
                join _lookup."Lookup" lookup on (v0.lookup_id=lookup._id and lookup.name=@Lookup)

                join _lookup."LookupValueLink" lk0 on (v0._id=lk0.source_value_id and lk0.name=@Link1)
                join _lookup."LookupValue" v1 on v1._id=lk0.target_value_id

                join _lookup."LookupValueLink" lk1 on (v1._id=lk1.source_value_id and lk1.name=@Link2)
                join _lookup."LookupValue" v2 on v2._id=lk1.target_value_id

                join _lookup."LookupValueLink" lk2 on (v2._id=lk2.source_value_id and lk2.name=@Link3)
                join _lookup."LookupValue" v3 on v3._id=lk2.target_value_id

                join _lookup."LookupValueLink" lk3 on (v3._id=lk3.source_value_id and lk3.name=@Link4)
                join _lookup."LookupValue" v4 on v4._id=lk3.target_value_id

                join _lookup."LookupValueLink" lk4 on (v4._id=lk4.source_value_id and lk4.name=@Link5)
                join _lookup."LookupValue" v5 on v5._id=lk4.target_value_id

               where v0.code=@Code;
              return @Result;
            end;""");

        c.commit();
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public List<Lookup> loadLookups() {
    return List.copyOf(lookups().values());
  }

  public Lookup loadLookup(String name) {
    return findLookup(name)
          .orElseThrow(() -> new NotFoundException("Lookup named '" + name + "' not found."));
  }

  public Optional<Lookup> findLookup(String name) {
    return Optional.ofNullable(lookups().get(name));
  }

  public LookupValue loadLookupValue(String lookup,
                                     String code) {
    return loadLookupValue(lookup, code, Lookup.MatchBy.code);
  }

  public LookupValue loadLookupValue(String lookup,
                                     String code,
                                     Lookup.MatchBy matchBy) {
    return findLookupValue(lookup, code, matchBy)
          .orElseThrow(() -> new NotFoundException(matchBy + "='" + code
                                                 + "' not found in lookup "
                                                 + lookup));
  }

  public Optional<LookupValue> findLookupValue(String lookup,
                                               String code) {
    return findLookupValue(lookup, code, Lookup.MatchBy.code);
  }

  public Optional<LookupValue> findLookupValue(String lookup,
                                               String code,
                                               Lookup.MatchBy matchBy) {
    return Optional.ofNullable(lookups().get(lookup))
                   .map(l -> l.mapBy(matchBy).get(code));
  }

  private Map<String, Lookup> lookups() {
    if (this.lookupsCache == null) {
      this.lookupsCache = new ConcurrentHashMap<>();
      try (EsqlConnection con = db.esql();
           Result rs = con.exec("""
                                select           source_id:source._id,
                                               source_name:source.name,
                                              source_group:source."group",
                                       source_display_name:source.display_name,
                                        source_description:source.description,
                                               target_name:target.name
                                       
                                  from source:_lookup.Lookup
                             left join   link:_lookup.LookupLink on link.source_lookup_id=source._id
                             left join target:_lookup.Lookup     on target._id=link.target_lookup_id""")) {
        record Link(String source,
                    String target) {}
        Map<String, Lookup> lookups = new HashMap<>();
        List<Link> links = new ArrayList<>();
        while (rs.toNext()) {
          String lookupName = rs.value("source_name");
          if (!lookups.containsKey(lookupName)) {
            lookups.put(lookupName, new Lookup(rs.value("source_id"),
                                               lookupName,
                                               rs.value("source_group"),
                                               rs.value("source_display_name"),
                                               rs.value("source_description"),
                                               new ArrayList<>(),
                                               new HashMap<>(),
                                               new HashMap<>()));
          }
          String targetName = rs.value("target_name");
          if (targetName != null) {
            links.add(new Link(lookupName, targetName));
          }
        }
        for (Link link: links) {
          Lookup source = lookups.get(link.source);
          Lookup target = lookups.get(link.target);
          source.links().add(target);
        }

        /*
         * Load values.
         */
        try (Result vrs = con.exec("""
                                   select          source_id:sv._id,
                                                 source_code:sv.code,
                                            source_alt_code1:sv.alt_code1,
                                            source_alt_code2:sv.alt_code2,
                                                source_label:sv.label,
                                          source_description:sv.description,
                                                 source_lang:sv.lang,
                                         
                                          source_lookup_name:sl.name,
                                                   link_name:lk.name,
                                          
                                          target_lookup_name:tl.name,
                                                   target_id:lk.target_value_id
                                          
                                     from sv:_lookup.LookupValue
                                     join sl:_lookup.Lookup          on sl._id=sv.lookup_id
                                left join lk:_lookup.LookupValueLink on lk.source_value_id=sv._id
                                left join tv:_lookup.LookupValue     on tv._id=lk.target_value_id
                                left join tl:_lookup.Lookup          on tl._id=tv.lookup_id""")) {
          record ValueLink(UUID   source,
                           String sourceLookup,
                           String linkName,
                           String targetLookup,
                           UUID   target) {}

          List<ValueLink> valueLinks = new ArrayList<>();
          while (vrs.toNext()) {
            UUID valueId = vrs.value("source_id");
            String sourceLookupName = vrs.value("source_lookup_name");
            Map<String, LookupValue> lookupValues = lookups.get(sourceLookupName).values();
            Map<UUID,   LookupValue> lookupValuesById = lookups.get(sourceLookupName).valuesById();
            String sourceCode = vrs.value("source_code");
            if (!lookupValues.containsKey(sourceCode)) {
              LookupValue value = new LookupValue(valueId,
                                                  sourceLookupName,
                                                  sourceCode,
                                                  vrs.value("source_alt_code1"),
                                                  vrs.value("source_alt_code2"),
                                                  vrs.value("source_label"),
                                                  vrs.value("source_description"),
                                                  vrs.value("source_lang"),
                                                  new ArrayList<>());
              lookupValues.put(sourceCode,  value);
              lookupValuesById.put(valueId, value);
            }
            String linkName = vrs.value("link_name");
            if (linkName != null) {
              valueLinks.add(new ValueLink(valueId,
                                           sourceLookupName,
                                           linkName,
                                           vrs.value("target_lookup_name"),
                                           vrs.value("target_id")));
            }
          }
          for (ValueLink link: valueLinks) {
            LookupValue source = lookups.get(link.sourceLookup).valuesById().get(link.source);
            LookupValue target = lookups.get(link.targetLookup).valuesById().get(link.target);
            source.links().add(new LookupValueLink(link.linkName, target));
          }
        }
        this.lookupsCache = lookups;
      }
    }
    return this.lookupsCache;
  }

  public UUID saveLookup(Lookup lookup) {
    UUID saved = saveLookup(lookup, new HashSet<>());
    lookupsCache = null;
    return saved;
  }

  private UUID saveLookup(Lookup lookup, Set<UUID> savedLookups) {
    if (!savedLookups.contains(lookup.id())) {
      savedLookups.add(lookup.id());
      try (EsqlConnection con = db.esql()) {
        Result rs = con.exec("""
                             select _id
                               from _lookup.Lookup
                              where name=@name""",
                             new QueryParams().add("name", lookup.name()));
        UUID lookupId;
        if (rs.toNext()) {
          lookupId = rs.value("_id");
          con.exec("""
                   update l
                     from l:_lookup.Lookup
                      set display_name=@displayName,
                          description=@description,
                          "group"=@grp
                    where _id=@id""",
                   new QueryParams()
                       .add("displayName", lookup.displayName())
                       .add("description", lookup.description())
                       .add("grp",         lookup.group())
                       .add("id",          lookupId));

          /*
           * Update links: set a special version on existing links, change that
           * version for updated links, then deleted those which were not updated
           * (meaning that they were not present in the definition).
           */
          con.exec("""
                   update ln
                     from ln:_lookup.LookupLink
                      set _version=0
                    where source_lookup_id=u'""" + lookupId + "'");

          if (lookup.links() != null) {
            for (Lookup link: lookup.links()) {
              saveLookupLink(lookupId, link);
            }
          }
          con.exec("""
                   delete ln
                     from ln:_lookup.LookupLink
                    where _version=0
                      and source_lookup_id=u'""" + lookupId + "'");

          /*
           * Update values using same strategy as links
           */
          con.exec("""
                   update lv
                     from lv:_lookup.LookupValue
                      set _version=0
                    where lookup_id=u'""" + lookupId + "'");

          if (lookup.values() != null) {
            for (LookupValue value: lookup.values().values()) {
              saveLookupValue(lookupId, value);
            }
          }
          con.exec("""
                   delete lv
                     from lv:_lookup.LookupValue
                    where _version=0
                      and lookup_id=u'""" + lookupId + "'");

        } else {
          lookupId = lookup.id();
          con.exec("""
                   insert into _lookup.Lookup(_id,  name, display_name,  description, "group")
                                       values(@id, @name, @displayName, @description, @grp)""",
                   new QueryParams()
                       .add("id",          lookupId)
                       .add("name",        lookup.name())
                       .add("displayName", lookup.displayName())
                       .add("description", lookup.description())
                       .add("grp",         lookup.group()));
          if (lookup.links() != null) {
            for (Lookup link: lookup.links()) {
              con.exec("""
                       insert into _lookup.LookupLink(_id,     source_lookup_id, target_lookup_id)
                                               values(newid(), @sourceId,        @targetId)""",
                       new QueryParams()
                           .add("sourceId",    lookupId)
                           .add("targetId",    link.id()));
            }
          }
          if (lookup.values() != null) {
            for (LookupValue value: lookup.values().values()) {
              saveLookupValue(lookupId, value);
            }
          }
        }
        return lookupId;
      }
    }
    return lookup.id();
  }

  public void saveLookupLink(UUID lookupId, Lookup link) {
    try (EsqlConnection con = db.esql();
         Result rs = con.exec("""
                              select ln._id
                                from ln:_lookup.LookupLink
                                join lk:_lookup.Lookup on lk._id=ln.source_lookup_id
                                                      and lk._id=@sourceId
                               where ln.target_lookup_id=@targetId
                              """,
                              new QueryParams()
                                 .add("sourceId", lookupId)
                                 .add("targetId", link.id()))) {
      if (rs.toNext()) {
        UUID linkId = rs.value(1);
        con.exec("""
                 update ln
                   from ln:_lookup.LookupLink
                    set _version=2,
                        target_lookup_id=@targetId
                  where ln._id=@linkId""",
                 new QueryParams()
                     .add("linkId",   linkId)
                     .add("targetId", link.id()));
//                     .add("targetId", loadLookup(link.name()).id()));
      } else {
        con.exec("""
                 insert into _lookup.LookupLink(_id,     _version, source_lookup_id, target_lookup_id)
                                         values(newid(), 1,        @sourceId,        @targetId)""",
                 new QueryParams()
                     .add("sourceId", lookupId)
                     .add("targetId", link.id()));
//                     .add("targetId", loadLookup(link.target().name()).id()));
      }
    }
  }

  public void saveLookupValue(UUID lookupId, LookupValue value) {
    try (EsqlConnection con = db.esql();
         Result rs = con.exec("""
                              select lv._id
                                from lv:_lookup.LookupValue
                               where lv.lookup_id=@lookupId
                                 and lv.code=@code
                              """,
                              new QueryParams()
                                 .add("lookupId", lookupId)
                                 .add("code",     value.code()))) {

      UUID valueId;
      if (rs.toNext()) {
        valueId = rs.value(1);
        con.exec("""
                 update lv
                   from lv:_lookup.LookupValue
                    set _version   =2,
                        alt_code1  =@altCode1,
                        alt_code2  =@altCode2,
                        label      =@label,
                        description=@description,
                        lang       =@lang
                  where _id=@valueId""",
                 new QueryParams()
                     .add("valueId",     valueId)
                     .add("altCode1",    value.altCode1())
                     .add("altCode2",    value.altCode2())
                     .add("label",       value.label())
                     .add("description", value.description())
                     .add("lang",        value.lang() == null ? "en" : value.lang()));
        /*
         * Delete existing value links; new links, if any, will be inserted below.
         */
        con.exec("""
                 delete ln
                   from ln:_lookup.LookupValueLink
                  where source_value_id=@valueId""",
                 new QueryParams().add( "valueId", valueId));
      } else {
        valueId = UUID.randomUUID();
        con.exec("""
                 insert into _lookup.LookupValue(_id,      _version, lookup_id,  code, alt_code1, alt_code2,  label,  description,  lang)
                                          values(@valueId, 1,        @lookupId, @code, @altCode1, @altCode2, @label, @description, @lang)""",
                 new QueryParams()
                     .add("valueId",     valueId)
                     .add("lookupId",    lookupId)
                     .add("code",        value.code())
                     .add("altCode1",    value.altCode1())
                     .add("altCode2",    value.altCode2())
                     .add("label",       value.label())
                     .add("description", value.description())
                     .add("lang",        value.lang() == null ? "en" : value.lang()));
      }
      /*
       * Insert value links
       */
      if (value.links() != null) {
        for (LookupValueLink link: value.links()) {
          con.exec("""
                     insert into _lookup.LookupValueLink(_id,     _version, name,  source_value_id, target_value_id)
                                                  values(newid(), 1,        @name, @sourceId,       @targetId)""",
                   new QueryParams()
                       .add("name",     link.name())
                       .add("sourceId", valueId)
                       .add("targetId", loadLookupValue(link.target().lookup(), link.target().code()).id()));
        }
      }
    }
  }

  private Map<String, Lookup> lookupsCache = null;

  private Database db;

  private static final System.Logger log = System.getLogger(LookupExtension.class.getName());
}