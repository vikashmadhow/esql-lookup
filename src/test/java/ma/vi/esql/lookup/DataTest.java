/*
 * Copyright (c) 2020 Vikash Madhow
 */

package ma.vi.esql.lookup;

import ma.vi.esql.database.Database;
import ma.vi.esql.exec.EsqlConnection;
import ma.vi.esql.exec.Param;
import ma.vi.esql.exec.Result;
import ma.vi.esql.parser.Parser;
import ma.vi.esql.parser.Program;
import org.junit.jupiter.api.BeforeAll;

import java.util.UUID;

import static java.lang.System.Logger.Level.INFO;
import static org.apache.commons.lang3.StringUtils.*;

public class DataTest {
  public static Database[] databases;

  @BeforeAll
  static void setup() {
    databases = new Database[] {
        Databases.Postgresql(),
        Databases.SqlServer(),
//        Databases.HSqlDb(),
//        Databases.MariaDb(),
    };

    for (Database db: databases) {
      System.out.println(db.target());
      Parser p = new Parser(db.structure());
      try (EsqlConnection con = db.esql(db.pooledConnection())) {
        try (Result rs = con.exec("select _id from _platform.lookup.Lookup l where name='EnsicSection'")) {
          if (!rs.next()) {
            log.log(INFO, "Creating EnsicSection lookup");

            UUID id = UUID.randomUUID();
            con.exec(p.parse("insert into _platform.lookup.Lookup(_id, name, description) " +
                                "values(:id, :name, :description)"),
                     Param.of("id", id.toString()),
                     Param.of("name", "EnsicSection"),
                     Param.of("description", "Ensic Section"));

            con.exec(p.parse("insert into _platform.lookup.LookupValue(_id, lookup_id, code, lang, label) values" +
                                "(newid(), '" + id + "', 'A', 'en', 'Agriculture forestry and fishing'), " +
                                "(newid(), '" + id + "', 'B', 'en', 'Mining and quarrying'), " +
                                "(newid(), '" + id + "', 'C', 'en', 'Manufacturing'), " +
                                "(newid(), '" + id + "', 'D', 'en', 'Electricity, gas, steam and air conditioning supply'), " +
                                "(newid(), '" + id + "', 'E', 'en', 'Water supply; sewerage, waste management and remediation activities'), " +
                                "(newid(), '" + id + "', 'F', 'en', 'Construction'), " +
                                "(newid(), '" + id + "', 'G', 'en', 'Wholesale and retail trade; repair of motor vehicles and motorcycles'), " +
                                "(newid(), '" + id + "', 'H', 'en', 'Transportation and storage'), " +
                                "(newid(), '" + id + "', 'I', 'en', 'Accommodation and food service activities'), " +
                                "(newid(), '" + id + "', 'J', 'en', 'Information and communication'), " +
                                "(newid(), '" + id + "', 'K', 'en', 'Financial and insurance activities'), " +
                                "(newid(), '" + id + "', 'L', 'en', 'Real estate activities'), " +
                                "(newid(), '" + id + "', 'M', 'en', 'Professional, scientific and technical activities'), " +
                                "(newid(), '" + id + "', 'N', 'en', 'Administrative and support service activities'), " +
                                "(newid(), '" + id + "', 'O', 'en', 'Public administration and defence; compulsory social security'), " +
                                "(newid(), '" + id + "', 'P', 'en', 'Education'), " +
                                "(newid(), '" + id + "', 'Q', 'en', 'Human health and social work activities'), " +
                                "(newid(), '" + id + "', 'R', 'en', 'Arts, entertainment and recreation'), " +
                                "(newid(), '" + id + "', 'S', 'en', 'Other service activities'), " +
                                "(newid(), '" + id + "', 'T', 'en', 'Activities of households as employers; undifferetiated goods- and services-producing activities of households for own use'), " +
                                "(newid(), '" + id + "', 'U', 'en', 'Activities of extraterritorial organisations and bodies'), " +
                                "(newid(), '" + id + "', 'V', 'en', 'Other activities not adequately defined')"));
          }
        }

        try (Result rs = con.exec(p.parse("select _id from _platform.lookup.Lookup where name='EnsicDivision'"))) {
          if (!rs.next()) {
            log.log(INFO, "Creating EnsicDivision lookup");

            UUID id = UUID.randomUUID();
            con.exec(p.parse("insert into _platform.lookup.Lookup(_id, name, description) " +
                                "values(:id, :name, :description)"),
                   Param.of("id", id.toString()),
                   Param.of("name", "EnsicDivision"),
                   Param.of("description", "Ensic Division"));

            con.exec(p.parse("insert into _platform.lookup.LookupLink(_id, name, display_name, source_lookup_id, target_lookup_id)" +
                                "values(newid(), 'EnsicSection', 'Ensic Section', '" + id + "', " +
                                "       (select _id from _platform.lookup.Lookup where name='EnsicSection'))"));

            con.exec(p.parse("insert into _platform.lookup.LookupValue(_id, lookup_id, code, lang, label) values" +
                                "(newid(), '" + id + "', '01', 'en', 'Crop and animal production, hunting and related service activities'), " +
                                "(newid(), '" + id + "', '02', 'en', 'Forestry and logging'), " +
                                "(newid(), '" + id + "', '03', 'en', 'Fishing and aquaculture'), " +
                                "(newid(), '" + id + "', '05', 'en', 'Mining of coal and lignite'), " +
                                "(newid(), '" + id + "', '06', 'en', 'Extraction of crude petroleum and natural gas'), " +
                                "(newid(), '" + id + "', '07', 'en', 'Mining of metal ores'), " +
                                "(newid(), '" + id + "', '08', 'en', 'Other mining and quarrying'), " +
                                "(newid(), '" + id + "', '09', 'en', 'Mining support service activities'), " +
                                "(newid(), '" + id + "', '10', 'en', 'Manufacture of food products'), " +
                                "(newid(), '" + id + "', '11', 'en', 'Manufacture of beverages'), " +
                                "(newid(), '" + id + "', '12', 'en', 'Manufacture of tobacco products'), " +
                                "(newid(), '" + id + "', '13', 'en', 'Manufacture of textiles'), " +
                                "(newid(), '" + id + "', '14', 'en', 'Manufacture of wearing apparel'), " +
                                "(newid(), '" + id + "', '15', 'en', 'Manufacture of leather and related products'), " +
                                "(newid(), '" + id + "', '16', 'en', 'Manufacture of wood and of products of wood & cork, except furniture; manufacture of articles of straw and plaiting materials'), " +
                                "(newid(), '" + id + "', '17', 'en', 'Manufacture of paper and paper products'), " +
                                "(newid(), '" + id + "', '18', 'en', 'Printing and reproduction of recorded media'), " +
                                "(newid(), '" + id + "', '19', 'en', 'Manufacture of coke and refined petroleum products'), " +
                                "(newid(), '" + id + "', '20', 'en', 'Manufacture of chemicals and chemical products'), " +
                                "(newid(), '" + id + "', '21', 'en', 'Manufacture of basic pharmaceutical products and pharmaceutical preparations'), " +
                                "(newid(), '" + id + "', '22', 'en', 'Manufacture of rubber and plastic products'), " +
                                "(newid(), '" + id + "', '23', 'en', 'Manufacture of other non-metallic mineral products'), " +
                                "(newid(), '" + id + "', '24', 'en', 'Manufacturing of basic metals'), " +
                                "(newid(), '" + id + "', '25', 'en', 'Manufacture of fabricated metal products, except machinery and equipment'), " +
                                "(newid(), '" + id + "', '26', 'en', 'Manufacture of computer, electronic and optical products'), " +
                                "(newid(), '" + id + "', '27', 'en', 'Manufacture of electrical equipment'), " +
                                "(newid(), '" + id + "', '28', 'en', 'Manufacture of machinery and equipment n.e.c.'), " +
                                "(newid(), '" + id + "', '29', 'en', 'Manufacture of motor vehicles, trailers and semi-trailers'), " +
                                "(newid(), '" + id + "', '30', 'en', 'Manufacture of other transport equipment'), " +
                                "(newid(), '" + id + "', '31', 'en', 'Manufacture of furniture'), " +
                                "(newid(), '" + id + "', '32', 'en', 'Other manufacturing'), " +
                                "(newid(), '" + id + "', '33', 'en', 'Repair and installation of machinery and equipment'), " +
                                "(newid(), '" + id + "', '35', 'en', 'Electricity, gas, steam and air conditioning supply'), " +
                                "(newid(), '" + id + "', '36', 'en', 'Water collection, treatment and supply'), " +
                                "(newid(), '" + id + "', '37', 'en', 'Sewerage'), " +
                                "(newid(), '" + id + "', '38', 'en', 'Waste collection, treatment and disposal activities; materials recovery'), " +
                                "(newid(), '" + id + "', '39', 'en', 'Remediation activities and other waste management services'), " +
                                "(newid(), '" + id + "', '41', 'en', 'Construction of buildings'), " +
                                "(newid(), '" + id + "', '42', 'en', 'Civil engineering'), " +
                                "(newid(), '" + id + "', '43', 'en', 'Specialised construction activities'), " +
                                "(newid(), '" + id + "', '45', 'en', 'Wholesale and retail trade and repair of motor vehicles and motorcycles'), " +
                                "(newid(), '" + id + "', '46', 'en', 'Wholesale trade, except for motor vehicles and motorcycles'), " +
                                "(newid(), '" + id + "', '47', 'en', 'Retail trade, except for motor vehicles and motorcycles'), " +
                                "(newid(), '" + id + "', '49', 'en', 'Land transport and transport via pipelines'), " +
                                "(newid(), '" + id + "', '50', 'en', 'Water transport'), " +
                                "(newid(), '" + id + "', '51', 'en', 'Air transport'), " +
                                "(newid(), '" + id + "', '52', 'en', 'Warehousing and support activities for transportation'), " +
                                "(newid(), '" + id + "', '53', 'en', 'Postal and courier activities'), " +
                                "(newid(), '" + id + "', '55', 'en', 'Accommodation'), " +
                                "(newid(), '" + id + "', '56', 'en', 'Food and beverage service activities'), " +
                                "(newid(), '" + id + "', '58', 'en', 'Publishing activities'), " +
                                "(newid(), '" + id + "', '59', 'en', 'Motion picture, video and television programme production, sound recording and music publishing activities'), " +
                                "(newid(), '" + id + "', '60', 'en', 'Programming and broadcasting activities'), " +
                                "(newid(), '" + id + "', '61', 'en', 'Telecommunications'), " +
                                "(newid(), '" + id + "', '62', 'en', 'Computer programming, consultancy and related activities'), " +
                                "(newid(), '" + id + "', '63', 'en', 'Information service activities'), " +
                                "(newid(), '" + id + "', '64', 'en', 'Financial service activities, except insurance and pension funding'), " +
                                "(newid(), '" + id + "', '65', 'en', 'Insurance, reinsurance and pension funding, except compulsory social security'), " +
                                "(newid(), '" + id + "', '66', 'en', 'Activities auxiliary to financial service and insurance activities'), " +
                                "(newid(), '" + id + "', '68', 'en', 'Real estate activities'), " +
                                "(newid(), '" + id + "', '69', 'en', 'Legal and accounting activities'), " +
                                "(newid(), '" + id + "', '70', 'en', 'Activities of head offices; management consultancy activities'), " +
                                "(newid(), '" + id + "', '71', 'en', 'Architectural and engineering activities; technical testing and analysis'), " +
                                "(newid(), '" + id + "', '72', 'en', 'Scientific research and development'), " +
                                "(newid(), '" + id + "', '73', 'en', 'Advertising and market research'), " +
                                "(newid(), '" + id + "', '74', 'en', 'Other professional, scientific and technical activities'), " +
                                "(newid(), '" + id + "', '75', 'en', 'Veterinary activities'), " +
                                "(newid(), '" + id + "', '77', 'en', 'Rental and leasing activities'), " +
                                "(newid(), '" + id + "', '78', 'en', 'Employment activities'), " +
                                "(newid(), '" + id + "', '79', 'en', 'Travel agency, tour operator, reservation service and related activities'), " +
                                "(newid(), '" + id + "', '80', 'en', 'Security and investigation activities'), " +
                                "(newid(), '" + id + "', '81', 'en', 'Services to buildings and landscape activities'), " +
                                "(newid(), '" + id + "', '82', 'en', 'Office administrative, office support and other business support activities'), " +
                                "(newid(), '" + id + "', '84', 'en', 'Public administration and defence; compulsory social security'), " +
                                "(newid(), '" + id + "', '85', 'en', 'Education'), " +
                                "(newid(), '" + id + "', '86', 'en', 'Human health activities'), " +
                                "(newid(), '" + id + "', '87', 'en', 'Residential care activities'), " +
                                "(newid(), '" + id + "', '88', 'en', 'Social work activities without accommodation'), " +
                                "(newid(), '" + id + "', '90', 'en', 'Creative, arts and entertainment activities'), " +
                                "(newid(), '" + id + "', '91', 'en', 'Libraries, archives, museums and other cultural activities'), " +
                                "(newid(), '" + id + "', '92', 'en', 'Gambling and betting activities'), " +
                                "(newid(), '" + id + "', '93', 'en', 'Sports activities and amusement and recreation activities'), " +
                                "(newid(), '" + id + "', '94', 'en', 'Activities of membership organisations'), " +
                                "(newid(), '" + id + "', '95', 'en', 'Repair of computers and personal household goods'), " +
                                "(newid(), '" + id + "', '96', 'en', 'Other personal service activities'), " +
                                "(newid(), '" + id + "', '97', 'en', 'Activities of households as employers of domestic personnel'), " +
                                "(newid(), '" + id + "', '98', 'en', 'Undifferentiated goods- and services-producing activities of private households for own use'), " +
                                "(newid(), '" + id + "', '99', 'en', 'Activities of extraterritorial organisations and bodies'), " +
                                "(newid(), '" + id + "', '9V', 'en', 'Other activities not adequately defined')"));

            String sectionId;
            try (Result result = con.exec(p.parse("select _id from _platform.lookup.Lookup where name='EnsicSection'"))) {
              result.next();
              sectionId = result.get(1).value.toString();
            }

            log.log(INFO, "Linking EnsicDivision to EnsicSection");

            con.exec(p.parse("insert into _platform.lookup.LookupValueLink(_id, name, source_value_id, target_value_id) values" +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='01'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='A')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='02'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='A')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='03'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='A')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='05'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='B')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='06'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='B')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='07'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='B')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='08'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='B')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='09'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='B')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='10'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='C')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='11'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='C')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='12'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='C')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='13'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='C')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='14'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='C')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='15'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='C')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='16'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='C')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='17'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='C')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='18'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='C')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='19'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='C')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='20'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='C')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='21'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='C')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='22'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='C')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='23'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='C')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='24'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='C')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='25'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='C')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='26'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='C')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='27'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='C')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='28'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='C')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='29'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='C')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='30'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='C')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='31'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='C')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='32'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='C')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='33'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='C')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='35'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='D')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='36'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='E')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='37'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='E')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='38'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='E')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='39'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='E')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='41'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='F')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='42'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='F')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='43'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='F')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='45'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='G')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='46'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='G')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='47'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='G')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='49'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='H')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='50'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='H')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='51'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='H')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='52'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='H')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='53'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='H')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='55'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='I')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='56'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='I')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='58'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='J')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='59'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='J')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='60'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='J')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='61'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='J')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='62'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='J')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='63'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='J')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='64'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='K')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='65'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='K')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='66'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='K')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='68'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='L')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='69'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='M')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='70'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='M')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='71'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='M')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='72'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='M')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='73'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='M')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='74'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='M')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='75'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='M')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='77'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='N')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='78'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='N')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='79'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='N')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='80'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='N')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='81'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='N')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='82'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='N')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='84'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='O')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='85'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='P')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='86'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='Q')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='87'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='Q')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='88'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='Q')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='90'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='R')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='91'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='R')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='92'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='R')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='93'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='R')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='94'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='S')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='95'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='S')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='96'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='S')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='97'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='T')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='98'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='T')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='99'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='U')), " +
                                "(newid(), 'EnsicSection', (select _id from _platform.lookup.LookupValue where lookup_id='" + id + "' and code='9V'), (select _id from _platform.lookup.LookupValue where lookup_id='" + sectionId + "' and code='V'))"));
          }
        }

        try (Result rs = con.exec(p.parse("select _id from _platform.lookup.Lookup where name='EnsicGroup'"))) {
          if (!rs.next()) {
            log.log(INFO, "Creating EnsicGroup lookup");

            UUID id = UUID.randomUUID();
            con.exec(p.parse("insert into _platform.lookup.Lookup(_id, name, description) " +
                                "values(:id, :name, :description)"),
                   Param.of("id", id.toString()),
                   Param.of("name", "EnsicGroup"),
                   Param.of("description", "Ensic group"));

            con.exec(p.parse("insert into _platform.lookup.LookupLink(_id, name, display_name, source_lookup_id, target_lookup_id)" +
                                "values(newid(), 'EnsicDivision', 'Ensic division', '" + id + "', " +
                                "       (select _id from _platform.lookup.Lookup where name='EnsicDivision'))"));

            con.exec(p.parse("insert into _platform.lookup.LookupValue(_id, lookup_id, code, lang, label) values" +
                                "(newid(), '" + id + "', '011', 'en', 'Growing of non-perennial crops'), " +
                                "(newid(), '" + id + "', '012', 'en', 'Growing of perennial crops'), " +
                                "(newid(), '" + id + "', '013', 'en', 'Plant propagation'), " +
                                "(newid(), '" + id + "', '014', 'en', 'Animal Production'), " +
                                "(newid(), '" + id + "', '015', 'en', 'Mixed farming'), " +
                                "(newid(), '" + id + "', '016', 'en', 'Support activities to agriculture'), " +
                                "(newid(), '" + id + "', '017', 'en', 'Hunting, trapping and related service activities'), " +
                                "(newid(), '" + id + "', '018', 'en', 'Production of Organic Fertiliser'), " +
                                "(newid(), '" + id + "', '021', 'en', 'Silviculture and other forestry activities'), " +
                                "(newid(), '" + id + "', '022', 'en', 'Logging'), " +
                                "(newid(), '" + id + "', '023', 'en', 'Gathering of non-wood forest products'), " +
                                "(newid(), '" + id + "', '024', 'en', 'Support services and forestry'), " +
                                "(newid(), '" + id + "', '025', 'en', 'Other activities of forestry and hunting n.e.c'), " +
                                "(newid(), '" + id + "', '031', 'en', 'Fishing'), " +
                                "(newid(), '" + id + "', '032', 'en', 'Aquaculture'), " +
                                "(newid(), '" + id + "', '033', 'en', 'Other activities of fish farm and related services'), " +
                                "(newid(), '" + id + "', '051', 'en', 'Mining of hard coal'), " +
                                "(newid(), '" + id + "', '052', 'en', 'Mining of lignite'), " +
                                "(newid(), '" + id + "', '061', 'en', 'Extraction of crude petroleum'), " +
                                "(newid(), '" + id + "', '062', 'en', 'Extraction of natural gas'), " +
                                "(newid(), '" + id + "', '071', 'en', 'Mining of metal ores'), " +
                                "(newid(), '" + id + "', '072', 'en', 'Mining of non-ferrous metal ores'), " +
                                "(newid(), '" + id + "', '081', 'en', 'Quarrying of stone, sand and clay'), " +
                                "(newid(), '" + id + "', '089', 'en', 'Mining and quarrying not elsewhere classified'), " +
                                "(newid(), '" + id + "', '091', 'en', 'Support activities for petroleum and natural gas extraction e.g. exploration services, test drilling'), " +
                                "(newid(), '" + id + "', '099', 'en', 'Support activities for other mining and quarrying'), " +
                                "(newid(), '" + id + "', '101', 'en', 'Processing and preserving of meat'), " +
                                "(newid(), '" + id + "', '102', 'en', 'Processing and preserving of fish, crustaceans & molluscs'), " +
                                "(newid(), '" + id + "', '103', 'en', 'Processing and preserving of fruits and vegetables'), " +
                                "(newid(), '" + id + "', '104', 'en', 'Manufacture of vegetable and animal oils and fats'), " +
                                "(newid(), '" + id + "', '105', 'en', 'Manufacture of dairy products'), " +
                                "(newid(), '" + id + "', '106', 'en', 'Manufacture of grain mill products, starches and starch products, and prepared animal feeds'), " +
                                "(newid(), '" + id + "', '107', 'en', 'Manufacture of other food products'), " +
                                "(newid(), '" + id + "', '108', 'en', 'Manufacture of animal feed'), " +
                                "(newid(), '" + id + "', '110', 'en', 'Manufacture of beverages'), " +
                                "(newid(), '" + id + "', '120', 'en', 'Manufacture of tobacco products'), " +
                                "(newid(), '" + id + "', '131', 'en', 'Spinning, weaving and finishing of textiles'), " +
                                "(newid(), '" + id + "', '139', 'en', 'Manufacture of other textiles'), " +
                                "(newid(), '" + id + "', '141', 'en', 'Manufacture of wearing apparel, except fur apparel'), " +
                                "(newid(), '" + id + "', '142', 'en', 'Manufacture of articles of fur including wearing apparel'), " +
                                "(newid(), '" + id + "', '143', 'en', 'Manufacture of knitted and crocheted apparel e.g. pullovers, socks, cardigans'), " +
                                "(newid(), '" + id + "', '151', 'en', 'Tanning and dressing of leather; manufacture of luggage, handbags, saddlery and harness; dressing and dyeing of fur'), " +
                                "(newid(), '" + id + "', '152', 'en', 'Manufacture of footwear'), " +
                                "(newid(), '" + id + "', '161', 'en', 'Sawmilling and planing of wood'), " +
                                "(newid(), '" + id + "', '162', 'en', 'Manufacture of products of wood, cork, straw and plaiting materials'), " +
                                "(newid(), '" + id + "', '170', 'en', 'Manufacture of paper and paper products'), " +
                                "(newid(), '" + id + "', '181', 'en', 'Printing and service activities related to printing'), " +
                                "(newid(), '" + id + "', '182', 'en', 'Reproduction of recorded media'), " +
                                "(newid(), '" + id + "', '191', 'en', 'Manufacture of coke oven products'), " +
                                "(newid(), '" + id + "', '192', 'en', 'Manufacture of refined petroleum products'), " +
                                "(newid(), '" + id + "', '201', 'en', 'Manufacture of basic chemicals, fertilizers and nitrogen compounds, plastics and synthetic rubber in primary forms'), " +
                                "(newid(), '" + id + "', '202', 'en', 'Manufacture of other chemical products'), " +
                                "(newid(), '" + id + "', '203', 'en', 'Manufacture of man-made fibres'), " +
                                "(newid(), '" + id + "', '210', 'en', 'Manufacture of pharmaceuticals, medicinal chemicals and botanical products'), " +
                                "(newid(), '" + id + "', '221', 'en', 'Manufacture of rubber products'), " +
                                "(newid(), '" + id + "', '222', 'en', 'Manufacture of plastic products'), " +
                                "(newid(), '" + id + "', '231', 'en', 'Manufacture of glass and glass products'), " +
                                "(newid(), '" + id + "', '239', 'en', 'Manufacture of non-metallic mineral products n.e.c.'), " +
                                "(newid(), '" + id + "', '241', 'en', 'Manufacture of basic iron and steel'), " +
                                "(newid(), '" + id + "', '242', 'en', 'Manufacture of basic precious and non-ferrous metals'), " +
                                "(newid(), '" + id + "', '243', 'en', 'Casting of metals'), " +
                                "(newid(), '" + id + "', '251', 'en', 'Manufacture of structural metal products, tanks, reservoirs and steam generators'), " +
                                "(newid(), '" + id + "', '252', 'en', 'Manufacture of weapons and ammunitions'), " +
                                "(newid(), '" + id + "', '259', 'en', 'Manufacture of other fabricated metal products; metal working service activities'), " +
                                "(newid(), '" + id + "', '261', 'en', 'Manufacture of electronic components and boards'), " +
                                "(newid(), '" + id + "', '262', 'en', 'Manufacture of computers and peripheral equipment'), " +
                                "(newid(), '" + id + "', '263', 'en', 'Manufacture of communication equipment'), " +
                                "(newid(), '" + id + "', '264', 'en', 'Manufacture of consumer electronics'), " +
                                "(newid(), '" + id + "', '265', 'en', 'Manufacture of measuring, testing, navigating and control equipment; watches & clocks'), " +
                                "(newid(), '" + id + "', '266', 'en', 'Manufacture of irradiation, electromedical and electrotherapeutic equipment (e.g. scanners, hearing aids)'), " +
                                "(newid(), '" + id + "', '267', 'en', 'Manufacture of optical instruments and photographic equipment'), " +
                                "(newid(), '" + id + "', '268', 'en', 'Manufacture of magnetic and optical media'), " +
                                "(newid(), '" + id + "', '269', 'en', 'Manufacture of medical appliances and instruments and appliances for measuring, checking, testing, navigating and for other purposes, except optical instruments'), " +
                                "(newid(), '" + id + "', '271', 'en', 'Manufacture of electric motors, generators, transformers and electricity distribution and control apparatus'), " +
                                "(newid(), '" + id + "', '272', 'en', 'Manufacture of batteries and accumulators'), " +
                                "(newid(), '" + id + "', '273', 'en', 'Manufacture of wiring and wiring devices'), " +
                                "(newid(), '" + id + "', '274', 'en', 'Manufacture of electrical lighting equipment'), " +
                                "(newid(), '" + id + "', '275', 'en', 'Manufacture of domestic appliances'), " +
                                "(newid(), '" + id + "', '276', 'en', 'Manufacture of energy-saving technology devices'), " +
                                "(newid(), '" + id + "', '279', 'en', 'Manufacture of other electrical equipment'), " +
                                "(newid(), '" + id + "', '281', 'en', 'Manufacture of general-purpose machinery'), " +
                                "(newid(), '" + id + "', '282', 'en', 'Manufacture of special-purpose machinery'), " +
                                "(newid(), '" + id + "', '291', 'en', 'Manufacture of motor vehicles'), " +
                                "(newid(), '" + id + "', '292', 'en', 'Manufacture of bodies (coachwork) for motor vehicles; manufacture of trailers and semi-trailers'), " +
                                "(newid(), '" + id + "', '293', 'en', 'Manufacture of parts and accessories for motor vehicles and their engines'), " +
                                "(newid(), '" + id + "', '301', 'en', 'Building of ships and boats'), " +
                                "(newid(), '" + id + "', '302', 'en', 'Manufacture of railway, locomotives and rolling stock'), " +
                                "(newid(), '" + id + "', '303', 'en', 'Manufacture of air and spacecraft and related machinery'), " +
                                "(newid(), '" + id + "', '304', 'en', 'Manufacture of military fighting vehicles'), " +
                                "(newid(), '" + id + "', '309', 'en', 'Manufacture of transport equipment n.e.c.'), " +
                                "(newid(), '" + id + "', '310', 'en', 'Manufacture of furniture'), " +
                                "(newid(), '" + id + "', '321', 'en', 'Manufacture of jewellery, bijouterie and related articles'), " +
                                "(newid(), '" + id + "', '322', 'en', 'Manufacture of musical instruments'), " +
                                "(newid(), '" + id + "', '323', 'en', 'Manufacture of sports goods'), " +
                                "(newid(), '" + id + "', '324', 'en', 'Manufacture of games and toys'), " +
                                "(newid(), '" + id + "', '325', 'en', 'Manufacture of medical and dental instruments and supplies'), " +
                                "(newid(), '" + id + "', '329', 'en', 'Other manufacturing n.e.c'), " +
                                "(newid(), '" + id + "', '331', 'en', 'Repair of fabricated metal products, machinery and equipment'), " +
                                "(newid(), '" + id + "', '332', 'en', 'Installation of industrial machinery and equipment'), " +
                                "(newid(), '" + id + "', '333', 'en', 'Equipment and machinery installation and maintenance service'), " +
                                "(newid(), '" + id + "', '351', 'en', 'Electric power generation, transmission and distribution'), " +
                                "(newid(), '" + id + "', '352', 'en', 'Manufacture of gas; distribution of gaseous fuels through mains'), " +
                                "(newid(), '" + id + "', '353', 'en', 'Steam and air conditioning supply'), " +
                                "(newid(), '" + id + "', '360', 'en', 'Water collection, treatment and supply'), " +
                                "(newid(), '" + id + "', '370', 'en', 'Sewerage'), " +
                                "(newid(), '" + id + "', '381', 'en', 'Waste collection'), " +
                                "(newid(), '" + id + "', '382', 'en', 'Waste treatment and disposal'), " +
                                "(newid(), '" + id + "', '383', 'en', 'Materials recovery'), " +
                                "(newid(), '" + id + "', '390', 'en', 'Remediation activities and other waste management services'), " +
                                "(newid(), '" + id + "', '410', 'en', 'Construction of buildings'), " +
                                "(newid(), '" + id + "', '421', 'en', 'Construction of roads and railways'), " +
                                "(newid(), '" + id + "', '422', 'en', 'Construction of utility projects e.g. pipelines, communication and power lines'), " +
                                "(newid(), '" + id + "', '429', 'en', 'Construction of other civil engineering projects'), " +
                                "(newid(), '" + id + "', '431', 'en', 'Demolition and site preparation'), " +
                                "(newid(), '" + id + "', '432', 'en', 'Electrical, plumbing and other construction installation activities'), " +
                                "(newid(), '" + id + "', '433', 'en', 'Building completion and finishing'), " +
                                "(newid(), '" + id + "', '439', 'en', 'Other specialised construction activities'), " +
                                "(newid(), '" + id + "', '451', 'en', 'Sale of motor vehicles'), " +
                                "(newid(), '" + id + "', '452', 'en', 'Maintenance and repair of motor vehicles'), " +
                                "(newid(), '" + id + "', '453', 'en', 'Sale of motor vehicles parts and accessories'), " +
                                "(newid(), '" + id + "', '454', 'en', 'Sale, maintenance and repair of motorcycles and related parts and accessories'), " +
                                "(newid(), '" + id + "', '461', 'en', 'Wholesale on a fee or contract basis'), " +
                                "(newid(), '" + id + "', '462', 'en', 'Wholesale of agricultural raw materials and live animals'), " +
                                "(newid(), '" + id + "', '463', 'en', 'Wholesale of food, beverages and tobacco'), " +
                                "(newid(), '" + id + "', '464', 'en', 'Wholesale of household goods'), " +
                                "(newid(), '" + id + "', '465', 'en', 'Wholesale of machine equipment and supplies'), " +
                                "(newid(), '" + id + "', '466', 'en', 'Other specialised wholesale'), " +
                                "(newid(), '" + id + "', '467', 'en', 'Wholesale trade in other agricultural products'), " +
                                "(newid(), '" + id + "', '469', 'en', 'Non-specialised wholesale trade'), " +
                                "(newid(), '" + id + "', '470', 'en', 'Retail trade of agricultural, industrial, construction and other equipment'), " +
                                "(newid(), '" + id + "', '471', 'en', 'Retail sale in non-specialised stores'), " +
                                "(newid(), '" + id + "', '472', 'en', 'Retail sale of food, beverages and tobacco in specialised stores'), " +
                                "(newid(), '" + id + "', '473', 'en', 'Retail sale of automotive fuel'), " +
                                "(newid(), '" + id + "', '474', 'en', 'Retail sale of information and communications equipment in specialised stores'), " +
                                "(newid(), '" + id + "', '475', 'en', 'Retail sale of other household equipment in specialised stores'), " +
                                "(newid(), '" + id + "', '476', 'en', 'Retail sale of cultural and recreation goods in specialised stores'), " +
                                "(newid(), '" + id + "', '477', 'en', 'Retail sale of other goods in specialised stores'), " +
                                "(newid(), '" + id + "', '478', 'en', 'Retail sale via stalls and markets'), " +
                                "(newid(), '" + id + "', '479', 'en', 'Retail sale not in stores, stalls and markets'), " +
                                "(newid(), '" + id + "', '491', 'en', 'Transport via railways'), " +
                                "(newid(), '" + id + "', '492', 'en', 'Other land transport'), " +
                                "(newid(), '" + id + "', '493', 'en', 'Transport via pipelines'), " +
                                "(newid(), '" + id + "', '501', 'en', 'Sea and coastal water transport'), " +
                                "(newid(), '" + id + "', '502', 'en', 'Inland water transport'), " +
                                "(newid(), '" + id + "', '511', 'en', 'Passenger air transport'), " +
                                "(newid(), '" + id + "', '512', 'en', 'Freight air transport'), " +
                                "(newid(), '" + id + "', '513', 'en', 'Aviation Support Activities'), " +
                                "(newid(), '" + id + "', '519', 'en', 'Other Activities of Air Transport n.e.c'), " +
                                "(newid(), '" + id + "', '521', 'en', 'Warehousing and storage'), " +
                                "(newid(), '" + id + "', '522', 'en', 'Support activities for transportation'), " +
                                "(newid(), '" + id + "', '531', 'en', 'Postal activities operating under universal service obligation'), " +
                                "(newid(), '" + id + "', '532', 'en', 'Courier activities not operating under universal service obligation'), " +
                                "(newid(), '" + id + "', '533', 'en', 'Other postal and related courier activities'), " +
                                "(newid(), '" + id + "', '551', 'en', 'Short-term accommodation activities'), " +
                                "(newid(), '" + id + "', '552', 'en', 'Camping grounds, recreational vehicle parks and trailer parks'), " +
                                "(newid(), '" + id + "', '553', 'en', 'Hotel management company'), " +
                                "(newid(), '" + id + "', '559', 'en', 'Accommodation n.e.c e.g. workers hostels, boarding houses'), " +
                                "(newid(), '" + id + "', '561', 'en', 'Restaurants and mobile food service activities'), " +
                                "(newid(), '" + id + "', '562', 'en', 'Event catering and other food service activities'), " +
                                "(newid(), '" + id + "', '563', 'en', 'Beverage serving facilities'), " +
                                "(newid(), '" + id + "', '581', 'en', 'Publishing of books, periodicals and other publishing activities'), " +
                                "(newid(), '" + id + "', '582', 'en', 'Software publishing'), " +
                                "(newid(), '" + id + "', '591', 'en', 'Motion picture, video and television programme production activities'), " +
                                "(newid(), '" + id + "', '592', 'en', 'Sound recording and music publishing activities'), " +
                                "(newid(), '" + id + "', '601', 'en', 'Radio broadcasting'), " +
                                "(newid(), '" + id + "', '602', 'en', 'Television programming and broadcasting activities'), " +
                                "(newid(), '" + id + "', '611', 'en', 'Wired telecommunications activities'), " +
                                "(newid(), '" + id + "', '612', 'en', 'Wireless telecommunications activities'), " +
                                "(newid(), '" + id + "', '613', 'en', 'Satellite telecommunications activities'), " +
                                "(newid(), '" + id + "', '619', 'en', 'Other telecommunications activities'), " +
                                "(newid(), '" + id + "', '620', 'en', 'Computer programming, consultancy and related activities'), " +
                                "(newid(), '" + id + "', '631', 'en', 'Data processing, hosting and related activities; web postal'), " +
                                "(newid(), '" + id + "', '639', 'en', 'Other information service activities'), " +
                                "(newid(), '" + id + "', '641', 'en', 'Monetary intermediation'), " +
                                "(newid(), '" + id + "', '642', 'en', 'Activities of holding companies, without managing'), " +
                                "(newid(), '" + id + "', '643', 'en', 'Trusts, funds and similar financial entities, without managing'), " +
                                "(newid(), '" + id + "', '649', 'en', 'Other financial services activities, except insurance and pension funding activities'), " +
                                "(newid(), '" + id + "', '651', 'en', 'Life insurance'), " +
                                "(newid(), '" + id + "', '652', 'en', 'Reinsurance'), " +
                                "(newid(), '" + id + "', '653', 'en', 'Pension funding'), " +
                                "(newid(), '" + id + "', '654', 'en', 'Medical aid funding Institutions'), " +
                                "(newid(), '" + id + "', '655', 'en', 'Plant and machinery valuation'), " +
                                "(newid(), '" + id + "', '659', 'en', 'Other insurance n.e.c.'), " +
                                "(newid(), '" + id + "', '661', 'en', 'Activities auxiliary to financial service activities except insurance and pension funding'), " +
                                "(newid(), '" + id + "', '662', 'en', 'Activities auxiliary to insurance and pension funding'), " +
                                "(newid(), '" + id + "', '663', 'en', 'Fund management activities'), " +
                                "(newid(), '" + id + "', '681', 'en', 'Real estate activities with own or leased property'), " +
                                "(newid(), '" + id + "', '682', 'en', 'Real estate activities on a fee or contract basis'), " +
                                "(newid(), '" + id + "', '691', 'en', 'Legal activities'), " +
                                "(newid(), '" + id + "', '692', 'en', 'Accounting, book-keeping and auditing activities; tax consultancy'), " +
                                "(newid(), '" + id + "', '701', 'en', 'Activities of head offices'), " +
                                "(newid(), '" + id + "', '702', 'en', 'Management consultancy activities'), " +
                                "(newid(), '" + id + "', '711', 'en', 'Architectural and engineering activities; and related technical consultancy'), " +
                                "(newid(), '" + id + "', '712', 'en', 'Technical testing and analysis'), " +
                                "(newid(), '" + id + "', '721', 'en', 'Research and experimental development on natural sciences and engineering'), " +
                                "(newid(), '" + id + "', '722', 'en', 'Research and experimental development on social sciences and humanities'), " +
                                "(newid(), '" + id + "', '723', 'en', 'Education consultancy'), " +
                                "(newid(), '" + id + "', '731', 'en', 'Advertising activities'), " +
                                "(newid(), '" + id + "', '732', 'en', 'Market research and public opinion polling'), " +
                                "(newid(), '" + id + "', '741', 'en', 'Specialised design activities'), " +
                                "(newid(), '" + id + "', '742', 'en', 'Photographic activities'), " +
                                "(newid(), '" + id + "', '743', 'en', 'Hotel & Tourism Consultancy'), " +
                                "(newid(), '" + id + "', '744', 'en', 'Art & Culture Consultancy'), " +
                                "(newid(), '" + id + "', '745', 'en', 'Quality Management System Consultancy'), " +
                                "(newid(), '" + id + "', '746', 'en', 'Occupational Safety & Health Control Consultancy'), " +
                                "(newid(), '" + id + "', '747', 'en', 'Other professional consultancy services'), " +
                                "(newid(), '" + id + "', '749', 'en', 'Other professional, scientific and technical activities n.e.c'), " +
                                "(newid(), '" + id + "', '750', 'en', 'Veterinary activities'), " +
                                "(newid(), '" + id + "', '771', 'en', 'Renting and leasing of motor vehicles'), " +
                                "(newid(), '" + id + "', '772', 'en', 'Renting and leasing of personal and household goods'), " +
                                "(newid(), '" + id + "', '773', 'en', 'Renting and leasing of other machinery, equipment and tangible goods'), " +
                                "(newid(), '" + id + "', '774', 'en', 'Leasing of intellectual property and similar products except copyrighted works'), " +
                                "(newid(), '" + id + "', '781', 'en', 'Activities of employment placement agencies'), " +
                                "(newid(), '" + id + "', '782', 'en', 'Temporary employment agency activities with supply of own employees'), " +
                                "(newid(), '" + id + "', '783', 'en', 'Other human resources provision'), " +
                                "(newid(), '" + id + "', '784', 'en', 'Labour recruitment and provision of staff'), " +
                                "(newid(), '" + id + "', '791', 'en', 'Travel agency and tour operator activities'), " +
                                "(newid(), '" + id + "', '799', 'en', 'Other reservation service and related activities'), " +
                                "(newid(), '" + id + "', '801', 'en', 'Private security activities'), " +
                                "(newid(), '" + id + "', '802', 'en', 'Security systems service activities'), " +
                                "(newid(), '" + id + "', '803', 'en', 'Investigation activities'), " +
                                "(newid(), '" + id + "', '811', 'en', 'Combined facilities support activities'), " +
                                "(newid(), '" + id + "', '812', 'en', 'Cleaning activities'), " +
                                "(newid(), '" + id + "', '813', 'en', 'Landscape care and maintenance service activities'), " +
                                "(newid(), '" + id + "', '821', 'en', 'Office administrative and support activities'), " +
                                "(newid(), '" + id + "', '822', 'en', 'Activities of call centres'), " +
                                "(newid(), '" + id + "', '823', 'en', 'Organisations of conventions and trade shows'), " +
                                "(newid(), '" + id + "', '824', 'en', 'Outsourced service'), " +
                                "(newid(), '" + id + "', '829', 'en', 'Business support service activities n.e.c.'), " +
                                "(newid(), '" + id + "', '841', 'en', 'Administration of the state and the economic and social policy of the community'), " +
                                "(newid(), '" + id + "', '842', 'en', 'Provision of services to the community as a whole'), " +
                                "(newid(), '" + id + "', '843', 'en', 'Compulsory social security activities'), " +
                                "(newid(), '" + id + "', '851', 'en', 'Pre-primary and primary education'), " +
                                "(newid(), '" + id + "', '852', 'en', 'Secondary education'), " +
                                "(newid(), '" + id + "', '853', 'en', 'Higher education'), " +
                                "(newid(), '" + id + "', '854', 'en', 'Other education'), " +
                                "(newid(), '" + id + "', '855', 'en', 'Educational support activities e.g. educational consulting'), " +
                                "(newid(), '" + id + "', '861', 'en', 'Hospital activities'), " +
                                "(newid(), '" + id + "', '862', 'en', 'Medical and dental practice activities'), " +
                                "(newid(), '" + id + "', '869', 'en', 'Other human health activities'), " +
                                "(newid(), '" + id + "', '871', 'en', 'Residential nursing care activities e.g. homes for elderly'), " +
                                "(newid(), '" + id + "', '872', 'en', 'Residential nursing care activities for mental retardation, mental health and substance abuse excluding hospital activities'), " +
                                "(newid(), '" + id + "', '873', 'en', 'Residential care activities for the elderly and the disabled'), " +
                                "(newid(), '" + id + "', '879', 'en', 'Residential care activities n.e.c e.g. halfway homes, orphanages'), " +
                                "(newid(), '" + id + "', '881', 'en', 'Social work activities without accommodation for the elderly and the disabled'), " +
                                "(newid(), '" + id + "', '889', 'en', 'Other social work activities without accommodation'), " +
                                "(newid(), '" + id + "', '900', 'en', 'Creative, arts and entertainment activities'), " +
                                "(newid(), '" + id + "', '901', 'en', 'Wildlife related commercial activities'), " +
                                "(newid(), '" + id + "', '909', 'en', 'Other entertainment activities n.e.c.(Event Organizer)'), " +
                                "(newid(), '" + id + "', '910', 'en', 'Libraries, archives, museums and other cultural activities'), " +
                                "(newid(), '" + id + "', '920', 'en', 'Gambling and betting activities'), " +
                                "(newid(), '" + id + "', '931', 'en', 'Sports activities'), " +
                                "(newid(), '" + id + "', '932', 'en', 'Other amusement and recreation activities'), " +
                                "(newid(), '" + id + "', '941', 'en', 'Activities of business employers and professional membership organisations'), " +
                                "(newid(), '" + id + "', '942', 'en', 'Activities of trade unions'), " +
                                "(newid(), '" + id + "', '949', 'en', 'Activities of other membership organisations n.e.c.'), " +
                                "(newid(), '" + id + "', '951', 'en', 'Repair of computers and communication equipment'), " +
                                "(newid(), '" + id + "', '952', 'en', 'Repair of personal household goods'), " +
                                "(newid(), '" + id + "', '960', 'en', 'Other personal service activities'), " +
                                "(newid(), '" + id + "', '970', 'en', 'Activities of households as employers of domestic personnel'), " +
                                "(newid(), '" + id + "', '981', 'en', 'Undifferentiated goods-producing activities of private households for own use'), " +
                                "(newid(), '" + id + "', '982', 'en', 'Undifferentiated service-producing activities of private households for own use'), " +
                                "(newid(), '" + id + "', '990', 'en', 'Activities of extraterritorial organisations and bodies'), " +
                                "(newid(), '" + id + "', '9V0', 'en', 'Other activities not adequately defined')"));

            log.log(INFO, "Linking EnsicGroup to EnsicDivision");

            String divisionId;
            try (Result result = con.exec(p.parse("select _id from _platform.lookup.Lookup where name='EnsicDivision'"))) {
              result.next();
              divisionId = result.get(1).value.toString();
            }
            con.exec(p.parse("insert into _platform.lookup.LookupValueLink(_id, name, source_value_id, target_value_id) " +
                                "select newid(), 'EnsicDivision', _id, " +
                                "       (select _id " +
                                "          from target:_platform.lookup.LookupValue  " +
                                "         where target.code=leftstr(lv.code, 2) " +
                                "           and target.lookup_id='" + divisionId + "') " +
                                "from   lv:_platform.lookup.LookupValue where lv.lookup_id='" + id + "'"));
          }
        }

        try (Result rs = con.exec(p.parse("select _id from _platform.lookup.Lookup where name='EnsicClass'"))) {
          if (!rs.next()) {
            log.log(INFO, "Creating EnsicClass lookup");

            UUID id = UUID.randomUUID();
            con.exec(p.parse("insert into _platform.lookup.Lookup(_id, name, description) " +
                                "values(:id, :name, :description)"),
                   Param.of("id", id.toString()),
                   Param.of("name", "EnsicClass"),
                   Param.of("description", "Ensic class"));

            con.exec(p.parse("insert into _platform.lookup.LookupLink(_id, name, display_name, source_lookup_id, target_lookup_id)" +
                                "values(newid(), 'EnsicGroup', 'Ensic Group', '" + id + "', " +
                                "       (select _id from _platform.lookup.Lookup where name='EnsicGroup'))"));

            con.exec(p.parse("insert into _platform.lookup.LookupValue(_id, lookup_id, code, lang, label) values" +
                                "(newid(), '" + id + "', '0111', 'en', 'Growing of cereals, leguminous crops and oil seeds'), " +
                                "(newid(), '" + id + "', '0112', 'en', 'Growing of rice'), " +
                                "(newid(), '" + id + "', '0113', 'en', 'Growing of vegetables and melons, roots and tubes'), " +
                                "(newid(), '" + id + "', '0114', 'en', 'Growing of sugarcane'), " +
                                "(newid(), '" + id + "', '0115', 'en', 'Growing of tobacco'), " +
                                "(newid(), '" + id + "', '0116', 'en', 'Growing of fibre crops'), " +
                                "(newid(), '" + id + "', '0119', 'en', 'Growing of other non-perennial crops'), " +
                                "(newid(), '" + id + "', '0121', 'en', 'Growing of grapes'), " +
                                "(newid(), '" + id + "', '0122', 'en', 'Growing of tropical and subtropical fruits'), " +
                                "(newid(), '" + id + "', '0123', 'en', 'Growing of citrus fruits e.g. lemons, oranges'), " +
                                "(newid(), '" + id + "', '0124', 'en', 'Growing of pome fruits and stone fruits e.g. apples, peaches'), " +
                                "(newid(), '" + id + "', '0125', 'en', 'Growing of other tree and bush fruits and nuts'), " +
                                "(newid(), '" + id + "', '0126', 'en', 'Growing of oleaginous fruits'), " +
                                "(newid(), '" + id + "', '0127', 'en', 'Growing of beverages crops'), " +
                                "(newid(), '" + id + "', '0128', 'en', 'Growing of spices, aromatic, drug and pharmaceutical crops'), " +
                                "(newid(), '" + id + "', '0129', 'en', 'Growing of other perennial crops e.g. Christmas trees'), " +
                                "(newid(), '" + id + "', '0130', 'en', 'Plant propagation'), " +
                                "(newid(), '" + id + "', '0141', 'en', 'Raising of cattle and buffaloes'), " +
                                "(newid(), '" + id + "', '0142', 'en', 'Raising of horses and other equines'), " +
                                "(newid(), '" + id + "', '0143', 'en', 'Raising of camels and camelids'), " +
                                "(newid(), '" + id + "', '0144', 'en', 'Raising of sheep and goats'), " +
                                "(newid(), '" + id + "', '0145', 'en', 'Raising of swine and pigs'), " +
                                "(newid(), '" + id + "', '0146', 'en', 'Raising of poultry'), " +
                                "(newid(), '" + id + "', '0149', 'en', 'Raising of other animals'), " +
                                "(newid(), '" + id + "', '0150', 'en', 'Mixed farming'), " +
                                "(newid(), '" + id + "', '0161', 'en', 'Support services for crop production'), " +
                                "(newid(), '" + id + "', '0162', 'en', 'Support activities for animal production except veterinary activities'), " +
                                "(newid(), '" + id + "', '0163', 'en', 'Post harvest crop activities e.g. cleaning, grading'), " +
                                "(newid(), '" + id + "', '0164', 'en', 'Seed processing for propagation'), " +
                                "(newid(), '" + id + "', '0165', 'en', 'Agricultural And Animal Husbandry Services Activities n.e.c.'), " +
                                "(newid(), '" + id + "', '0170', 'en', 'Hunting, trapping and related service activities'), " +
                                "(newid(), '" + id + "', '0181', 'en', 'Production of Organic Fertiliser'), " +
                                "(newid(), '" + id + "', '0210', 'en', 'Silviculture and other forestry activities'), " +
                                "(newid(), '" + id + "', '0220', 'en', 'Logging'), " +
                                "(newid(), '" + id + "', '0230', 'en', 'Gathering of non-wood forest products'), " +
                                "(newid(), '" + id + "', '0240', 'en', 'Support services and forestry'), " +
                                "(newid(), '" + id + "', '0250', 'en', 'Other activities of forestry and hunting n.e.c'), " +
                                "(newid(), '" + id + "', '0311', 'en', 'Marine fishing'), " +
                                "(newid(), '" + id + "', '0312', 'en', 'Freshwater fishing'), " +
                                "(newid(), '" + id + "', '0321', 'en', 'Marine aquaculture (sea water)'), " +
                                "(newid(), '" + id + "', '0322', 'en', 'Fresh water aquaculture'), " +
                                "(newid(), '" + id + "', '0331', 'en', 'Other activities of fish farm and related services'), " +
                                "(newid(), '" + id + "', '0510', 'en', 'Mining of hard coal'), " +
                                "(newid(), '" + id + "', '0520', 'en', 'Mining of lignite'), " +
                                "(newid(), '" + id + "', '0610', 'en', 'Extraction of crude petroleum'), " +
                                "(newid(), '" + id + "', '0620', 'en', 'Extraction of natural gas'), " +
                                "(newid(), '" + id + "', '0710', 'en', 'Mining of metal ores'), " +
                                "(newid(), '" + id + "', '0721', 'en', 'Mining of uranium and thorium ores'), " +
                                "(newid(), '" + id + "', '0722', 'en', 'Mining of gold and uranium ores'), " +
                                "(newid(), '" + id + "', '0729', 'en', 'Mining of other non-ferrous metal ores'), " +
                                "(newid(), '" + id + "', '0810', 'en', 'Quarrying of stone, sand and clay'), " +
                                "(newid(), '" + id + "', '0891', 'en', 'Mining of chemical and fertiliser minerals'), " +
                                "(newid(), '" + id + "', '0892', 'en', 'Extraction of peat'), " +
                                "(newid(), '" + id + "', '0893', 'en', 'Extraction of salt (including refining by producer)'), " +
                                "(newid(), '" + id + "', '0899', 'en', 'Mining and quarrying n.e.c'), " +
                                "(newid(), '" + id + "', '0910', 'en', 'Support activities for petroleum and natural gas extraction e.g. exploration services, test drilling'), " +
                                "(newid(), '" + id + "', '0990', 'en', 'Support activities for other mining and quarrying'), " +
                                "(newid(), '" + id + "', '0991', 'en', 'Activities for control of natural radio nuclides'), " +
                                "(newid(), '" + id + "', '0992', 'en', 'Research & training and service activities incidental to mining of minerals'), " +
                                "(newid(), '" + id + "', '0993', 'en', 'Prospecting of minerals'), " +
                                "(newid(), '" + id + "', '1010', 'en', 'Processing and preserving of meat'), " +
                                "(newid(), '" + id + "', '1020', 'en', 'Processing and preserving of fish, crustaceans & molluscs'), " +
                                "(newid(), '" + id + "', '1030', 'en', 'Processing and preserving of fruits and vegetables'), " +
                                "(newid(), '" + id + "', '1040', 'en', 'Manufacture of vegetable and animal oils and fats'), " +
                                "(newid(), '" + id + "', '1050', 'en', 'Manufacture of dairy products'), " +
                                "(newid(), '" + id + "', '1061', 'en', 'Manufacture of grain mill products'), " +
                                "(newid(), '" + id + "', '1062', 'en', 'Manufacture of starches and starch products'), " +
                                "(newid(), '" + id + "', '1063', 'en', 'Manufacture of prepared animal feeds'), " +
                                "(newid(), '" + id + "', '1064', 'en', 'other manufacture of grain mill products, starches and starch products and prepared animal feeds'), " +
                                "(newid(), '" + id + "', '1071', 'en', 'Manufacture of bakery products'), " +
                                "(newid(), '" + id + "', '1072', 'en', 'Manufacture of sugar'), " +
                                "(newid(), '" + id + "', '1073', 'en', 'Manufacture of cocoa, chocolate and sugar confectionery'), " +
                                "(newid(), '" + id + "', '1074', 'en', 'Manufacture of macaroni, noodles, couscous and similar farinaceous products'), " +
                                "(newid(), '" + id + "', '1075', 'en', 'Manufacture of prepared meals and dishes (not for immediate consumption)'), " +
                                "(newid(), '" + id + "', '1076', 'en', 'Manufacture of honey products'), " +
                                "(newid(), '" + id + "', '1077', 'en', 'Manufacture of ethanol'), " +
                                "(newid(), '" + id + "', '1079', 'en', 'Manufacture of other food products n.e.c'), " +
                                "(newid(), '" + id + "', '1080', 'en', 'Manufacture of animal feed'), " +
                                "(newid(), '" + id + "', '1101', 'en', 'Manufacture of distilled potable alcoholic beverages'), " +
                                "(newid(), '" + id + "', '1102', 'en', 'Manufacture of wines'), " +
                                "(newid(), '" + id + "', '1103', 'en', 'Manufacture of malt liquors and malt including non alcoholic beer'), " +
                                "(newid(), '" + id + "', '1104', 'en', 'Manufacture of soft drinks; production of mineral waters and other bottled waters'), " +
                                "(newid(), '" + id + "', '1105', 'en', 'Manufacture of traditional liquors and drinks'), " +
                                "(newid(), '" + id + "', '1106', 'en', 'Manufacture of bottled water by recycling'), " +
                                "(newid(), '" + id + "', '1107', 'en', 'Other manufacture of beverages'), " +
                                "(newid(), '" + id + "', '1200', 'en', 'Manufacture of tobacco products'), " +
                                "(newid(), '" + id + "', '1311', 'en', 'Preparation and spinning of textile fibres'), " +
                                "(newid(), '" + id + "', '1312', 'en', 'Weaving of textile'), " +
                                "(newid(), '" + id + "', '1313', 'en', 'Finishing of textiles'), " +
                                "(newid(), '" + id + "', '1391', 'en', 'Manufacture of knitted and crocheted fabrics'), " +
                                "(newid(), '" + id + "', '1392', 'en', 'Manufacture of made-up textile articles; except apparel'), " +
                                "(newid(), '" + id + "', '1393', 'en', 'Manufacture of carpets and rugs'), " +
                                "(newid(), '" + id + "', '1394', 'en', 'Manufacture of cordage, rope twine and netting'), " +
                                "(newid(), '" + id + "', '1395', 'en', 'Manufacture of bags, sacks, rapping & packing materials'), " +
                                "(newid(), '" + id + "', '1399', 'en', 'Manufacture of other textiles n.e.c'), " +
                                "(newid(), '" + id + "', '1410', 'en', 'Manufacture of wearing apparel, except fur apparel'), " +
                                "(newid(), '" + id + "', '1420', 'en', 'Manufacture of articles of fur including wearing apparel'), " +
                                "(newid(), '" + id + "', '1430', 'en', 'Manufacture of knitted and crocheted apparel e.g. pullovers, socks, cardigans'), " +
                                "(newid(), '" + id + "', '1511', 'en', 'Tanning and dressing of leather; dressing and dyeing of fur'), " +
                                "(newid(), '" + id + "', '1512', 'en', 'Manufacture of luggage and handbags and the like, saddlery and harness'), " +
                                "(newid(), '" + id + "', '1513', 'en', 'Other tanning and dressing of leather; manufacture of luggage, handbags, saddler and harness'), " +
                                "(newid(), '" + id + "', '1520', 'en', 'Manufacture of footwear'), " +
                                "(newid(), '" + id + "', '1610', 'en', 'Sawmilling and planing of wood'), " +
                                "(newid(), '" + id + "', '1621', 'en', 'Manufacture of veneer sheets and wood based panels e.g. plywood'), " +
                                "(newid(), '" + id + "', '1622', 'en', 'Manufacture of wooden buildersÆ carpentry and joinery e.g. doors, windows, shutters, stairs'), " +
                                "(newid(), '" + id + "', '1623', 'en', 'Manufacture of wooden containers e.g. boxes, drums, pallets'), " +
                                "(newid(), '" + id + "', '1629', 'en', 'Manufacture of other products of wood, cork, straw and plaiting materials'), " +
                                "(newid(), '" + id + "', '1701', 'en', 'Manufacture of pulp, paper and paperboard'), " +
                                "(newid(), '" + id + "', '1702', 'en', 'Manufacture of corrugated paper and of containers of paper and paperboard'), " +
                                "(newid(), '" + id + "', '1709', 'en', 'Manufacture of other articles of paper and paperboard'), " +
                                "(newid(), '" + id + "', '1811', 'en', 'Printing of newspapers, magazines, books etc.'), " +
                                "(newid(), '" + id + "', '1812', 'en', 'Service activities related to printing'), " +
                                "(newid(), '" + id + "', '1813', 'en', 'Electronics printing'), " +
                                "(newid(), '" + id + "', '1814', 'en', 'Printing of recorded media'), " +
                                "(newid(), '" + id + "', '1819', 'en', 'Other Printing services n.e.c'), " +
                                "(newid(), '" + id + "', '1820', 'en', 'Reproduction of recorded media'), " +
                                "(newid(), '" + id + "', '1910', 'en', 'Manufacture of coke oven products'), " +
                                "(newid(), '" + id + "', '1920', 'en', 'Manufacture of refined petroleum products'), " +
                                "(newid(), '" + id + "', '2011', 'en', 'Manufacture of basic chemicals'), " +
                                "(newid(), '" + id + "', '2012', 'en', 'Manufacture of fertilizers and nitrogen compounds'), " +
                                "(newid(), '" + id + "', '2013', 'en', 'Manufacture of plastics and synthetic rubber in primary forms'), " +
                                "(newid(), '" + id + "', '2014', 'en', 'Other manufacture of basic chemicals'), " +
                                "(newid(), '" + id + "', '2021', 'en', 'Manufacture of pesticides and other agro-chemical products'), " +
                                "(newid(), '" + id + "', '2022', 'en', 'Manufacture of paints, varnishes and similar coatings, printing ink and mastics'), " +
                                "(newid(), '" + id + "', '2023', 'en', 'Manufacture of soap and detergents, cleaning and polishing preparations, perfumes and toilet preparations'), " +
                                "(newid(), '" + id + "', '2029', 'en', 'Manufacture of other chemical products'), " +
                                "(newid(), '" + id + "', '2030', 'en', 'Manufacture of man-made fibres'), " +
                                "(newid(), '" + id + "', '2100', 'en', 'Manufacture of pharmaceuticals, medicinal chemicals and botanical products'), " +
                                "(newid(), '" + id + "', '2211', 'en', 'Manufacture of rubber tyres and tubes, rethreading and rebuilding of rubber tyres'), " +
                                "(newid(), '" + id + "', '2219', 'en', 'Manufacture of other rubber products e.g. balloons, pipes, transmission belts'), " +
                                "(newid(), '" + id + "', '2220', 'en', 'Manufacture of plastic products'), " +
                                "(newid(), '" + id + "', '2310', 'en', 'Manufacture of glass and glass products'), " +
                                "(newid(), '" + id + "', '2391', 'en', 'Manufacture of refractory products'), " +
                                "(newid(), '" + id + "', '2392', 'en', 'Manufacture of clay and ceramic building materials e.g. non-refractory wall tiles, sanitary fixtures'), " +
                                "(newid(), '" + id + "', '2393', 'en', 'Manufacture of porcelain and ceramic furniture n.e.c e.g. statuettes, pots, jars'), " +
                                "(newid(), '" + id + "', '2394', 'en', 'Manufacture of cement, lime and plaster'), " +
                                "(newid(), '" + id + "', '2395', 'en', 'Manufacture of articles of concrete, cement and plaster'), " +
                                "(newid(), '" + id + "', '2396', 'en', 'Cutting, shaping and finishing of stone'), " +
                                "(newid(), '" + id + "', '2397', 'en', 'Manufacture of non-structural non-refractory ceramic ware'), " +
                                "(newid(), '" + id + "', '2398', 'en', 'Manufacture of souvenirs, handcraft and artificial jewellery'), " +
                                "(newid(), '" + id + "', '2399', 'en', 'Manufacture of other non-metallic mineral products e.g. sandpaper, sharpening stone (mineral insulating materials)'), " +
                                "(newid(), '" + id + "', '2410', 'en', 'Manufacture of basic iron and steel'), " +
                                "(newid(), '" + id + "', '2420', 'en', 'Manufacture of basic precious and non-ferrous metals'), " +
                                "(newid(), '" + id + "', '2431', 'en', 'Casting of iron and steel including manufacture of tubes, pipes etc., of cast iron and steel'), " +
                                "(newid(), '" + id + "', '2432', 'en', 'Casting of non-ferrous metals'), " +
                                "(newid(), '" + id + "', '2433', 'en', 'Casting of gold'), " +
                                "(newid(), '" + id + "', '2434', 'en', 'Other casting of metals'), " +
                                "(newid(), '" + id + "', '2511', 'en', 'Manufacture of structural metal products (e.g. doors, frame, shutters, metal frame works)'), " +
                                "(newid(), '" + id + "', '2512', 'en', 'Manufacture of tanks, reservoirs and containers of metal of types normally installed as fixtures for storage or manufacturing use'), " +
                                "(newid(), '" + id + "', '2513', 'en', 'Manufacture of steam generators, except hot water boilers'), " +
                                "(newid(), '" + id + "', '2520', 'en', 'Manufacture of weapons and ammunitions'), " +
                                "(newid(), '" + id + "', '2591', 'en', 'Forging, pressing, stamping and roll-forming of metal; powder metallurgy'), " +
                                "(newid(), '" + id + "', '2592', 'en', 'Treatment and coating of metals; machining'), " +
                                "(newid(), '" + id + "', '2593', 'en', 'Manufacture of cutlery, hand tools and general hardware'), " +
                                "(newid(), '" + id + "', '2599', 'en', 'Manufacture of other fabricated metal products'), " +
                                "(newid(), '" + id + "', '2610', 'en', 'Manufacture of electronic components and boards'), " +
                                "(newid(), '" + id + "', '2620', 'en', 'Manufacture of computers and peripheral equipment'), " +
                                "(newid(), '" + id + "', '2630', 'en', 'Manufacture of communication equipment'), " +
                                "(newid(), '" + id + "', '2640', 'en', 'Manufacture of consumer electronics'), " +
                                "(newid(), '" + id + "', '2651', 'en', 'Manufacture of measuring, testing, navigating and control equipment'), " +
                                "(newid(), '" + id + "', '2652', 'en', 'Manufacture of watches & clocks'), " +
                                "(newid(), '" + id + "', '2660', 'en', 'Manufacture of irradiation, electromedical and electrotherapeutic equipment (e.g. scanners, hearing aids)'), " +
                                "(newid(), '" + id + "', '2670', 'en', 'Manufacture of optical instruments and photographic equipment'), " +
                                "(newid(), '" + id + "', '2680', 'en', 'Manufacture of magnetic and optical media'), " +
                                "(newid(), '" + id + "', '2690', 'en', 'Manufacture of medical appliances and instruments and appliances for measuring, checking, testing, navigating and for other purposes, except optical instruments'), " +
                                "(newid(), '" + id + "', '2710', 'en', 'Manufacture of electric motors, generators, transformers and electricity distribution and control apparatus'), " +
                                "(newid(), '" + id + "', '2720', 'en', 'Manufacture of batteries and accumulators'), " +
                                "(newid(), '" + id + "', '2731', 'en', 'Manufacture of fibre optic cables'), " +
                                "(newid(), '" + id + "', '2732', 'en', 'Manufacture of other electronic and electric wires and cables'), " +
                                "(newid(), '" + id + "', '2733', 'en', 'Manufacture of wiring devices e.g. switch boxes, sockets'), " +
                                "(newid(), '" + id + "', '2740', 'en', 'Manufacture of electrical lighting equipment'), " +
                                "(newid(), '" + id + "', '2750', 'en', 'Manufacture of domestic appliances'), " +
                                "(newid(), '" + id + "', '2760', 'en', 'Manufacture of energy-saving technology devices'), " +
                                "(newid(), '" + id + "', '2790', 'en', 'Manufacture of other electrical equipment'), " +
                                "(newid(), '" + id + "', '2811', 'en', 'Manufacture of engines and turbines except aircraft, vehicle and cycle engines'), " +
                                "(newid(), '" + id + "', '2812', 'en', 'Manufacture of fluid power equipment'), " +
                                "(newid(), '" + id + "', '2813', 'en', 'Manufacture of other pumps, compressors, taps and valves'), " +
                                "(newid(), '" + id + "', '2814', 'en', 'Manufacture of bearings, gears, gearing and driving elements'), " +
                                "(newid(), '" + id + "', '2815', 'en', 'Manufacture of ovens, furnaces and furnace burners'), " +
                                "(newid(), '" + id + "', '2816', 'en', 'Manufacture of lifting and handling equipment'), " +
                                "(newid(), '" + id + "', '2817', 'en', 'Manufacture of office machinery and equipment (except computers and peripheral equipment)'), " +
                                "(newid(), '" + id + "', '2818', 'en', 'Manufacture of power driven hand tools'), " +
                                "(newid(), '" + id + "', '2819', 'en', 'Manufacture of other general-purpose machinery'), " +
                                "(newid(), '" + id + "', '2821', 'en', 'Manufacture of agricultural and forestry machinery'), " +
                                "(newid(), '" + id + "', '2822', 'en', 'Manufacture of metal-forming machinery and machine tools'), " +
                                "(newid(), '" + id + "', '2823', 'en', 'Manufacture of machinery for metallurgy'), " +
                                "(newid(), '" + id + "', '2824', 'en', 'Manufacture of machinery for mining, quarrying and construction'), " +
                                "(newid(), '" + id + "', '2825', 'en', 'Manufacture of machinery for food, beverage and tobacco processing'), " +
                                "(newid(), '" + id + "', '2826', 'en', 'Manufacture of machinery for textile, apparel and leather production'), " +
                                "(newid(), '" + id + "', '2827', 'en', 'Manufacture of mills from stone'), " +
                                "(newid(), '" + id + "', '2829', 'en', 'Manufacture of special-purpose machinery n.e.c'), " +
                                "(newid(), '" + id + "', '2910', 'en', 'Manufacture of motor vehicles'), " +
                                "(newid(), '" + id + "', '2920', 'en', 'Manufacture of bodies (coachwork) for motor vehicles; manufacture of trailers and semi-trailers'), " +
                                "(newid(), '" + id + "', '2930', 'en', 'Manufacture of parts and accessories for motor vehicles and their engines'), " +
                                "(newid(), '" + id + "', '3011', 'en', 'Building of ships and floating structures'), " +
                                "(newid(), '" + id + "', '3012', 'en', 'Building of pleasure and sporting boats'), " +
                                "(newid(), '" + id + "', '3020', 'en', 'Manufacture of railway, locomotives and rolling stock'), " +
                                "(newid(), '" + id + "', '3030', 'en', 'Manufacture of air and spacecraft and related machinery'), " +
                                "(newid(), '" + id + "', '3040', 'en', 'Manufacture of military fighting vehicles'), " +
                                "(newid(), '" + id + "', '3091', 'en', 'Manufacture of motorcycles'), " +
                                "(newid(), '" + id + "', '3092', 'en', 'Manufacture of bicycles and invalid carriages'), " +
                                "(newid(), '" + id + "', '3099', 'en', 'Manufacture of transport equipment n.e.c'), " +
                                "(newid(), '" + id + "', '3100', 'en', 'Manufacture of furniture'), " +
                                "(newid(), '" + id + "', '3211', 'en', 'Diamond cutting and processing'), " +
                                "(newid(), '" + id + "', '3212', 'en', 'Manufacture of imitation jewellery and related articles'), " +
                                "(newid(), '" + id + "', '3220', 'en', 'Manufacture of musical instruments'), " +
                                "(newid(), '" + id + "', '3230', 'en', 'Manufacture of sports goods'), " +
                                "(newid(), '" + id + "', '3240', 'en', 'Manufacture of games and toys'), " +
                                "(newid(), '" + id + "', '3250', 'en', 'Manufacture of medical and dental instruments and supplies'), " +
                                "(newid(), '" + id + "', '3290', 'en', 'Other manufacturing n.e.c'), " +
                                "(newid(), '" + id + "', '3311', 'en', 'Repair of fabricated metal products'), " +
                                "(newid(), '" + id + "', '3312', 'en', 'Repair of machinery'), " +
                                "(newid(), '" + id + "', '3313', 'en', 'Repair of electronic and optical instrument'), " +
                                "(newid(), '" + id + "', '3314', 'en', 'Repair of electrical equipment'), " +
                                "(newid(), '" + id + "', '3315', 'en', 'Repair of transport equipment, except motor vehicles'), " +
                                "(newid(), '" + id + "', '3319', 'en', 'Repair of other equipment n.e.c'), " +
                                "(newid(), '" + id + "', '3320', 'en', 'Installation of industrial machinery and equipment'), " +
                                "(newid(), '" + id + "', '3330', 'en', 'Equipment and machinery installation and maintenance service'), " +
                                "(newid(), '" + id + "', '3510', 'en', 'Electric power generation, transmission and distribution'), " +
                                "(newid(), '" + id + "', '3520', 'en', 'Manufacture of gas; distribution of gaseous fuels through mains'), " +
                                "(newid(), '" + id + "', '3530', 'en', 'Steam and air conditioning supply'), " +
                                "(newid(), '" + id + "', '3600', 'en', 'Water collection, treatment and supply'), " +
                                "(newid(), '" + id + "', '3700', 'en', 'Sewerage'), " +
                                "(newid(), '" + id + "', '3811', 'en', 'Collection of non-hazardous waste'), " +
                                "(newid(), '" + id + "', '3812', 'en', 'Collection of hazardous waste'), " +
                                "(newid(), '" + id + "', '3821', 'en', 'Treatment and disposal of non-hazardous waste'), " +
                                "(newid(), '" + id + "', '3822', 'en', 'Treatment and disposal of hazardous waste'), " +
                                "(newid(), '" + id + "', '3830', 'en', 'Materials recovery'), " +
                                "(newid(), '" + id + "', '3900', 'en', 'Remediation activities and other waste management services'), " +
                                "(newid(), '" + id + "', '4100', 'en', 'Construction of buildings'), " +
                                "(newid(), '" + id + "', '4210', 'en', 'Construction of roads and railways'), " +
                                "(newid(), '" + id + "', '4220', 'en', 'Construction of utility projects e.g. pipelines, communication and power lines'), " +
                                "(newid(), '" + id + "', '4290', 'en', 'Construction of other civil engineering projects'), " +
                                "(newid(), '" + id + "', '4311', 'en', 'Demolition of buildings'), " +
                                "(newid(), '" + id + "', '4312', 'en', 'Site preparation e.g. renting of equipment with operator, earth moving'), " +
                                "(newid(), '" + id + "', '4313', 'en', 'Other activities of demolition'), " +
                                "(newid(), '" + id + "', '4321', 'en', 'Electrical installation'), " +
                                "(newid(), '" + id + "', '4322', 'en', 'Plumbing, heat and air conditioning installation'), " +
                                "(newid(), '" + id + "', '4329', 'en', 'Other construction installation'), " +
                                "(newid(), '" + id + "', '4330', 'en', 'Building completion and finishing'), " +
                                "(newid(), '" + id + "', '4390', 'en', 'Other specialised construction activities'), " +
                                "(newid(), '" + id + "', '4510', 'en', 'Sale of motor vehicles'), " +
                                "(newid(), '" + id + "', '4511', 'en', 'Wholesale sale of motor vehicles a. Wholesale of new motor vehicles b. Wholesale of used motor vehicles'), " +
                                "(newid(), '" + id + "', '4512', 'en', 'Retail sale of motor vehicles'), " +
                                "(newid(), '" + id + "', '4520', 'en', 'Maintenance and repair of motor vehicles'), " +
                                "(newid(), '" + id + "', '4530', 'en', 'Sale of motor vehicles parts and accessories'), " +
                                "(newid(), '" + id + "', '4531', 'en', 'Wholesale trade in vehicles tyres and battery'), " +
                                "(newid(), '" + id + "', '4532', 'en', 'Sale of new parts and accessories'), " +
                                "(newid(), '" + id + "', '4533', 'en', 'Sale of parts and accessories of vehicles a.ááá Sales of new parts and accessories b.áá Sales of used parts and accessories'), " +
                                "(newid(), '" + id + "', '4540', 'en', 'Sale, maintenance and repair of motorcycles and related parts and accessories'), " +
                                "(newid(), '" + id + "', '4610', 'en', 'Wholesale on a fee or contract basis'), " +
                                "(newid(), '" + id + "', '4620', 'en', 'Wholesale of agricultural raw materials and live animals'), " +
                                "(newid(), '" + id + "', '4621', 'en', 'Wholesale trade in agricultural raw materials'), " +
                                "(newid(), '" + id + "', '4622', 'en', 'Wholesale trade in livestock and livestock products'), " +
                                "(newid(), '" + id + "', '4630', 'en', 'Wholesale of food, beverages and tobacco'), " +
                                "(newid(), '" + id + "', '4631', 'en', 'Wholesale trade in food'), " +
                                "(newid(), '" + id + "', '4632', 'en', 'Wholesale trade in beverages'), " +
                                "(newid(), '" + id + "', '4633', 'en', 'Wholesale trade in processed agricultural products'), " +
                                "(newid(), '" + id + "', '4634', 'en', 'Wholesale trade in tobacco and tobacco products'), " +
                                "(newid(), '" + id + "', '4635', 'en', 'Whole sale of flour products'), " +
                                "(newid(), '" + id + "', '4641', 'en', 'Wholesale of textiles, clothing and footwear'), " +
                                "(newid(), '" + id + "', '4642', 'en', 'Wholesale of furniture, home furnishings and other household equipment'), " +
                                "(newid(), '" + id + "', '4643', 'en', 'Wholesale of Sporting and other Recreational Goods'), " +
                                "(newid(), '" + id + "', '4644', 'en', 'Wholesale of Paper, Paper Products, packaging materials including plastic and fibre materials and Stationery'), " +
                                "(newid(), '" + id + "', '4649', 'en', 'Wholesale of other household goods'), " +
                                "(newid(), '" + id + "', '4651', 'en', 'Wholesale of computers, computer peripheral equipment and software'), " +
                                "(newid(), '" + id + "', '4652', 'en', 'Wholesale of electronic and telecommunications equipment and parts'), " +
                                "(newid(), '" + id + "', '4653', 'en', 'Wholesale of agricultural machinery, equipment and supplies'), " +
                                "(newid(), '" + id + "', '4654', 'en', 'Wholesale trade in industrial machinery and equipment'), " +
                                "(newid(), '" + id + "', '4655', 'en', 'Wholesale trade in construction equipment (e.g. concrete mixer)'), " +
                                "(newid(), '" + id + "', '4656', 'en', 'Wholesale trade in lifts, escalators and industrial and office air-conditioning equipment'), " +
                                "(newid(), '" + id + "', '4657', 'en', 'Wholesale trade in Grain mill and its spare parts'), " +
                                "(newid(), '" + id + "', '4658', 'en', 'Wholesale trade in pagers, hand phones and other telecommunications apparatus (e.g. palmtops, smart watches, wearable computer and electronic books)'), " +
                                "(newid(), '" + id + "', '4659', 'en', 'Wholesale of machinery and equipment n.e.c'), " +
                                "(newid(), '" + id + "', '4661', 'en', 'Wholesale of solid, liquid and gaseous fuels and related products'), " +
                                "(newid(), '" + id + "', '4662', 'en', 'Wholesale of metals and ores'), " +
                                "(newid(), '" + id + "', '4663', 'en', 'Wholesale of construction materials, hardware, plumbing and heating equipment and supplies'), " +
                                "(newid(), '" + id + "', '4664', 'en', 'Wholesale trade in non-metallic minerals'), " +
                                "(newid(), '" + id + "', '4665', 'en', 'Wholesale of chemicals and chemical products'), " +
                                "(newid(), '" + id + "', '4669', 'en', 'Wholesale of waste and scrap and other products n.e.c.'), " +
                                "(newid(), '" + id + "', '4671', 'en', 'Wholesale trade in other agricultural products'), " +
                                "(newid(), '" + id + "', '4690', 'en', 'Non-specialised wholesale trade'), " +
                                "(newid(), '" + id + "', '4701', 'en', 'Retail trade of agricultural, industrial, construction and other equipment'), " +
                                "(newid(), '" + id + "', '4711', 'en', 'Retail sale in non-specialised stores with food, beverages or tobacco predominating'), " +
                                "(newid(), '" + id + "', '4719', 'en', 'Other retail sale in non-specialised stores(non foodstuff predominant)'), " +
                                "(newid(), '" + id + "', '4721', 'en', 'Retail sale of food in specialised stores'), " +
                                "(newid(), '" + id + "', '4722', 'en', 'Retail sale of beverages in specialised stores (not consumed on the premises)'), " +
                                "(newid(), '" + id + "', '4723', 'en', 'Retail sale of tobacco in stores'), " +
                                "(newid(), '" + id + "', '4724', 'en', 'Retail Trade In Agricultural Raw Materials'), " +
                                "(newid(), '" + id + "', '4725', 'en', 'Retail trade of livestock'), " +
                                "(newid(), '" + id + "', '4726', 'en', 'Retail trade of other agricultural products'), " +
                                "(newid(), '" + id + "', '4730', 'en', 'Retail sale of automotive fuel'), " +
                                "(newid(), '" + id + "', '4741', 'en', 'Retail sale of computers, peripheral units, software and telecommunications equipment in specialised stores'), " +
                                "(newid(), '" + id + "', '4742', 'en', 'Retail sale of audio and video equipment in specialised stores'), " +
                                "(newid(), '" + id + "', '4751', 'en', 'Retail sale of textiles (including haberdashery) in specialised stores'), " +
                                "(newid(), '" + id + "', '4752', 'en', 'Retail sale of hardware, paints and glass in specialised stores'), " +
                                "(newid(), '" + id + "', '4753', 'en', 'Retail sale of carpets, rugs, wall and floor coverings in specialised stores'), " +
                                "(newid(), '" + id + "', '4759', 'en', 'Retail sale of electrical household appliances, furniture, lighting equipment and other household articles in specialised stores'), " +
                                "(newid(), '" + id + "', '4761', 'en', 'Retail sale of books, newspapers and stationery in specialised stores'), " +
                                "(newid(), '" + id + "', '4762', 'en', 'Retail sale of music and video recordings in specialised stores'), " +
                                "(newid(), '" + id + "', '4763', 'en', 'Retail sale of sporting equipment in specialised stores'), " +
                                "(newid(), '" + id + "', '4764', 'en', 'Retail sale of games and toys (except video games) in specialised stores'), " +
                                "(newid(), '" + id + "', '4765', 'en', 'Retail trade in musical instruments'), " +
                                "(newid(), '" + id + "', '4770', 'en', 'Retail trade of non-agricultural intermediate products, waste and scrap'), " +
                                "(newid(), '" + id + "', '4771', 'en', 'Retail sale of clothing, footwear and leather articles in specialised stores'), " +
                                "(newid(), '" + id + "', '4772', 'en', 'Retail sale of pharmaceutical and medical goods, cosmetic and toilet articles in specialised stores'), " +
                                "(newid(), '" + id + "', '4773', 'en', 'Other retail sale of new goods in specialised stores'), " +
                                "(newid(), '" + id + "', '4774', 'en', 'Retail sale of second-hand goods in stores'), " +
                                "(newid(), '" + id + "', '4775', 'en', 'Retail trade of veterinary drugs and medicines'), " +
                                "(newid(), '" + id + "', '4776', 'en', 'Retail trade radiation emitting equipment and radio active sources'), " +
                                "(newid(), '" + id + "', '4777', 'en', 'Retail trade of construction materials hardware, plumbing (including pvc pipes) and heating equipment and supplies'), " +
                                "(newid(), '" + id + "', '4778', 'en', 'Retail trade of photographic and optical materials (including eye glass)'), " +
                                "(newid(), '" + id + "', '4779', 'en', 'Retail trade in other machinery and equipment'), " +
                                "(newid(), '" + id + "', '4781', 'en', 'Retail sale via stalls and markets of food, beverages and tobacco products'), " +
                                "(newid(), '" + id + "', '4782', 'en', 'Retail sale via stalls and markets of textiles, clothing and footwear'), " +
                                "(newid(), '" + id + "', '4789', 'en', 'Retail sale via stalls and markets of other goods'), " +
                                "(newid(), '" + id + "', '4791', 'en', 'Retail sale via mail order houses or via internet'), " +
                                "(newid(), '" + id + "', '4792', 'en', 'Retail trade of sheet metals'), " +
                                "(newid(), '" + id + "', '4799', 'en', 'Other retail sale not in stores, stalls and markets'), " +
                                "(newid(), '" + id + "', '4911', 'en', 'Passenger rail transport interurban'), " +
                                "(newid(), '" + id + "', '4912', 'en', 'Freight rail transport'), " +
                                "(newid(), '" + id + "', '4913', 'en', 'Railway commuter services'), " +
                                "(newid(), '" + id + "', '4921', 'en', 'Urban and suburban passenger land transport'), " +
                                "(newid(), '" + id + "', '4922', 'en', 'Other passenger land transport'), " +
                                "(newid(), '" + id + "', '4923', 'en', 'Freight transport by road'), " +
                                "(newid(), '" + id + "', '4930', 'en', 'Transport via pipelines'), " +
                                "(newid(), '" + id + "', '5011', 'en', 'Sea and coastal passenger water transport'), " +
                                "(newid(), '" + id + "', '5012', 'en', 'Sea and coastal freight water transport'), " +
                                "(newid(), '" + id + "', '5021', 'en', 'Inland passenger water transport'), " +
                                "(newid(), '" + id + "', '5022', 'en', 'Inland freight water transport'), " +
                                "(newid(), '" + id + "', '5110', 'en', 'Passenger air transport'), " +
                                "(newid(), '" + id + "', '5120', 'en', 'Freight air transport'), " +
                                "(newid(), '" + id + "', '5130', 'en', 'Aviation Support Activities'), " +
                                "(newid(), '" + id + "', '5190', 'en', 'Other Activities of Air Transport n.e.c'), " +
                                "(newid(), '" + id + "', '5210', 'en', 'Warehousing and storage'), " +
                                "(newid(), '" + id + "', '5221', 'en', 'Service activities incidental to land transportation'), " +
                                "(newid(), '" + id + "', '5222', 'en', 'Service activities incidental to water transportation'), " +
                                "(newid(), '" + id + "', '5223', 'en', 'Service activities incidental to air transportation'), " +
                                "(newid(), '" + id + "', '5224', 'en', 'Cargo handling'), " +
                                "(newid(), '" + id + "', '5229', 'en', 'Other transportation support activities'), " +
                                "(newid(), '" + id + "', '5310', 'en', 'Postal activities operating under universal service obligation'), " +
                                "(newid(), '" + id + "', '5320', 'en', 'Courier activities not operating under universal service obligation'), " +
                                "(newid(), '" + id + "', '5330', 'en', 'Other postal and related courier activities'), " +
                                "(newid(), '" + id + "', '5510', 'en', 'Short-term accommodation activities'), " +
                                "(newid(), '" + id + "', '5520', 'en', 'Camping grounds, recreational vehicle parks and trailer parks'), " +
                                "(newid(), '" + id + "', '5530', 'en', 'Hotel management company'), " +
                                "(newid(), '" + id + "', '5590', 'en', 'Accommodation n.e.c e.g. workers hostels, boarding houses'), " +
                                "(newid(), '" + id + "', '5610', 'en', 'Restaurants and mobile food service activities'), " +
                                "(newid(), '" + id + "', '5621', 'en', 'Event catering e.g. for wedding'), " +
                                "(newid(), '" + id + "', '5629', 'en', 'Other food service activities'), " +
                                "(newid(), '" + id + "', '5630', 'en', 'Beverage serving facilities'), " +
                                "(newid(), '" + id + "', '5811', 'en', 'Publishing of books'), " +
                                "(newid(), '" + id + "', '5812', 'en', 'Publishing of directories and mailing lists'), " +
                                "(newid(), '" + id + "', '5813', 'en', 'Publishing of newspapers, journals and periodicals'), " +
                                "(newid(), '" + id + "', '5819', 'en', 'Other publishing activities'), " +
                                "(newid(), '" + id + "', '5820', 'en', 'Software publishing'), " +
                                "(newid(), '" + id + "', '5911', 'en', 'Production of motion pictures, videos, television programmes or television commercials'), " +
                                "(newid(), '" + id + "', '5912', 'en', 'Motion picture, video and television programme, post-production activities'), " +
                                "(newid(), '" + id + "', '5913', 'en', 'Motion picture, video and television programme distribution activities'), " +
                                "(newid(), '" + id + "', '5914', 'en', 'Motion picture projection activities'), " +
                                "(newid(), '" + id + "', '5915', 'en', 'Related activities - film and tape renting to other industries, booking, delivery and storage'), " +
                                "(newid(), '" + id + "', '5920', 'en', 'Sound recording and music publishing activities'), " +
                                "(newid(), '" + id + "', '6010', 'en', 'Radio broadcasting'), " +
                                "(newid(), '" + id + "', '6020', 'en', 'Television programming and broadcasting activities'), " +
                                "(newid(), '" + id + "', '6110', 'en', 'Wired telecommunications activities'), " +
                                "(newid(), '" + id + "', '6120', 'en', 'Wireless telecommunications activities'), " +
                                "(newid(), '" + id + "', '6130', 'en', 'Satellite telecommunications activities'), " +
                                "(newid(), '" + id + "', '6190', 'en', 'Other telecommunications activities'), " +
                                "(newid(), '" + id + "', '6201', 'en', 'Computer programming'), " +
                                "(newid(), '" + id + "', '6202', 'en', 'Computer consultancy and computer facilities management activities'), " +
                                "(newid(), '" + id + "', '6209', 'en', 'Other information technology and computer service activities e.g. computer disaster recovery'), " +
                                "(newid(), '" + id + "', '6311', 'en', 'Data processing, hosting and related activities'), " +
                                "(newid(), '" + id + "', '6312', 'en', 'Web portals'), " +
                                "(newid(), '" + id + "', '6391', 'en', 'News agency activities'), " +
                                "(newid(), '" + id + "', '6392', 'en', 'Geographical information service'), " +
                                "(newid(), '" + id + "', '6399', 'en', 'Other information service activities n.e.c e.g. telephone based information service'), " +
                                "(newid(), '" + id + "', '6411', 'en', 'Central banking'), " +
                                "(newid(), '" + id + "', '6419', 'en', 'Other monetary intermediation'), " +
                                "(newid(), '" + id + "', '6420', 'en', 'Activities of holding companies, without managing'), " +
                                "(newid(), '" + id + "', '6430', 'en', 'Trusts, funds and similar financial entities, without managing'), " +
                                "(newid(), '" + id + "', '6491', 'en', 'Financial leasing'), " +
                                "(newid(), '" + id + "', '6492', 'en', 'Other credit granting'), " +
                                "(newid(), '" + id + "', '6499', 'en', 'Financial service activities n.e.c e.g. own account investment activities'), " +
                                "(newid(), '" + id + "', '6511', 'en', 'Life insurance'), " +
                                "(newid(), '" + id + "', '6512', 'en', 'Non-life insurance'), " +
                                "(newid(), '" + id + "', '6520', 'en', 'Reinsurance'), " +
                                "(newid(), '" + id + "', '6530', 'en', 'Pension funding'), " +
                                "(newid(), '" + id + "', '6540', 'en', 'Medical aid funding Institutions'), " +
                                "(newid(), '" + id + "', '6550', 'en', 'Plant and machinery valuation'), " +
                                "(newid(), '" + id + "', '6590', 'en', 'Other insurance n.e.c.'), " +
                                "(newid(), '" + id + "', '6611', 'en', 'Administration of financial markets e.g. stock and security exchanges'), " +
                                "(newid(), '" + id + "', '6612', 'en', 'Security and commodity contracts brokerage'), " +
                                "(newid(), '" + id + "', '6619', 'en', 'Other activities auxiliary to financial service activities'), " +
                                "(newid(), '" + id + "', '6621', 'en', 'Risk and damage evaluation'), " +
                                "(newid(), '" + id + "', '6622', 'en', 'Activities of insurance agents and brokers'), " +
                                "(newid(), '" + id + "', '6629', 'en', 'Activities auxiliary to insurance and pension funding n.e.c e.g. actuarial services'), " +
                                "(newid(), '" + id + "', '6630', 'en', 'Fund management activities'), " +
                                "(newid(), '" + id + "', '6810', 'en', 'Real estate activities with own or leased property'), " +
                                "(newid(), '" + id + "', '6820', 'en', 'Real estate activities on a fee or contract basis'), " +
                                "(newid(), '" + id + "', '6910', 'en', 'Legal activities'), " +
                                "(newid(), '" + id + "', '6920', 'en', 'Accounting, book-keeping and auditing activities; tax consultancy'), " +
                                "(newid(), '" + id + "', '7010', 'en', 'Activities of head offices'), " +
                                "(newid(), '" + id + "', '7020', 'en', 'Management consultancy activities'), " +
                                "(newid(), '" + id + "', '7110', 'en', 'Architectural and engineering activities; and related technical consultancy'), " +
                                "(newid(), '" + id + "', '7120', 'en', 'Technical testing and analysis'), " +
                                "(newid(), '" + id + "', '7210', 'en', 'Research and experimental development on natural sciences and engineering'), " +
                                "(newid(), '" + id + "', '7220', 'en', 'Research and experimental development on social sciences and humanities'), " +
                                "(newid(), '" + id + "', '7230', 'en', 'Education consultancy'), " +
                                "(newid(), '" + id + "', '7310', 'en', 'Advertising activities'), " +
                                "(newid(), '" + id + "', '7320', 'en', 'Market research and public opinion polling'), " +
                                "(newid(), '" + id + "', '7410', 'en', 'Specialised design activities'), " +
                                "(newid(), '" + id + "', '7420', 'en', 'Photographic activities'), " +
                                "(newid(), '" + id + "', '7430', 'en', 'Hotel & Tourism Consultancy'), " +
                                "(newid(), '" + id + "', '7440', 'en', 'Art & Culture Consultancy'), " +
                                "(newid(), '" + id + "', '7450', 'en', 'Quality Management System Consultancy'), " +
                                "(newid(), '" + id + "', '7460', 'en', 'Occupational Safety & Health Control Consultancy'), " +
                                "(newid(), '" + id + "', '7470', 'en', 'Other professional consultancy services'), " +
                                "(newid(), '" + id + "', '7490', 'en', 'Other professional, scientific and technical activities n.e.c'), " +
                                "(newid(), '" + id + "', '7500', 'en', 'Veterinary activities'), " +
                                "(newid(), '" + id + "', '7710', 'en', 'Renting and leasing of motor vehicles'), " +
                                "(newid(), '" + id + "', '7721', 'en', 'Renting and leasing of recreational and sports goods'), " +
                                "(newid(), '" + id + "', '7722', 'en', 'Renting of video tapes and cassettes, CDs, DVDs etc. (video club)'), " +
                                "(newid(), '" + id + "', '7729', 'en', 'Renting and leasing of personal and household goods'), " +
                                "(newid(), '" + id + "', '7730', 'en', 'Renting and leasing of other machinery, equipment and tangible goods'), " +
                                "(newid(), '" + id + "', '7740', 'en', 'Leasing of intellectual property and similar products except copyrighted works'), " +
                                "(newid(), '" + id + "', '7810', 'en', 'Activities of employment placement agencies'), " +
                                "(newid(), '" + id + "', '7820', 'en', 'Temporary employment agency activities with supply of own employees'), " +
                                "(newid(), '" + id + "', '7830', 'en', 'Other human resources provision'), " +
                                "(newid(), '" + id + "', '7840', 'en', 'Labour recruitment and provision of staff'), " +
                                "(newid(), '" + id + "', '7911', 'en', 'Travel agency activities'), " +
                                "(newid(), '" + id + "', '7912', 'en', 'Tour operator activities'), " +
                                "(newid(), '" + id + "', '7990', 'en', 'Other reservation service and related activities'), " +
                                "(newid(), '" + id + "', '8010', 'en', 'Private security activities'), " +
                                "(newid(), '" + id + "', '8020', 'en', 'Security systems service activities'), " +
                                "(newid(), '" + id + "', '8030', 'en', 'Investigation activities'), " +
                                "(newid(), '" + id + "', '8110', 'en', 'Combined facilities support activities'), " +
                                "(newid(), '" + id + "', '8121', 'en', 'General cleaning of buildings'), " +
                                "(newid(), '" + id + "', '8129', 'en', 'Other building and industrial cleaning activities'), " +
                                "(newid(), '" + id + "', '8130', 'en', 'Landscape care and maintenance service activities'), " +
                                "(newid(), '" + id + "', '8211', 'en', 'Combined office administrative service activities for others'), " +
                                "(newid(), '" + id + "', '8219', 'en', 'Photocopying, document preparation and other specialised office support activities'), " +
                                "(newid(), '" + id + "', '8220', 'en', 'Activities of call centres'), " +
                                "(newid(), '" + id + "', '8230', 'en', 'Organisations of conventions and trade shows'), " +
                                "(newid(), '" + id + "', '8240', 'en', 'Outsourced service'), " +
                                "(newid(), '" + id + "', '8291', 'en', 'Activities of collection agencies and credit bureaus'), " +
                                "(newid(), '" + id + "', '8292', 'en', 'Packaging activities'), " +
                                "(newid(), '" + id + "', '8299', 'en', 'Business support service activities n.e.c'), " +
                                "(newid(), '" + id + "', '8411', 'en', 'General (overall) public service activities'), " +
                                "(newid(), '" + id + "', '8412', 'en', 'Regulation of the activities of providing health care, education, cultural services and other social services, excluding social security'), " +
                                "(newid(), '" + id + "', '8413', 'en', 'Regulation of and contribution to more efficient operation of business'), " +
                                "(newid(), '" + id + "', '8421', 'en', 'Foreign affairs'), " +
                                "(newid(), '" + id + "', '8422', 'en', 'Defence activities'), " +
                                "(newid(), '" + id + "', '8423', 'en', 'Public order and safety activities'), " +
                                "(newid(), '" + id + "', '8430', 'en', 'Compulsory social security activities'), " +
                                "(newid(), '" + id + "', '8510', 'en', 'Pre-primary and primary education'), " +
                                "(newid(), '" + id + "', '8521', 'en', 'General secondary education'), " +
                                "(newid(), '" + id + "', '8522', 'en', 'Technical and vocational secondary education'), " +
                                "(newid(), '" + id + "', '8530', 'en', 'Higher education'), " +
                                "(newid(), '" + id + "', '8541', 'en', 'Sports and recreation education e.g. swimming, yoga'), " +
                                "(newid(), '" + id + "', '8542', 'en', 'Cultural education, non-academic e.g. dance and music instruction'), " +
                                "(newid(), '" + id + "', '8543', 'en', 'Cross boundary education'), " +
                                "(newid(), '" + id + "', '8544', 'en', 'Education by techniques (TVET) and training (including short-term training)'), " +
                                "(newid(), '" + id + "', '8545', 'en', 'Education by correspondence'), " +
                                "(newid(), '" + id + "', '8549', 'en', 'Other education n.e.c.'), " +
                                "(newid(), '" + id + "', '8550', 'en', 'Educational support activities e.g. educational consulting'), " +
                                "(newid(), '" + id + "', '8610', 'en', 'Hospital activities'), " +
                                "(newid(), '" + id + "', '8620', 'en', 'Medical and dental practice activities'), " +
                                "(newid(), '" + id + "', '8690', 'en', 'Other human health activities'), " +
                                "(newid(), '" + id + "', '8710', 'en', 'Residential nursing care activities e.g. homes for elderly'), " +
                                "(newid(), '" + id + "', '8720', 'en', 'Residential nursing care activities for mental retardation, mental health and substance abuse excluding hospital activities'), " +
                                "(newid(), '" + id + "', '8730', 'en', 'Residential care activities for the elderly and the disabled'), " +
                                "(newid(), '" + id + "', '8790', 'en', 'Residential care activities n.e.c e.g. halfway homes, orphanages'), " +
                                "(newid(), '" + id + "', '8810', 'en', 'Social work activities without accommodation for the elderly and the disabled'), " +
                                "(newid(), '" + id + "', '8890', 'en', 'Other social work activities without accommodation'), " +
                                "(newid(), '" + id + "', '9000', 'en', 'Creative, arts and entertainment activities'), " +
                                "(newid(), '" + id + "', '9010', 'en', 'Wildlife related commercial activities'), " +
                                "(newid(), '" + id + "', '9090', 'en', 'Other entertainment activities n.e.c.(Event Organizer)'), " +
                                "(newid(), '" + id + "', '9101', 'en', 'Libraries and archives activities'), " +
                                "(newid(), '" + id + "', '9102', 'en', 'Museums activities and operation of historical sites and buildings'), " +
                                "(newid(), '" + id + "', '9103', 'en', 'Botanical and zoological gardens and nature reserves activities'), " +
                                "(newid(), '" + id + "', '9104', 'en', 'Topographic beauty'), " +
                                "(newid(), '" + id + "', '9200', 'en', 'Gambling and betting activities'), " +
                                "(newid(), '" + id + "', '9311', 'en', 'Operation of sports facilities'), " +
                                "(newid(), '" + id + "', '9312', 'en', 'Activities of sports clubs'), " +
                                "(newid(), '" + id + "', '9319', 'en', 'Other sports activities'), " +
                                "(newid(), '" + id + "', '9321', 'en', 'Activities of amusement parks and theme parks'), " +
                                "(newid(), '" + id + "', '9329', 'en', 'Other amusement and recreation activities n.e.c.'), " +
                                "(newid(), '" + id + "', '9411', 'en', 'Activities of business employers membership organisations'), " +
                                "(newid(), '" + id + "', '9412', 'en', 'Activities of professional membership organisations'), " +
                                "(newid(), '" + id + "', '9413', 'en', 'Activities of sports associations (Licensing on activities of sport association)'), " +
                                "(newid(), '" + id + "', '9420', 'en', 'Activities of trade unions'), " +
                                "(newid(), '" + id + "', '9491', 'en', 'Activities of religious organisations'), " +
                                "(newid(), '" + id + "', '9492', 'en', 'Activities of political organisations'), " +
                                "(newid(), '" + id + "', '9499', 'en', 'Activities of other membership organisations'), " +
                                "(newid(), '" + id + "', '9511', 'en', 'Repair of computers and peripheral equipment'), " +
                                "(newid(), '" + id + "', '9512', 'en', 'Repair of communication equipment'), " +
                                "(newid(), '" + id + "', '9521', 'en', 'Repair of consumer electronics e.g. TV, VCD, DVD'), " +
                                "(newid(), '" + id + "', '9522', 'en', 'Repair of household appliances, home and garden equipment'), " +
                                "(newid(), '" + id + "', '9523', 'en', 'Repair of footwear and leather goods'), " +
                                "(newid(), '" + id + "', '9524', 'en', 'Repair of furniture and home furnishings'), " +
                                "(newid(), '" + id + "', '9529', 'en', 'Repair of other personal and household goods'), " +
                                "(newid(), '" + id + "', '9601', 'en', 'Washing and (dry) cleaning of textile and fur products'), " +
                                "(newid(), '" + id + "', '9602', 'en', 'Hairdressing and other beauty treatment'), " +
                                "(newid(), '" + id + "', '9603', 'en', 'Funeral and related activities'), " +
                                "(newid(), '" + id + "', '9609', 'en', 'Other personal service activities n.e.c.'), " +
                                "(newid(), '" + id + "', '9700', 'en', 'Activities of households as employers of domestic personnel'), " +
                                "(newid(), '" + id + "', '9810', 'en', 'Undifferentiated goods-producing activities of private households for own use'), " +
                                "(newid(), '" + id + "', '9820', 'en', 'Undifferentiated service-producing activities of private households for own use'), " +
                                "(newid(), '" + id + "', '9900', 'en', 'Activities of extraterritorial organisations and bodies'), " +
                                "(newid(), '" + id + "', '9V00', 'en', 'Other activities not adequately defined') "));

            log.log(INFO, "Linking EnsicClass to EnsicGroup");
            String groupId;
            try (Result result = con.exec(p.parse("select _id from _platform.lookup.Lookup where name='EnsicGroup'"))) {
              result.next();
              groupId = result.get(1).value.toString();
            }
            con.exec(p.parse("insert into _platform.lookup.LookupValueLink(_id, name, source_value_id, target_value_id) " +
                                "select newid(), 'EnsicGroup', _id, " +
                                "       (select _id " +
                                "          from target:_platform.lookup.LookupValue  " +
                                "         where target.code=leftstr(lv.code, 3) " +
                                "           and target.lookup_id='" + groupId + "') " +
                                "from lv:_platform.lookup.LookupValue where lv.lookup_id='" + id + "'"));
          }
        }

        Program s = p.parse("create table LkS drop undefined(" +
                                "  {" +
                                "    name: 'LkS'," +
                                "    description: 'LkS test table'," +
                                "    tm1: (max(b) from LkS)," +
                                "    tm2: a > b" +
                                "  }, " +
                                "  _id uuid not null," +
                                "  a int {" +
                                "    m1: b > 5," +
                                "    m2: 10," +
                                "    m3: a != 0" +
                                "  }," +
                                "  b int {" +
                                "    m1: b < 0" +
                                "  }," +
                                "  c=a+b {" +
                                "    m1: a > 5," +
                                "    m2: a + b," +
                                "    m3: b > 5" +
                                "  }," +
                                "  d=b+c {" +
                                "    m1: 10" +
                                "  }," +
                                "  e bool {" +
                                "    m1: c" +
                                "  }," +
                                "  f=(max(a) from LkS) {" +
                                "    m1: (min(a) from LkS)" +
                                "  }," +
                                "  g=(distinct c from LkS where d>5) {" +
                                "    m1: (min(a) from a.b.LkT)" +
                                "  }," +
                                "  h text[] {" +
                                "    m1: 5" +
                                "  }," +
                                "  i string {" +
                                "    label: lookuplabel(i, 'EnsicClass')" +
                                "  }," +
                                "  j int[], " +
                                "  k interval, " +
                                "  l int, " +
                                "  primary key(_id)" +
                                ")");
        con.exec(s);

        s = p.parse("create table a.b.LkT drop undefined(" +
                    "  {" +
                    "    name: 'LkT'," +
                    "    description: 'LkT test table'," +
                    "    tm1: (max(b) from a.b.LkT)," +
                    "    tm2: a > b" +
                    "  }, " +
                    "  _id uuid not null," +
                    "  a int {" +
                    "    m1: b > 5," +
                    "    m2: 10," +
                    "    m3: a != 0" +
                    "  }," +
                    "  b int {" +
                    "    m1: b < 0" +
                    "  }," +
                    "  c=a+b {" +
                    "    m1: a > 5," +
                    "    m2: a + b," +
                    "    m3: b > 5" +
                    "  }," +
                    "  x int {" +
                    "    x1: b > 5," +
                    "    x2: a != 0" +
                    "  }," +
                    "  y int {" +
                    "    y1: b > 5," +
                    "    y2: a != 0" +
                    "  }," +
                    "  s_id uuid {" +
                    "    link_table: 'LkS', " +
                    "    link_table_code: '_id', " +
                    "    link_table_label: 'a' " +
                    "  }," +
                    "  foreign key(s_id) references LkS(_id)," +
                    "  primary key(_id)" +
                    ")");
        con.exec(s);

        s = p.parse("create table a.b.LkX drop undefined(" +
                    "  {" +
                    "    name: 'LkX'," +
                    "    description: 'LkX test table'," +
                    "    tm1: (max(b) from a.b.LkX)," +
                    "    tm2: a > b" +
                    "  }, " +
                    "  _id uuid not null," +
                    "  a int {" +
                    "    m1: b > 5," +
                    "    m2: 10," +
                    "    m3: a != 0" +
                    "  }," +
                    "  b int {" +
                    "    m1: b < 0" +
                    "  }," +
                    "  c=a+b {" +
                    "    m1: a > 5," +
                    "    m2: a + b," +
                    "    m3: b > 5" +
                    "  }," +
                    "  s_id uuid {" +
                    "    link_table: 'LkS', " +
                    "    link_table_code: '_id', " +
                    "    link_table_label: 'a' " +
                    "  }," +
                    "  t_id uuid {" +
                    "    link_table: 'a.b.LkT', " +
                    "    link_table_code: '_id', " +
                    "    link_table_label: 'b' " +
                    "  }," +
                    "  foreign key(s_id) references LkS(_id)," +
                    "  foreign key(t_id) references a.b.LkT(_id) on update cascade on delete cascade," +
                    "  primary key(_id)" +
                    ")");
        con.exec(s);

        s = p.parse("create table b.LkY drop undefined(" +
                    "  {" +
                    "    name: 'LkY'," +
                    "    description: 'LkY test table'," +
                    "    tm1: (max(b) from b.LkY)," +
                    "    tm2: a > b" +
                    "  }, " +
                    "  _id uuid not null," +
                    "  a int {" +
                    "    m1: b > 5," +
                    "    m2: 10," +
                    "    m3: a != 0" +
                    "  }," +
                    "  b int {" +
                    "    m1: b < 0" +
                    "  }," +
                    "  c=a+b {" +
                    "    m1: a > 5," +
                    "    m2: a + b," +
                    "    m3: b > 5" +
                    "  }," +
                    "  s_id uuid {" +
                    "    link_table: 'LkS', " +
                    "    link_table_code: '_id', " +
                    "    link_table_label: 'a' " +
                    "  }," +
                    "  t_id uuid {" +
                    "    link_table: 'a.b.LkT', " +
                    "    link_table_code: '_id', " +
                    "    link_table_label: 'b' " +
                    "  }," +
                    "  x_id uuid {" +
                    "    link_table: 'a.b.LkX', " +
                    "    link_table_code: '_id', " +
                    "    link_table_label: 'b' " +
                    "  }," +
                    "  foreign key(s_id) references LkS(_id)," +
                    "  foreign key(t_id) references a.b.LkT(_id) on update cascade on delete cascade," +
                    "  foreign key(x_id) references a.b.LkX(_id)," +
                    "  primary key(_id)" +
                    ")");
        con.exec(s);
      }
    }
  }

  public static void printResult(Result rs, int columnWidth) {
    boolean first = true;
    while(rs.next()) {
      if (first) {
        System.out.println('+' + repeat(repeat('-', columnWidth) + '+', rs.columns()));
        System.out.print('|');
        for (int i = 0; i < rs.columns(); i++) {
          System.out.print(rightPad(rs.column(i + 1).alias(), columnWidth) + '|');
        }
        System.out.println();
        System.out.println('+' + repeat(repeat('-', columnWidth) + '+', rs.columns()));
        first = false;
      }
      System.out.print('|');
      for (int i = 0; i < rs.columns(); i++) {
        Object value = rs.value(i + 1);
        if (value == null) {
          System.out.print(repeat(' ', columnWidth) + '|');
        } else if (value instanceof Number) {
          System.out.print(leftPad(value.toString(), columnWidth) + '|');
        } else {
          System.out.print(rightPad(value.toString(), columnWidth) + '|');
        }
      }
      System.out.println();
    }
    if (!first) {
      System.out.println('+' + repeat(repeat('-', columnWidth) + '+', rs.columns()));
    }
  }

  private static final System.Logger log = System.getLogger(DataTest.class.getName());
}
