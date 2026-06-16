/**************************************************************************
    Package: qbl.MQ_PKG
    Author: Blaine Rudow
    Last Edit: 2026-06-15
    Description: This is a helper package for ingesting Master Data Management
                 records in batch and incrementally from MuleSoft or another source.
    Helpful Links:
    https://docs.oracle.com/en/database/oracle/oracle-database/18/sqlrf/constraint.html
    https://docs.oracle.com/en/database/oracle/oracle-database/19/jjdbc/Oracle-object-types.html
**************************************************************************/
create or replace package body qbl.mq_pkg as

    /**************************************************************************
        Global Constant Values
    **************************************************************************/
   g_schema       constant varchar2(20 char) := 'qbl';
   g_package_name constant varchar2(20 char) := 'MQ_PKG';

    /* MuleSoft Uses g_package_user to determine if they need to retrieve an updated
       record while polling ERM tables. "MDM" is their Sentinel value */
   g_package_user constant varchar2(10 char) := 'MDM';

    /* mq_status uses HTTP codes directly (FK to qbl.http_codes):
       202 staged, 200 changed, 304 no change, 404 pid not in source, 500 error. */

    /**************************************************************************
        Shared compare cursor + batch type.

        One definition of the inbound -> qbl.mq_vw join, reused by the single
        path (scoped to one mq_id) and the row-engine batch path. Assumes
        qbl.mq_vw yields one row per pid (the phone leg is pinned to type 1).
    **************************************************************************/
   cursor c_mqi (
      p_status  in number,
      p_from_id in number,
      p_to_id   in number
   ) is
   select mqi.*,
          v.pid             as src_pid,
          v.mq_demogr_hash  as src_demo_hash,
          v.mq_address_hash as src_address_hash,
          v.mq_phone_hash   as src_phone_hash
     from qbl.mq_inbound mqi
     left join qbl.mq_vw v
   on v.pid = mqi.pid
    where mqi.mq_status = p_status
      and mqi.mq_id >= p_from_id
      and mqi.mq_id <= nvl(p_to_id, mqi.mq_id)
    order by mqi.mq_id asc;

   type t_mqi_table is
      table of c_mqi%rowtype;

    /**************************************************************************
        Package Logging Procedure: qbl.MQ_PKG.MQ_LOGGER
    **************************************************************************/
   procedure mq_logger (
      i_mq_id                 in varchar2 default null,
      i_json_record           in clob default null,
      i_error_code            in number default null,
      i_error_message         in varchar2 default null,
      i_error_location        in varchar2 default null,
      i_mq_status_code        in varchar2 default null,
      i_procedure_name        in varchar2 default null,
      i_procedure_description in varchar2 default null,
      i_package_name          in varchar2 := g_package_name
   ) as
      pragma autonomous_transaction;
   begin
      insert into mq_pkg_log (
         mq_id,
         json_record,
         error_code,
         error_message,
         error_location,
         mq_status_code,
         procedure_name,
         procedure_description,
         package_name
      ) values ( i_mq_id,
                 i_json_record,
                 i_error_code,
                 i_error_message,
                 i_error_location,
                 i_mq_status_code,
                 i_procedure_name,
                 i_procedure_description,
                 i_package_name );
      commit;
   end mq_logger;

    /**************************************************************************
        Procedure: census_sentinel
    **************************************************************************/
   procedure census_sentinel (
      i_pen_id      in varchar2,
      i_first_nm    in varchar2,
      i_middle_intl in varchar2,
      i_last_nm     in varchar2,
      i_birth_dt    in date,
      i_death_dt    in date,
      i_addr1       in varchar2,
      i_addr2       in varchar2,
      i_addr3       in varchar2,
      i_city        in varchar2,
      i_state       in varchar2,
      i_country     in varchar2,
      i_postal      in varchar2,
      i_phn         in varchar2,
      i_email       in varchar2,
      i_marital_st  in varchar2,
      i_mbr_gender  in varchar2
   ) as
      v_procedure_name        varchar2(32 char) := 'CENSUS_SENTINEL';
      v_procedure_description varchar2(480 char) := 'Syncs the staged census row to incoming MDM values so the nightly MINUS delta does not re-flag the member.';
   begin
      update qbl.ps_csm_voya_census
         set csm_first_nm = i_first_nm,
             csm_middle_intl = i_middle_intl,
             csm_last_nm = i_last_nm,
             csm_birth_dt = i_birth_dt,
             csm_death_dt = i_death_dt,
             csm_addr1 = i_addr1,
             csm_addr2 = i_addr2,
             csm_addr3 = i_addr3,
             csm_city = i_city,
             csm_state = i_state,
             csm_postal = i_postal,
             csm_country = i_country,
             csm_phn = i_phn,
             csm_email = i_email,
             csm_marital_st = i_marital_st,
             csm_mbr_gender = i_mbr_gender,
             updated_at = systimestamp,
             updated_by = g_package_user
       where csm_pen_id = i_pen_id;
   exception
      when others then
         mq_logger(
            i_mq_id                 => null,
            i_json_record           => null,
            i_error_code            => sqlcode,
            i_error_message         => dbms_utility.format_error_stack,
            i_error_location        => dbms_utility.format_error_backtrace,
            i_mq_status_code        => 500,
            i_procedure_name        => v_procedure_name,
            i_procedure_description => v_procedure_description || ' pen_id=' || i_pen_id,
            i_package_name          => g_package_name
         );
         raise;
   end census_sentinel;

    /**************************************************************************
        Function: hydrate_mq_hash

        Computes the inbound-side hashes. These MUST match qbl.mq_vw exactly
        (same columns, order, and chr() defaults) or every record looks changed.
    **************************************************************************/
   function hydrate_mq_hash (
      mqi_record in out qbl.mq_inbound%rowtype
   ) return qbl.mq_inbound%rowtype as
   begin
      mqi_record.mq_demogr_hash := rawtohex(standard_hash(
         nvl(mqi_record.first_name, chr(124))
         || nvl(mqi_record.middle_name, chr(124))
         || nvl(mqi_record.last_name, chr(124))
         || nvl(to_char(mqi_record.name_suffix_id), chr(124))
         || nvl(to_char(mqi_record.birthdate, 'YYYY-MM-DD'), chr(124))
         || nvl(to_char(mqi_record.dt_of_death, 'YYYY-MM-DD'), chr(124))
         || nvl(mqi_record.email_addr, chr(124))
         || nvl(to_char(mqi_record.gender_id), chr(124))
         || nvl(to_char(mqi_record.mar_status_id), chr(124)),
         'MD5'
      ));

      mqi_record.mq_address_hash := rawtohex(standard_hash(
         nvl(mqi_record.address1, chr(124))
         || nvl(mqi_record.address2, chr(124))
         || nvl(mqi_record.address3, chr(124))
         || nvl(mqi_record.address4, chr(124))
         || nvl(mqi_record.city, chr(124))
         || nvl(mqi_record.county, chr(124))
         || nvl(mqi_record.state, chr(124))
         || nvl(mqi_record.postal, chr(124))
         || nvl(mqi_record.country, chr(124)),
         'MD5'
      ));

      mqi_record.mq_phone_hash := rawtohex(standard_hash(
         nvl(mqi_record.phone, chr(124))
         || nvl(to_char(mqi_record.phone_type_id), chr(0)),
         'MD5'
      ));

      return mqi_record;
   end hydrate_mq_hash;

    /**************************************************************************
        Function: mqi_record_constructor
    **************************************************************************/
   function mqi_record_constructor (
      i_mq_id             in number default null,
      i_pid               in varchar2,
      i_csm_mbr_demogr_id in varchar2 default null,
        /* DEMO */
      i_first_name        in varchar2 default null,
      i_middle_name       in varchar2 default null,
      i_last_name         in varchar2 default null,
      i_name_suffix_id    in number default null,
      i_birthdate         in date default null, --iso 8601 format
      i_dt_of_death       in date default null,
      i_email_addr        in varchar2 default null,
      i_gender_id         in number default null,
      i_mar_status_id     in number default null,
      i_mq_demogr_hash    in varchar2 default null,
        /* PHONE */
      i_csm_mbr_phn_id    in number default null,
      i_phone             in varchar2 default null,
      i_phone_type_id     in number default null,
      i_mq_phone_hash     in varchar2 default null,
        /* ADDRESS */
      i_csm_mbr_addr_id   in number default null,
      i_address1          in varchar2 default null,
      i_address2          in varchar2 default null,
      i_address3          in varchar2 default null,
      i_address4          in varchar2 default null,
      i_city              in varchar2 default null,
      i_county            in varchar2 default null,
      i_state             in varchar2 default null,
      i_postal            in varchar2 default null,
      i_country           in varchar2 default null,
      i_mq_address_hash   in varchar2 default null,
      i_mq_status         in number default 202,
      i_updated_at        in timestamp default null,
      i_updated_by        in varchar2 default g_package_user,
      i_created_at        in timestamp default null,
      i_created_by        in varchar2 default g_package_user
   ) return qbl.mq_inbound%rowtype as
      mqi_record qbl.mq_inbound%rowtype;
   begin
        /* IDs*/
      mqi_record.mq_id := i_mq_id;
      mqi_record.pid := i_pid;
      mqi_record.csm_mbr_demogr_id := i_csm_mbr_demogr_id;
        /* NAME */
      mqi_record.first_name := i_first_name;
      mqi_record.middle_name := i_middle_name;
      mqi_record.last_name := i_last_name;
      mqi_record.name_suffix_id := i_name_suffix_id;
        /* DEMO */
      mqi_record.birthdate := i_birthdate;
      mqi_record.dt_of_death := i_dt_of_death;
      mqi_record.email_addr := i_email_addr;
      mqi_record.gender_id := i_gender_id;
      mqi_record.mar_status_id := i_mar_status_id;
      mqi_record.mq_demogr_hash := i_mq_demogr_hash;
        /* PHONE */
      mqi_record.csm_mbr_phn_id := i_csm_mbr_phn_id;
      mqi_record.phone := i_phone;
      mqi_record.phone_type_id := i_phone_type_id;
      mqi_record.mq_phone_hash := i_mq_phone_hash;
        /* ADDRESS */
      mqi_record.csm_mbr_addr_id := i_csm_mbr_addr_id;
      mqi_record.address1 := i_address1;
      mqi_record.address2 := i_address2;
      mqi_record.address3 := i_address3;
      mqi_record.address4 := i_address4;
      mqi_record.city := i_city;
      mqi_record.county := i_county;
      mqi_record.state := i_state;
      mqi_record.postal := i_postal;
      mqi_record.country := i_country;
      mqi_record.mq_address_hash := i_mq_address_hash;
      mqi_record.mq_status := i_mq_status;
      mqi_record.updated_at := i_updated_at;
      mqi_record.updated_by := i_updated_by;
      mqi_record.created_at := i_created_at;
      mqi_record.created_by := i_created_by;
      return mqi_record;
   end mqi_record_constructor;

    /**************************************************************************
        Function: mqi_to_json
    **************************************************************************/
   function mqi_to_json (
      mqi_record in qbl.mq_inbound%rowtype
   ) return clob as
      v_procedure_name        varchar2(30) := 'MQI_TO_JSON';
      v_procedure_description varchar2(200) := 'A Function that accepts a MQ_INBOUND record and serializes it to JSON format returned as a clob.';
      o_json                  clob;
   begin
      o_json :=
         json_object(
            'mq_id' value mqi_record.mq_id,
            'csm_mbr_demogr_id' value mqi_record.csm_mbr_demogr_id,
            'pid' value mqi_record.pid,
            /* DEMO */
            'first_name' value mqi_record.first_name,
            'middle_name' value mqi_record.middle_name,
            'last_name' value mqi_record.last_name,
            'name_suffix_id' value mqi_record.name_suffix_id,
            'birthdate' value mqi_record.birthdate,
            'dt_of_death' value mqi_record.dt_of_death,
            'email_addr' value mqi_record.email_addr,
            'gender_id' value mqi_record.gender_id,
            'mar_status_id' value mqi_record.mar_status_id,
            'mq_demogr_hash' value mqi_record.mq_demogr_hash,
            /* PHONE */
            'csm_mbr_phn_id' value mqi_record.csm_mbr_phn_id,
            'phone' value mqi_record.phone,
            'phone_type_id' value mqi_record.phone_type_id,
            'mq_phone_hash' value mqi_record.mq_phone_hash,
            /* ADDRESS */
            'csm_mbr_addr_id' value mqi_record.csm_mbr_addr_id,
            'address1' value mqi_record.address1,
            'address2' value mqi_record.address2,
            'address3' value mqi_record.address3,
            'address4' value mqi_record.address4,
            'city' value mqi_record.city,
            'county' value mqi_record.county,
            'state' value mqi_record.state,
            'postal' value mqi_record.postal,
            'country' value mqi_record.country,
            'mq_address_hash' value mqi_record.mq_address_hash,
            'mq_status' value mqi_record.mq_status,
            /* AUDIT */
            'updated_at' value mqi_record.updated_at,
            'updated_by' value mqi_record.updated_by,
            'created_at' value mqi_record.created_at,
            'created_by' value mqi_record.created_by
         returning clob);
      -- JSON_SERIALIZE cannot be called from a PL/SQL expression on 19c
      -- (PL/SQL support was added in 21c); evaluate it in SQL instead.
      select json_serialize(o_json returning clob pretty)
        into o_json
        from dual;
      return o_json;
   exception
      when others then
         mq_logger(
            i_mq_id                 => mqi_record.mq_id,
            i_json_record           => o_json,
            i_error_code            => sqlcode,
            i_error_message         => dbms_utility.format_error_stack,
            i_error_location        => dbms_utility.format_error_backtrace,
            i_mq_status_code        => 500,
            i_procedure_name        => v_procedure_name,
            i_procedure_description => v_procedure_description,
            i_package_name          => g_package_name
         );
         raise;
   end mqi_to_json;

    /**************************************************************************
        Function: process_record

        The single source of truth for per-record business logic. Compares the
        inbound row to current ERM state and applies demographic / address /
        phone changes. Returns the resulting mq_status. Does NOT update
        mq_inbound, commit, or own a savepoint -- the caller does, so this can
        serve both the single and batch drivers.
    **************************************************************************/
   function process_record (
      p_row       in c_mqi%rowtype,
      p_bulk_mode in boolean
   ) return number as
      v_demo_changed  boolean := false;
      v_addr_changed  boolean := false;
      v_phone_changed boolean := false;
   begin
      -- pid not present in source -> nothing to apply
      if p_row.src_pid is null then
         return 404; -- pid not in source
      end if;

      -- Demographic hash check and audit handling.
      if nvl(p_row.mq_demogr_hash, '#NULL#') != nvl(p_row.src_demo_hash, '#NULL#') then
         v_demo_changed := true;
         update qbl.ps_csm_mbr_demogr d
            set d.csm_first_nm = p_row.first_name,
                d.csm_middle_nm = p_row.middle_name,
                d.csm_last_nm = p_row.last_name,
                d.csm_nm_suf_id = p_row.name_suffix_id,
                d.csm_birth_dt = p_row.birthdate,
                d.csm_death_dt = p_row.dt_of_death,
                d.csm_email = p_row.email_addr,
                d.csm_sex = p_row.gender_id,
                d.csm_maritial_st = p_row.mar_status_id,
                d.update_date = systimestamp,
                d.updated_by = g_package_user
          where d.csm_mbr_demogr_id = p_row.csm_mbr_demogr_id;
      end if;

      /* Address Section */
      if nvl(p_row.mq_address_hash, '#NULL#') != nvl(p_row.src_address_hash, '#NULL#') then
         v_addr_changed := true;
         update qbl.ps_csm_mbr_addr addr
            set addr.csm_addr1 = p_row.address1,
                addr.csm_addr2 = p_row.address2,
                addr.csm_addr3 = p_row.address3,
                addr.csm_addr4 = p_row.address4,
                addr.csm_city = p_row.city,
                addr.csm_county = p_row.county,
                addr.csm_state = p_row.state,
                addr.csm_postal = p_row.postal,
                addr.csm_country = p_row.country,
                addr.update_date = systimestamp,
                addr.updated_by = g_package_user
          where addr.csm_mbr_demogr_id = p_row.csm_mbr_demogr_id
            and addr.csm_mbr_addr_id = p_row.csm_mbr_addr_id;
      end if;

      /* Phone Section */
      if nvl(p_row.mq_phone_hash, '#NULL#') != nvl(p_row.src_phone_hash, '#NULL#') then
         v_phone_changed := true;
         update qbl.ps_csm_mbr_phn phn
            set phn.csm_phn_type_id = p_row.phone_type_id,
                phn.csm_phn = p_row.phone,
                phn.update_date = systimestamp,
                phn.updated_by = g_package_user
          where phn.csm_mbr_demogr_id = p_row.csm_mbr_demogr_id
            and phn.csm_mbr_phn_id = p_row.csm_mbr_phn_id;
      end if;

      if v_demo_changed or v_addr_changed or v_phone_changed then
         if p_bulk_mode then
            census_sentinel(
               i_pen_id      => p_row.pid,
               i_first_nm    => p_row.first_name,
               i_middle_intl => substr(p_row.middle_name, 1, 1),
               i_last_nm     => p_row.last_name,
               i_birth_dt    => p_row.birthdate,
               i_death_dt    => p_row.dt_of_death,
               i_addr1       => p_row.address1,
               i_addr2       => p_row.address2,
               i_addr3       => p_row.address3,
               i_city        => p_row.city,
               i_state       => p_row.state,
               i_country     => p_row.country,
               i_postal      => p_row.postal,
               i_phn         => p_row.phone,
               i_email       => p_row.email_addr,
               i_marital_st  => p_row.mar_status_id,
               i_mbr_gender  => p_row.gender_id
            );
         end if;
         return 200; -- change applied
      else
         return 304; -- no change
      end if;
   end process_record;

    /**************************************************************************
        Function: mq_loader

        Stages a single record into qbl.mq_inbound. No commit -- the caller
        owns the transaction boundary.
    **************************************************************************/
   function mq_loader (
      mqi_record in qbl.mq_inbound%rowtype
   ) return qbl.mq_inbound%rowtype as
      v_rec qbl.mq_inbound%rowtype := mqi_record;
   begin
      if v_rec.mq_id is not null then
         update qbl.mq_inbound
            set row = v_rec
          where mq_id = v_rec.mq_id
         returning mq_id into v_rec.mq_id;
      else
         insert into qbl.mq_inbound values v_rec
         returning mq_id into v_rec.mq_id;
      end if;
      return v_rec;
   end mq_loader;

    /**************************************************************************
        Procedure: mq_insert

        Public single-record API for the integration team. Stages the inbound
        record, processes it against ERM, returns 0 on success (status 200/304)
        and raises (response := 1) on failure (404/500). Owns one transaction
        end to end -- no nested commits, no savepoint-after-commit.
    **************************************************************************/
   procedure mq_insert (
      pid         in varchar2,
      first_name  in varchar2 default null,
      middle_name in varchar2 default null,
      last_name   in varchar2 default null,
      name_suffix in number default null, -- 1 through 13 as of 5/14/2026
      birthdate   in date default null, --iso 8601 format 'YYYY-MM-DD'
      dt_of_death in date default null,
      email_addr  in varchar2 default null,
      sex         in number default null, --20, 21
      mar_status  in number default null, --23, 24, 25
      phone       in varchar2 default null,
      phone_type  in number default null, --defaults to 1 if phone is not null
      address1    in varchar2 default null,
      address2    in varchar2 default null,
      address3    in varchar2 default null,
      address4    in varchar2 default null,
      city        in varchar2 default null,
      county      in varchar2 default null, --open question
      state       in varchar2 default null,
      postal      in varchar2 default null,
      country     in varchar2 default null,
      response    out number --preference for 0=ok 1=fail
   ) as
      v_procedure_name varchar2(32 char) := 'MQ_INSERT';
      v_mqi_record     qbl.mq_inbound%rowtype;
      v_row            c_mqi%rowtype;
      v_status         number;
      v_id             number;
   begin
      v_mqi_record := mqi_record_constructor(
         i_pid            => pid,
            /* DEMO */
         i_first_name     => first_name,
         i_middle_name    => middle_name,
         i_last_name      => last_name,
         i_name_suffix_id => name_suffix,
         i_birthdate      => birthdate,
         i_dt_of_death    => dt_of_death,
         i_email_addr     => email_addr,
         i_gender_id      => sex,
         i_mar_status_id  => mar_status,
            /* PHONE */
         i_phone          => phone,
         i_phone_type_id  => phone_type,
            /* ADDRESS */
         i_address1       => address1,
         i_address2       => address2,
         i_address3       => address3,
         i_address4       => address4,
         i_city           => city,
         i_county         => county,
         i_state          => state,
         i_postal         => postal,
         i_country        => country
      );
      v_mqi_record := hydrate_mq_hash(v_mqi_record);
      v_mqi_record := mq_loader(v_mqi_record);
      v_id := v_mqi_record.mq_id;

      -- Pull the staged row joined to current ERM state (scoped to this id only).
      open c_mqi(202, v_id, v_id);
      fetch c_mqi into v_row;
      if c_mqi%notfound then
         close c_mqi;
         raise_application_error(
            -20003,
            'Staged MQ_ID=' || v_id || ' not found for processing'
         );
      end if;
      close c_mqi;

      -- Real-time integration updates flow to the census normally (bulk_mode => false).
      v_status := process_record(v_row, p_bulk_mode => false);

      update qbl.mq_inbound
         set mq_status = v_status,
             updated_at = systimestamp,
             updated_by = g_package_user
       where mq_id = v_id;
      commit;

      -- 200 ingested, 304 unchanged, 404 pid not in source (rejected, but the
      -- row stays staged in mq_inbound for the audit trail). All three are
      -- acceptable outcomes -> 0. MuleSoft fans out every member; a not-found
      -- pid is an expected rejection, not a failure. A genuine processing
      -- error surfaces as an exception below, never as a status value.
      response := 0;
   exception
      when others then
         rollback;
         response := 1;
         mq_logger(
            i_mq_id                 => v_id,
            i_json_record           => null,
            i_error_code            => sqlcode,
            i_error_message         => dbms_utility.format_error_stack,
            i_error_location        => dbms_utility.format_error_backtrace,
            i_mq_status_code        => 500,
            i_procedure_name        => v_procedure_name,
            i_procedure_description => 'Single-record integration API',
            i_package_name          => g_package_name
         );
         raise;
   end mq_insert;

    /*******************************************************
        process_window_setbased (private)

        Applies one mq_id window set-based: MERGE the demographic, address and
        phone changes (each gated by a hash diff), optionally sync the census,
        then finalize mq_status for every row in the window. No commit and no
        error handling -- the caller owns the transaction and the row fallback.
    *******************************************************/
   procedure process_window_setbased (
      i_status    in number,
      v_low       in number,
      v_high      in number,
      i_bulk_mode in number
   ) as
   begin
            -- Demographics: apply where the demographic hash differs.
            merge into qbl.ps_csm_mbr_demogr d
            using (
               select s.csm_mbr_demogr_id,
                      s.first_name, s.middle_name, s.last_name, s.name_suffix_id,
                      s.birthdate, s.dt_of_death, s.email_addr, s.gender_id, s.mar_status_id
                 from ( select i.csm_mbr_demogr_id,
                               i.first_name, i.middle_name, i.last_name, i.name_suffix_id,
                               i.birthdate, i.dt_of_death, i.email_addr, i.gender_id, i.mar_status_id,
                               row_number() over (
                                  partition by i.csm_mbr_demogr_id order by i.mq_id desc
                               ) rn
                          from qbl.mq_inbound i
                          join qbl.mq_vw v on v.pid = i.pid
                         where i.mq_status = i_status
                           and i.mq_id between v_low and v_high
                           and i.csm_mbr_demogr_id is not null
                           and nvl(i.mq_demogr_hash, '#NULL#') != nvl(v.mq_demogr_hash, '#NULL#')
                      ) s
                where s.rn = 1
            ) src
            on ( d.csm_mbr_demogr_id = src.csm_mbr_demogr_id )
            when matched then update
               set d.csm_first_nm = src.first_name,
                   d.csm_middle_nm = src.middle_name,
                   d.csm_last_nm = src.last_name,
                   d.csm_nm_suf_id = src.name_suffix_id,
                   d.csm_birth_dt = src.birthdate,
                   d.csm_death_dt = src.dt_of_death,
                   d.csm_email = src.email_addr,
                   d.csm_sex = src.gender_id,
                   d.csm_maritial_st = src.mar_status_id,
                   d.update_date = systimestamp,
                   d.updated_by = g_package_user;

            -- Address: apply where the address hash differs.
            merge into qbl.ps_csm_mbr_addr a
            using (
               select s.csm_mbr_demogr_id, s.csm_mbr_addr_id,
                      s.address1, s.address2, s.address3, s.address4,
                      s.city, s.county, s.state, s.postal, s.country
                 from ( select i.csm_mbr_demogr_id, i.csm_mbr_addr_id,
                               i.address1, i.address2, i.address3, i.address4,
                               i.city, i.county, i.state, i.postal, i.country,
                               row_number() over (
                                  partition by i.csm_mbr_demogr_id, i.csm_mbr_addr_id order by i.mq_id desc
                               ) rn
                          from qbl.mq_inbound i
                          join qbl.mq_vw v on v.pid = i.pid
                         where i.mq_status = i_status
                           and i.mq_id between v_low and v_high
                           and i.csm_mbr_addr_id is not null
                           and nvl(i.mq_address_hash, '#NULL#') != nvl(v.mq_address_hash, '#NULL#')
                      ) s
                where s.rn = 1
            ) src
            on ( a.csm_mbr_demogr_id = src.csm_mbr_demogr_id
             and a.csm_mbr_addr_id = src.csm_mbr_addr_id )
            when matched then update
               set a.csm_addr1 = src.address1,
                   a.csm_addr2 = src.address2,
                   a.csm_addr3 = src.address3,
                   a.csm_addr4 = src.address4,
                   a.csm_city = src.city,
                   a.csm_county = src.county,
                   a.csm_state = src.state,
                   a.csm_postal = src.postal,
                   a.csm_country = src.country,
                   a.update_date = systimestamp,
                   a.updated_by = g_package_user;

            -- Phone: apply where the phone hash differs.
            merge into qbl.ps_csm_mbr_phn p
            using (
               select s.csm_mbr_demogr_id, s.csm_mbr_phn_id, s.phone, s.phone_type_id
                 from ( select i.csm_mbr_demogr_id, i.csm_mbr_phn_id, i.phone, i.phone_type_id,
                               row_number() over (
                                  partition by i.csm_mbr_demogr_id, i.csm_mbr_phn_id order by i.mq_id desc
                               ) rn
                          from qbl.mq_inbound i
                          join qbl.mq_vw v on v.pid = i.pid
                         where i.mq_status = i_status
                           and i.mq_id between v_low and v_high
                           and i.csm_mbr_phn_id is not null
                           and nvl(i.mq_phone_hash, '#NULL#') != nvl(v.mq_phone_hash, '#NULL#')
                      ) s
                where s.rn = 1
            ) src
            on ( p.csm_mbr_demogr_id = src.csm_mbr_demogr_id
             and p.csm_mbr_phn_id = src.csm_mbr_phn_id )
            when matched then update
               set p.csm_phn_type_id = src.phone_type_id,
                   p.csm_phn = src.phone,
                   p.update_date = systimestamp,
                   p.updated_by = g_package_user;

            -- Census sentinel: sync the staged census for members that changed.
            if i_bulk_mode = 1 then
               merge into qbl.ps_csm_voya_census c
               using (
                  select s.pid, s.first_name, s.middle_name, s.last_name,
                         s.birthdate, s.dt_of_death, s.address1, s.address2, s.address3,
                         s.city, s.state, s.postal, s.country, s.phone, s.email_addr,
                         s.mar_status_id, s.gender_id
                    from ( select i.pid, i.first_name, i.middle_name, i.last_name,
                                  i.birthdate, i.dt_of_death, i.address1, i.address2, i.address3,
                                  i.city, i.state, i.postal, i.country, i.phone, i.email_addr,
                                  i.mar_status_id, i.gender_id,
                                  row_number() over (
                                     partition by i.pid order by i.mq_id desc
                                  ) rn
                             from qbl.mq_inbound i
                             join qbl.mq_vw v on v.pid = i.pid
                            where i.mq_status = i_status
                              and i.mq_id between v_low and v_high
                              and ( nvl(i.mq_demogr_hash, '#NULL#') != nvl(v.mq_demogr_hash, '#NULL#')
                                 or nvl(i.mq_address_hash, '#NULL#') != nvl(v.mq_address_hash, '#NULL#')
                                 or nvl(i.mq_phone_hash, '#NULL#') != nvl(v.mq_phone_hash, '#NULL#') )
                         ) s
                   where s.rn = 1
               ) src
               on ( c.csm_pen_id = src.pid )
               when matched then update
                  set c.csm_first_nm = src.first_name,
                      c.csm_middle_intl = substr(src.middle_name, 1, 1),
                      c.csm_last_nm = src.last_name,
                      c.csm_birth_dt = src.birthdate,
                      c.csm_death_dt = src.dt_of_death,
                      c.csm_addr1 = src.address1,
                      c.csm_addr2 = src.address2,
                      c.csm_addr3 = src.address3,
                      c.csm_city = src.city,
                      c.csm_state = src.state,
                      c.csm_postal = src.postal,
                      c.csm_country = src.country,
                      c.csm_phn = src.phone,
                      c.csm_email = src.email_addr,
                      c.csm_marital_st = src.mar_status_id,
                      c.csm_mbr_gender = src.gender_id,
                      c.updated_at = systimestamp,
                      c.updated_by = g_package_user;
            end if;

            -- Finalize status for every staged row in the window.
            merge into qbl.mq_inbound mqi
            using (
               select i.mq_id,
                      case
                         when v.pid is null then 404
                         when nvl(i.mq_demogr_hash, '#NULL#') != nvl(v.mq_demogr_hash, '#NULL#')
                           or nvl(i.mq_address_hash, '#NULL#') != nvl(v.mq_address_hash, '#NULL#')
                           or nvl(i.mq_phone_hash, '#NULL#') != nvl(v.mq_phone_hash, '#NULL#')
                         then 200
                         else 304
                      end as new_status
                 from qbl.mq_inbound i
                 left join qbl.mq_vw v on v.pid = i.pid
                where i.mq_status = i_status
                  and i.mq_id between v_low and v_high
            ) src
            on ( mqi.mq_id = src.mq_id )
            when matched then update
               set mqi.mq_status = src.new_status,
                   mqi.updated_at = systimestamp,
                   mqi.updated_by = g_package_user;

   end process_window_setbased;

    /*******************************************************
        process_window_rowbyrow (private)

        Fallback from batch to row-by-row.
    *******************************************************/
   procedure process_window_rowbyrow (
      i_status         in number,
      v_low            in number,
      v_high           in number,
      i_bulk_mode      in number,
      i_raise_on_error in number
   ) as
      v_batch          t_mqi_table;
      v_status         number;
      v_procedure_name varchar2(100 char) := 'process_mq_inbound (row fallback)';
   begin
      open c_mqi(i_status, v_low, v_high);
      loop
         fetch c_mqi bulk collect into v_batch limit 2000;
         exit when v_batch.count = 0;
         for i in 1..v_batch.count loop
            begin
               savepoint start_of_row;
               v_status := process_record(v_batch(i), p_bulk_mode => ( i_bulk_mode = 1 ));
               update qbl.mq_inbound mqi
                  set mqi.mq_status = v_status,
                      updated_at = systimestamp,
                      updated_by = g_package_user
                where mqi.mq_id = v_batch(i).mq_id;
            exception
               when others then
                  rollback to start_of_row;
                  update qbl.mq_inbound mqi
                     set mqi.mq_status = 500,
                         updated_at = systimestamp,
                         updated_by = g_package_user
                   where mqi.mq_id = v_batch(i).mq_id;
                  mq_logger(
                     i_mq_id                 => v_batch(i).mq_id,
                     i_json_record           => null,
                     i_error_code            => sqlcode,
                     i_error_message         => dbms_utility.format_error_stack,
                     i_error_location        => dbms_utility.format_error_backtrace,
                     i_mq_status_code        => 500,
                     i_procedure_name        => v_procedure_name,
                     i_procedure_description => 'Row-engine processing error',
                     i_package_name          => g_package_name
                  );
                  if i_raise_on_error = 1 then
                     raise;
                  end if;
            end;
         end loop;
         commit;
      end loop;
      close c_mqi;
   exception
      when others then
         if c_mqi%isopen then
            close c_mqi;
         end if;
         raise;
   end process_window_rowbyrow;

    /*******************************************************
        qbl.MQ_PKG.PROCESS_MQ_INBOUND

        Public bulk entrypoint. Processes staged rows in mq_id windows: each
        window is attempted set-based (fast) and committed; if a window throws,
        it is rolled back and re-run row-by-row so the offending row is isolated
        as 500 and the rest of the window still completes.
    *******************************************************/
   procedure process_mq_inbound (
      i_starting_mq_id in number := 0,
      i_chunk_size     in number := 0,
      i_status         in number := 202,
      i_bulk_mode      in number := 0,
      i_raise_on_error in number := 0
   ) as
      v_window pls_integer := nvl(nullif(i_chunk_size, 0), 50000);
      v_low    number := nvl(i_starting_mq_id, 0);
      v_high   number;
      v_max    number;
   begin
      select max(mq_id)
        into v_max
        from qbl.mq_inbound
       where mq_status = i_status
         and mq_id >= v_low;

      if v_max is null then
         return;
      end if;

      while v_low <= v_max loop
         v_high := v_low + v_window - 1;
         begin
            -- Fast path: apply the whole window set-based, then commit it.
            process_window_setbased(i_status, v_low, v_high, i_bulk_mode);
            commit;
         exception
            when others then
               -- re-run the window row-by-row to isolate the error records.
               rollback;
               mq_logger(
                  i_mq_id                 => null,
                  i_json_record           => null,
                  i_error_code            => sqlcode,
                  i_error_message         => dbms_utility.format_error_stack,
                  i_error_location        => dbms_utility.format_error_backtrace,
                  i_mq_status_code        => 500,
                  i_procedure_name        => 'process_mq_inbound',
                  i_procedure_description => 'Set-based window ' || v_low || '-' || v_high || ' failed; falling back row-by-row',
                  i_package_name          => g_package_name
               );
               process_window_rowbyrow(i_status, v_low, v_high, i_bulk_mode, i_raise_on_error);
         end;
         v_low := v_high + 1;
      end loop;
   end process_mq_inbound;

    /**************************************************************************
        Package Initialization Block:
    **************************************************************************/
end mq_pkg;
/
