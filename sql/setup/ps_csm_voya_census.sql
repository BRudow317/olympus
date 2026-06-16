/**************************************************************************
    Table: qbl.ps_csm_voya_census
    Staged census snapshot. census_sentinel / the bulk census merge sync
    this to incoming MDM values so the nightly MINUS delta does not
    re-flag the member. Keyed by csm_pen_id (= mq_vw.pid).

    Note: this table uses csm_marital_st (single 'i') and csm_mbr_gender.
    The real table carries no updated_at/updated_by columns, so the package
    does not write audit fields on the census. Holds only addr1-3 (no addr4),
    matching the sentinel column set.
**************************************************************************/
create table qbl.ps_csm_voya_census (
   csm_pen_id      varchar2(12 char) not null
      constraint pk_csm_voya_census primary key,
   csm_first_nm    varchar2(64 char),
   csm_middle_intl varchar2(1 char),
   csm_last_nm     varchar2(64 char),
   csm_birth_dt    date,
   csm_death_dt    date,
   csm_addr1       varchar2(64 char),
   csm_addr2       varchar2(64 char),
   csm_addr3       varchar2(64 char),
   csm_city        varchar2(64 char),
   csm_state       varchar2(64 char),
   csm_postal      varchar2(16 char),
   csm_country     varchar2(16 char),
   csm_phn         varchar2(32 char),
   csm_email       varchar2(120 char),
   csm_marital_st  number,
   csm_mbr_gender  number,
   /* AUDIT FIELDS -- the real ps_csm_voya_census carries no updated_at/updated_by;
      the package does not write audit columns on the census. */
   created_at      timestamp default on null systimestamp not null,
   created_by      varchar2(32 char) default on null nvl(
      sys_context('userenv','client_identifier'), user
   ) not null
);
