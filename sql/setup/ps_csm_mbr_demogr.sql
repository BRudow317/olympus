/**************************************************************************
    Table: qbl.ps_csm_mbr_demogr
    Member demographics -- the core ERM record that mq_vw and mq_pkg
    compare inbound MDM rows against. Parent of ps_csm_mbr_addr and
    ps_csm_mbr_phn (via csm_mbr_demogr_id).

    Columns/widths inferred from qbl.mq_inbound and the references in
    qbl.mq_vw and qbl.mq_pkg (process_record / process_window_setbased).
**************************************************************************/
create table qbl.ps_csm_mbr_demogr (
   csm_mbr_demogr_id varchar2(24 char) not null
      constraint pk_csm_mbr_demogr primary key,
   csm_pen_id        varchar2(12 char) not null,
   /* NAME */
   csm_first_nm      varchar2(64 char),
   csm_middle_nm     varchar2(64 char),
   csm_last_nm       varchar2(64 char),
   csm_nm_pre_id     number,
   csm_nm_suf_id     number,
   /* DEMO */
   csm_birth_dt      date,
   csm_death_dt      date,
   csm_email         varchar2(120 char),
   csm_sex           number,
   csm_maritial_st   number,
   csm_ssn           varchar2(11 char),
   /* TERTIARY */
   csm_trf_pre96_flg varchar2(1 char),
   csm_status_id     varchar2(12 char),
   /* AUDIT FIELDS */
   create_date       timestamp default on null systimestamp not null,
   created_by        varchar2(32 char) default on null nvl(
      sys_context('userenv','client_identifier'), user
   ) not null,
   update_date       timestamp default on null systimestamp not null,
   updated_by        varchar2(32 char) default on null nvl(
      sys_context('userenv','client_identifier'), user
   ) not null
);
