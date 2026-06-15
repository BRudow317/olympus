/**************************************************************************
    Table: qbl.ps_csm_nm_prefix
    Name prefix lookup. mq_vw joins csm_nm_pre_id to d.csm_nm_pre_id to
    surface csm_nm_pre_desc.
**************************************************************************/
create table qbl.ps_csm_nm_prefix (
   csm_nm_pre_id   number not null
      constraint pk_csm_nm_prefix primary key,
   csm_nm_pre_desc varchar2(64 char)
);
