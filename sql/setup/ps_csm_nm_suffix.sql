/**************************************************************************
    Table: qbl.ps_csm_nm_suffix
    Name suffix lookup. mq_vw joins csm_nm_suf_id to d.csm_nm_suf_id to
    surface csm_nm_suf_desc. mq_insert notes suffix ids run 1..13.
**************************************************************************/
create table qbl.ps_csm_nm_suffix (
   csm_nm_suf_id   number not null
      constraint pk_csm_nm_suffix primary key,
   csm_nm_suf_desc varchar2(64 char)
);
