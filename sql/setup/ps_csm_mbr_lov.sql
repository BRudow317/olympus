/**************************************************************************
    Table: qbl.ps_csm_mbr_lov
    Member list-of-values lookup. mq_vw joins it twice: once on
    csm_mbr_lov_type = 'CSM_SEX' (gender) and once on 'CSM_MARITAL_ST'
    (marital status), each matching d.csm_sex / d.csm_maritial_st to
    csm_mbr_lov_id. lov ids are globally unique across types
    (sex 20/21, marital 23/24/25), so csm_mbr_lov_id is the PK.
**************************************************************************/
create table qbl.ps_csm_mbr_lov (
   csm_mbr_lov_id   number not null
      constraint pk_csm_mbr_lov primary key,
   csm_mbr_lov_type varchar2(32 char) not null,
   csm_mbr_lov_desc varchar2(120 char)
);
