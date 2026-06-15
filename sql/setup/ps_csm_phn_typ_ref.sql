/**************************************************************************
    Table: qbl.ps_csm_phn_typ_ref
    Phone type lookup. mq_vw joins phone_type_id to p.csm_phn_type_id to
    surface csm_phone_type. (Note the non-csm_-prefixed key column name,
    matching the reference in qbl.mq_vw.)
**************************************************************************/
create table qbl.ps_csm_phn_typ_ref (
   phone_type_id  number not null
      constraint pk_csm_phn_typ_ref primary key,
   csm_phone_type varchar2(32 char)
);
