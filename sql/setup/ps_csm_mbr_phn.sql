/**************************************************************************
    Table: qbl.ps_csm_mbr_phn
    Member phones. Keyed by (csm_mbr_demogr_id, csm_mbr_phn_id). mq_vw pins
    the phone leg to csm_phn_type_id = 1. Child of ps_csm_mbr_demogr.

    Columns/widths inferred from qbl.mq_inbound and the references in
    qbl.mq_vw and qbl.mq_pkg.
**************************************************************************/
create table qbl.ps_csm_mbr_phn (
   csm_mbr_phn_id    number not null,
   csm_mbr_demogr_id varchar2(24 char) not null,
   csm_phn           varchar2(32 char),
   csm_phn_type_id   number,
   phone_ext         varchar2(16 char),
   /* AUDIT FIELDS */
   create_date       timestamp default on null systimestamp not null,
   created_by        varchar2(32 char) default on null nvl(
      sys_context('userenv','client_identifier'), user
   ) not null,
   update_date       timestamp default on null systimestamp not null,
   updated_by        varchar2(32 char) default on null nvl(
      sys_context('userenv','client_identifier'), user
   ) not null,
   constraint pk_csm_mbr_phn primary key ( csm_mbr_demogr_id, csm_mbr_phn_id ),
   constraint fk_csm_phn_demogr foreign key ( csm_mbr_demogr_id )
      references qbl.ps_csm_mbr_demogr ( csm_mbr_demogr_id )
);
