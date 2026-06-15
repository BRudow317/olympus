/**************************************************************************
    Table: qbl.ps_csm_mbr_addr
    Member addresses. Keyed by (csm_mbr_demogr_id, csm_mbr_addr_id) -- the
    pair mq_pkg uses to target an address row. Child of ps_csm_mbr_demogr.

    Columns/widths inferred from qbl.mq_inbound and the references in
    qbl.mq_vw and qbl.mq_pkg.
**************************************************************************/
create table qbl.ps_csm_mbr_addr (
   csm_mbr_addr_id   number not null,
   csm_mbr_demogr_id varchar2(24 char) not null,
   csm_addr1         varchar2(64 char),
   csm_addr2         varchar2(64 char),
   csm_addr3         varchar2(64 char),
   csm_addr4         varchar2(64 char),
   csm_city          varchar2(64 char),
   csm_county        varchar2(64 char),
   csm_state         varchar2(64 char),
   csm_postal        varchar2(16 char),
   csm_country       varchar2(16 char),
   csm_addr_type     varchar2(32 char),
   /* AUDIT FIELDS */
   create_date       timestamp default on null systimestamp not null,
   created_by        varchar2(32 char) default on null nvl(
      sys_context('userenv','client_identifier'), user
   ) not null,
   update_date       timestamp default on null systimestamp not null,
   updated_by        varchar2(32 char) default on null nvl(
      sys_context('userenv','client_identifier'), user
   ) not null,
   constraint pk_csm_mbr_addr primary key ( csm_mbr_demogr_id, csm_mbr_addr_id ),
   constraint fk_csm_addr_demogr foreign key ( csm_mbr_demogr_id )
      references qbl.ps_csm_mbr_demogr ( csm_mbr_demogr_id )
);
