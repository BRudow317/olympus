create table qbl.mq_inbound ( /*Shared global sequence with qbl.MQ_OUTBOUND*/
   mq_id             number default on null global_mq_id_sequence.nextval primary key,
   csm_mbr_demogr_id varchar2(24 char),
   pid               varchar2(12 char) not null, /*DEMO*/
   first_name        varchar2(64 char),
   middle_name       varchar2(64 char),
   last_name         varchar2(64 char),
   name_suffix_id    number,
   birthdate         date,
   dt_of_death       date,
   email_addr        varchar2(120 char),
   gender_id         number,
   mar_status_id     number,
   mq_demogr_hash    varchar2(32 char), /*PHONE*/
   csm_mbr_phn_id    number,
   phone             varchar2(32 char),
   phone_type_id     number,
   mq_phone_hash     varchar2(32 char), /*ADDRESS*/
   csm_mbr_addr_id   number,
   address1          varchar2(64 char),
   address2          varchar2(64 char),
   address3          varchar2(64 char),
   address4          varchar2(64 char),
   city              varchar2(64 char),
   county            varchar2(64 char),
   state             varchar2(64 char),
   postal            varchar2(16 char),
   country           varchar2(16 char),
   mq_address_hash   varchar2(32 char), /*INTEGRATION*/
   mq_status         number default on null 202, --202 Accepted --200 OK --304 No Change --404 Not Found --500 Error 
   constraint fk_mqi_status_code foreign key ( mq_status )
      references qbl.http_codes ( code ), /*AUDIT FIELDS*/
   updated_at        timestamp default on null systimestamp not null,
   updated_by        varchar2(32 char) default on null nvl(
      sys_context(
         'userenv',
         'client_identifier'
      ),
      user
   ) not null,
   created_at        timestamp default on null systimestamp not null,
   created_by        varchar2(32 char) default on null nvl(
      sys_context(
         'userenv',
         'client_identifier'
      ),
      user
   ) not null
)
      partition by list ( mq_status ) ( partition p_new values ( 202 ), --Accepted 
         partition p_default values ( default )
      )
   enable row movement;