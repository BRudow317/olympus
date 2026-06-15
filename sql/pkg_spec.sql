create or replace package qbl.mq_pkg as
   procedure mq_insert (
      pid         in varchar2,
      first_name  in varchar2 default null,
      middle_name in varchar2 default null,
      last_name   in varchar2 default null,
      name_suffix in number default null,
      birthdate   in date default null,
      dt_of_death in date default null,
      email_addr  in varchar2 default null,
      sex         in number default null,
      mar_status  in number default null,
      phone       in varchar2 default null,
      phone_type  in number default null,
      address1    in varchar2 default null,
      address2    in varchar2 default null,
      address3    in varchar2 default null,
      address4    in varchar2 default null,
      city        in varchar2 default null,
      county      in varchar2 default null,
      state       in varchar2 default null,
      postal      in varchar2 default null,
      country     in varchar2 default null,
      response    out number
   );

   procedure process_mq_inbound (
      i_starting_mq_id in number := 0,
      i_chunk_size     in number := 0,
      i_status         in number := 202,
      i_bulk_mode      in number := 0,
      i_raise_on_error in number := 0
   );

end mq_pkg;
/