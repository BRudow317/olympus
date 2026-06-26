declare
   v_response    number;
   v_server_name varchar2(50);
   e_not_dev_exception exception;
   e_expected_error exception;
   v_pid_1       varchar2(9);
   v_pid_2       varchar2(9);
   v_pid_3       varchar2(9);
   
    -- Cursor building mock golden records aka mq_inbound%rowtype
   cursor c_mock_data is
   with mock_rows as (
         -- Test Case 1: 404
      select '1' as pid,
             'John' as first_name,
             'A' as middle_name,
             'Doe' as last_name,
             1 as name_suffix,
             to_date('1985-05-12','YYYY-MM-DD') as birthdate,
             cast(null as date) as dt_of_death,
             'john.doe@email.com' as email_addr,
             20 as sex,
             21 as mar_status,
             '555-0100' as phone,
             1 as phone_type,
             '123 Main St' as address1,
             cast(null as varchar2(100)) as address2,
             cast(null as varchar2(100)) as address3,
             cast(null as varchar2(100)) as address4,
             'Austin' as city,
             'Travis' as county,
             'TX' as state,
             '78701' as postal,
             'USA' as country
        from dual
      union all
         -- Test Case 2: adding DoD 
      select (
         select csm_pen_id
           from perf.ps_csm_mbr_demogr
          where csm_death_dt is null
            and rownum = 1
      ) as pid,
             'Jane' as first_name,
             cast(null as varchar2(100)) as middle_name,
             'Smith' as last_name,
             cast(null as number) as name_suffix,
             to_date('1940-11-23','YYYY-MM-DD') as birthdate,
             to_date('2024-02-15','YYYY-MM-DD') as dt_of_death,
             'jane.smith@email.com' as email_addr,
             21 as sex,
             25 as mar_status,
             '555-0200' as phone,
             1 as phone_type,
             '456 Oak Ave' as address1,
             'Apt 2B' as address2,
             cast(null as varchar2(100)) as address3,
             cast(null as varchar2(100)) as address4,
             'Chicago' as city,
             'Cook' as county,
             'IL' as state,
             '60601' as postal,
             'USA' as country
        from dual
      union all
         -- Test Case 3: Minimal Record (testing default parameters)
      select '003003003' as pid,
             'Alex' as first_name,
             cast(null as varchar2(100)) as middle_name,
             'Jones' as last_name,
             cast(null as number) as name_suffix,
             cast(null as date) as birthdate,
             cast(null as date) as dt_of_death,
             cast(null as varchar2(100)) as email_addr,
             cast(null as number) as sex,
             cast(null as number) as mar_status,
             cast(null as varchar2(100)) as phone,
             cast(null as number) as phone_type,
             cast(null as varchar2(100)) as address1,
             cast(null as varchar2(100)) as address2,
             cast(null as varchar2(100)) as address3,
             cast(null as varchar2(100)) as address4,
             cast(null as varchar2(100)) as city,
             cast(null as varchar2(100)) as county,
             cast(null as varchar2(100)) as state,
             cast(null as varchar2(100)) as postal,
             cast(null as varchar2(100)) as country
        from dual
      union all
         -- Test Case 4: Changing new fields for found people
      select v.pid,
             v.first_name,
             v.middle_name,
             v.last_name,
             v.name_suffix_id as name_suffix,
             v.birthdate,
             v.dt_of_death,
             'test4-changed-data@example.com' as email_addr,
             v.gender_id as sex,
             v.mar_status_id as mar_status,
             '1112223344' as phone,
             v.phone_type_id as phone_type,
             v.address1,
             v.address2,
             v.address3,
             'ToWhomItMayConcern' as address4,
             v.city,
             'COOK' as county,
             v.state,
             v.postal,
             v.country
        from perf.mq_vw v
       where rownum <= 3
         and nvl(
         v.email_addr,
         'null'
      ) != 'test4-changed-data@example.com'
         and nvl(
         v.county,
         'null'
      ) != 'COOK'
         and nvl(
         v.phone,
         'null'
      ) != '1112223344'
         and nvl(
         v.address4,
         'null'
      ) != 'ToWhomItMayConcern'
   ) --end mock_rows
   select *
     from mock_rows;


    -- Cursor for mq_vw for a given pid
   cursor c_vw_details (
      i_pid varchar2
   ) is
   select *
     from perf.mq_vw
    where pid = i_pid;


    /**************************************************************************
        assert_source
    **************************************************************************/
   function assert_source (
      i_pid            in varchar2,
      i_field          in varchar2,
      i_expected_value in varchar2
   ) return boolean is
      v_sql_stmt    varchar2(1000);
      v_found_value varchar2(4000);
   begin
      v_sql_stmt := 'SELECT TO_CHAR('
                    || dbms_assert.enquote_name(i_field)
                    || ') 
                      FROM perf.mq_vw 
                      WHERE pid = :1 AND rownum <= 1';
      begin
         execute immediate v_sql_stmt
           into v_found_value
            using i_pid;
      exception
         when no_data_found then
            v_found_value := null;
      end;

      if nvl(
         i_expected_value,
         '###NULL###'
      ) = nvl(
         v_found_value,
         '###NULL###'
      ) then
         return true;
      else
         dbms_output.put_line('Assertion Failed for '
                              || i_field
                              || '. Expected: '
                              || i_expected_value
                              || ', Got: ' || v_found_value);
         return false;
      end if;
   end assert_source;
 
 
    /**************************************************************************
        assert_source_val
    **************************************************************************/
   procedure assert_source_val (
      i_pid            in varchar2,
      i_field          in varchar2,
      i_expected_value in varchar2
   ) is
   begin
      if not assert_source(
         i_pid,
         i_field,
         i_expected_value
      ) then
         raise_application_error(
            -20502,
            'ASSERTION FAILED: Field '
            || i_field
            || ' for PID '
            || i_pid
            || ' did not match expected value: '
            || nvl(
               i_expected_value,
               'NULL'
            )
         );
      else
         dbms_output.put_line('PASS [PID: '
                              || i_pid
                              || ']: Field '
                              || i_field
                              || ' matched expected value: ' || nvl(
            i_expected_value,
            'NULL'
         ));
      end if;
   end assert_source_val;
 
 
    /**************************************************************************
        assert_status
    **************************************************************************/
   procedure assert_status (
      i_pid  in varchar2,
      p_code in number
   ) is
      v_meaning varchar2(50);
   begin
      v_meaning :=
         case p_code
            when 200 then
               '200=OK'
            when 202 then
               '202=Accepted'
            when 304 then
               '304=No Change'
            when 404 then
               '404=Not Found'
            when 500 then
               '500=Error'
            else
               'UNKNOWN CODE: ' || to_char(p_code)
         end;

      if p_code = 500 then
         raise_application_error(
            -20500,
            'ASSERTION FAILED: Systemic 500 error encountered for PID ' || i_pid
         );
      elsif p_code not in ( 200,
                            202,
                            304,
                            404 ) then
         raise_application_error(
            -20501,
            'ASSERTION FAILED: Unexpected status returned for PID '
            || i_pid
            || ' ('
            || v_meaning
            || ')'
         );
      else
         dbms_output.put_line('PASS [PID: '
                              || i_pid
                              || ']: Processed successfully -> ' || v_meaning);
      end if;
   end assert_status;


    /**************************************************************************
        assert_actual_status
    **************************************************************************/
   procedure assert_actual_status (
      i_pid           in varchar2,
      p_expected_code in number,
      p_actual_code   in number
   ) is
      v_expected_meaning varchar2(50);
      v_actual_meaning   varchar2(50);

      function get_meaning (
         p_code in number
      ) return varchar2 is
      begin
         return
            case p_code
               when 200 then
                  '200=OK'
               when 202 then
                  '202=Accepted'
               when 304 then
                  '304=No Change'
               when 404 then
                  '404=Not Found'
               when 500 then
                  '500=Error'
               else
                  'UNKNOWN CODE: ' || to_char(p_code)
            end;
      end get_meaning;
   begin
      v_expected_meaning := get_meaning(p_expected_code);
      v_actual_meaning := get_meaning(p_actual_code);
      if p_expected_code = p_actual_code then
         dbms_output.put_line('PASS [PID: '
                              || i_pid
                              || ']: Status code matched expected ('
                              || v_expected_meaning || ')');
      else
         raise_application_error(
            -20503,
            'ASSERTION FAILED: Status code mismatch for PID '
            || i_pid
            || '. Expected: '
            || v_expected_meaning
            || ', Got: '
            || v_actual_meaning
         );
      end if;
   end assert_actual_status;
 
    
    /**************************************************************************
        test_mq_insert_census_mode
    **************************************************************************/
   procedure test_mq_insert_census_mode (
      i_pid        in varchar2,
      i_first_name in varchar2,
      i_last_name  in varchar2
   ) is
      v_resp             number;
      v_actual_status    number;
      v_census_first     varchar2(64);
      v_current_first    varchar2(64);
      v_true_first_name  varchar2(64);
      v_false_first_name varchar2(64);
   begin
      dbms_output.put_line('--- Running test_mq_insert_census_mode ---');

      -- Force a deterministic data change so this test is repeatable across runs.
      select nvl(
         first_name,
         'NULL'
      )
        into v_current_first
        from perf.mq_vw
       where pid = i_pid
         and rownum = 1;

      if v_current_first = nvl(
         i_first_name,
         'NULL'
      ) then
         v_true_first_name := substr(
            i_first_name || '_T1',
            1,
            64
         );
      else
         v_true_first_name := i_first_name;
      end if;
      v_false_first_name := substr(
         v_true_first_name || '_NoCensus',
         1,
         64
      );
      
      -- Test with i_block_census_mode => true
      delete from perf.ps_csm_voya_census
       where csm_pen_id = i_pid;
      insert into perf.ps_csm_voya_census (
         csm_pen_id,
         csm_first_nm,
         csm_last_nm
      ) values ( i_pid,
                 'OldFirst',
                 'OldLast' );

      perf.mq_pkg.mq_insert(
         pid                 => i_pid,
         first_name          => v_true_first_name,
         last_name           => i_last_name,
         response            => v_resp,
         i_block_census_mode => true
      );

      if v_resp != 0 then
         raise_application_error(
            -20504,
            'mq_insert returned failure response'
         );
      end if;
      select mq_status
        into v_actual_status
        from (
         select mq_status
           from perf.mq_inbound
          where pid = i_pid
          order by mq_id desc
      )
       where rownum = 1;

      assert_actual_status(
         i_pid,
         200,
         v_actual_status
      );
      
      -- Verify census was updated by census_sentinel
      select csm_first_nm
        into v_census_first
        from perf.ps_csm_voya_census
       where csm_pen_id = i_pid;

      if nvl(
         v_census_first,
         'NULL'
      ) != v_true_first_name then
         raise_application_error(
            -20506,
            'Census test failed: Expected census first name to be '
            || v_true_first_name
            || ', got '
            || nvl(
               v_census_first,
               'NULL'
            )
         );
      else
         dbms_output.put_line('PASS: Census table updated correctly when block_census_mode is true.');
      end if;
      
      -- Test with i_block_census_mode => false
      delete from perf.ps_csm_voya_census
       where csm_pen_id = i_pid;
      insert into perf.ps_csm_voya_census (
         csm_pen_id,
         csm_first_nm,
         csm_last_nm
      ) values ( i_pid,
                 'OldFirst',
                 'OldLast' );

      perf.mq_pkg.mq_insert(
         pid                 => i_pid,
         first_name          => v_false_first_name,
         last_name           => i_last_name,
         response            => v_resp,
         i_block_census_mode => false
      );

      select csm_first_nm
        into v_census_first
        from perf.ps_csm_voya_census
       where csm_pen_id = i_pid;

      if nvl(
         v_census_first,
         'NULL'
      ) = v_false_first_name then
         raise_application_error(
            -20507,
            'Census test failed: Census table should NOT have been updated when block_census_mode is false.'
         );
      else
         dbms_output.put_line('PASS: Census table remained unchanged when block_census_mode is false.');
      end if;
   end test_mq_insert_census_mode;

   /**************************************************************************
      test_process_mq_inbound
   **************************************************************************/
   procedure test_process_mq_inbound (
      i_pid_1 in varchar2,
      i_pid_2 in varchar2
   ) is
      v_vw_rec_1        c_vw_details%rowtype;
      v_vw_rec_2        c_vw_details%rowtype;
      v_row1_first_name varchar2(10);
      v_mq_id_1         number;
      v_mq_id_2         number;
      v_mq_id_3         number;
      v_status_1        number;
      v_status_2        number;
      v_status_3        number;
   begin
      dbms_output.put_line('--- Running test_process_mq_inbound (Bulk test 1) ---');
      delete from perf.mq_inbound
       where mq_status = 202;
      
      -- Fetch current state to construct deterministic mock updates
      open c_vw_details(i_pid_1);
      fetch c_vw_details into v_vw_rec_1;
      close c_vw_details;
      open c_vw_details(i_pid_2);
      fetch c_vw_details into v_vw_rec_2;
      close c_vw_details;

      -- Toggle to a guaranteed-different value so this test is rerun-safe.
      if nvl(
         v_vw_rec_1.first_name,
         'NULL'
      ) = 'X1' then
         v_row1_first_name := 'X2';
      else
         v_row1_first_name := 'X1';
      end if;
      
      -- 1. Row to update (status 200)
      insert into perf.mq_inbound (
         pid,
         first_name,
         middle_name,
         last_name,
         name_suffix_id,
         birthdate,
         dt_of_death,
         email_addr,
         gender_id,
         mar_status_id,
         phone,
         phone_type_id,
         address1,
         address2,
         address3,
         address4,
         city,
         county,
         state,
         postal,
         country,
         mq_status
      ) values ( v_vw_rec_1.pid,
                 v_row1_first_name,
                 v_vw_rec_1.middle_name,
                 v_vw_rec_1.last_name,
                 v_vw_rec_1.name_suffix_id,
                 v_vw_rec_1.birthdate,
                 v_vw_rec_1.dt_of_death,
                 v_vw_rec_1.email_addr,
                 v_vw_rec_1.gender_id,
                 v_vw_rec_1.mar_status_id,
                 v_vw_rec_1.phone,
                 v_vw_rec_1.phone_type_id,
                 v_vw_rec_1.address1,
                 v_vw_rec_1.address2,
                 v_vw_rec_1.address3,
                 v_vw_rec_1.address4,
                 v_vw_rec_1.city,
                 v_vw_rec_1.county,
                 v_vw_rec_1.state,
                 v_vw_rec_1.postal,
                 v_vw_rec_1.country,
                 202 ) returning mq_id into v_mq_id_1;
      
      -- 2. Row with no changes (status 304)
      insert into perf.mq_inbound (
         pid,
         first_name,
         middle_name,
         last_name,
         name_suffix_id,
         birthdate,
         dt_of_death,
         email_addr,
         gender_id,
         mar_status_id,
         phone,
         phone_type_id,
         address1,
         address2,
         address3,
         address4,
         city,
         county,
         state,
         postal,
         country,
         mq_status
      ) values ( v_vw_rec_2.pid,
                 v_vw_rec_2.first_name,
                 v_vw_rec_2.middle_name,
                 v_vw_rec_2.last_name,
                 v_vw_rec_2.name_suffix_id,
                 v_vw_rec_2.birthdate,
                 v_vw_rec_2.dt_of_death,
                 v_vw_rec_2.email_addr,
                 v_vw_rec_2.gender_id,
                 v_vw_rec_2.mar_status_id,
                 v_vw_rec_2.phone,
                 v_vw_rec_2.phone_type_id,
                 v_vw_rec_2.address1,
                 v_vw_rec_2.address2,
                 v_vw_rec_2.address3,
                 v_vw_rec_2.address4,
                 v_vw_rec_2.city,
                 v_vw_rec_2.county,
                 v_vw_rec_2.state,
                 v_vw_rec_2.postal,
                 v_vw_rec_2.country,
                 202 ) returning mq_id into v_mq_id_2;
      
      -- 3. Row with invalid/non-existent pid (status 404)
      insert into perf.mq_inbound (
         pid,
         first_name,
         last_name,
         mq_status
      ) values ( '999999999999',
                 'NonExist',
                 'Person',
                 202 ) returning mq_id into v_mq_id_3;

      perf.mq_pkg.process_mq_inbound(
         i_starting_mq_id => least(
            v_mq_id_1,
            v_mq_id_2,
            v_mq_id_3
         ),
         i_status         => 202
      );

      select mq_status
        into v_status_1
        from perf.mq_inbound
       where mq_id = v_mq_id_1;
      select mq_status
        into v_status_2
        from perf.mq_inbound
       where mq_id = v_mq_id_2;
      select mq_status
        into v_status_3
        from perf.mq_inbound
       where mq_id = v_mq_id_3;

      assert_actual_status(
         i_pid_1,
         200,
         v_status_1
      );
      assert_actual_status(
         i_pid_2,
         304,
         v_status_2
      );
      assert_actual_status(
         '999999999999',
         404,
         v_status_3
      );
      
      -- Verify the update took effect in the source view for Row 1
      assert_source_val(
         i_pid_1,
         'first_name',
         v_row1_first_name
      );
   end test_process_mq_inbound;

   /**************************************************************************
      test_process_mq_inbound_max_records
   **************************************************************************/
   procedure test_process_mq_inbound_max_records (
      i_pid_1 in varchar2,
      i_pid_2 in varchar2,
      i_pid_3 in varchar2
   ) is
      v_vw_rec_1 c_vw_details%rowtype;
      v_vw_rec_2 c_vw_details%rowtype;
      v_vw_rec_3 c_vw_details%rowtype;
      v_mq_id_1  number;
      v_mq_id_2  number;
      v_mq_id_3  number;
      v_status_1 number;
      v_status_2 number;
      v_status_3 number;
   begin
      dbms_output.put_line('--- Running test_process_mq_inbound_max_records ---');
      delete from perf.mq_inbound
       where mq_status = 202;

      open c_vw_details(i_pid_1);
      fetch c_vw_details into v_vw_rec_1;
      close c_vw_details;
      open c_vw_details(i_pid_2);
      fetch c_vw_details into v_vw_rec_2;
      close c_vw_details;
      open c_vw_details(i_pid_3);
      fetch c_vw_details into v_vw_rec_3;
      close c_vw_details;
      insert into perf.mq_inbound (
         pid,
         first_name,
         middle_name,
         last_name,
         name_suffix_id,
         birthdate,
         dt_of_death,
         email_addr,
         gender_id,
         mar_status_id,
         phone,
         phone_type_id,
         address1,
         address2,
         address3,
         address4,
         city,
         county,
         state,
         postal,
         country,
         mq_status
      ) values ( v_vw_rec_1.pid,
                 v_vw_rec_1.first_name || 'Max1',
                 v_vw_rec_1.middle_name,
                 v_vw_rec_1.last_name,
                 v_vw_rec_1.name_suffix_id,
                 v_vw_rec_1.birthdate,
                 v_vw_rec_1.dt_of_death,
                 v_vw_rec_1.email_addr,
                 v_vw_rec_1.gender_id,
                 v_vw_rec_1.mar_status_id,
                 v_vw_rec_1.phone,
                 v_vw_rec_1.phone_type_id,
                 v_vw_rec_1.address1,
                 v_vw_rec_1.address2,
                 v_vw_rec_1.address3,
                 v_vw_rec_1.address4,
                 v_vw_rec_1.city,
                 v_vw_rec_1.county,
                 v_vw_rec_1.state,
                 v_vw_rec_1.postal,
                 v_vw_rec_1.country,
                 202 ) returning mq_id into v_mq_id_1;

      insert into perf.mq_inbound (
         pid,
         first_name,
         middle_name,
         last_name,
         name_suffix_id,
         birthdate,
         dt_of_death,
         email_addr,
         gender_id,
         mar_status_id,
         phone,
         phone_type_id,
         address1,
         address2,
         address3,
         address4,
         city,
         county,
         state,
         postal,
         country,
         mq_status
      ) values ( v_vw_rec_2.pid,
                 v_vw_rec_2.first_name || 'Max2',
                 v_vw_rec_2.middle_name,
                 v_vw_rec_2.last_name,
                 v_vw_rec_2.name_suffix_id,
                 v_vw_rec_2.birthdate,
                 v_vw_rec_2.dt_of_death,
                 v_vw_rec_2.email_addr,
                 v_vw_rec_2.gender_id,
                 v_vw_rec_2.mar_status_id,
                 v_vw_rec_2.phone,
                 v_vw_rec_2.phone_type_id,
                 v_vw_rec_2.address1,
                 v_vw_rec_2.address2,
                 v_vw_rec_2.address3,
                 v_vw_rec_2.address4,
                 v_vw_rec_2.city,
                 v_vw_rec_2.county,
                 v_vw_rec_2.state,
                 v_vw_rec_2.postal,
                 v_vw_rec_2.country,
                 202 ) returning mq_id into v_mq_id_2;

      insert into perf.mq_inbound (
         pid,
         first_name,
         middle_name,
         last_name,
         name_suffix_id,
         birthdate,
         dt_of_death,
         email_addr,
         gender_id,
         mar_status_id,
         phone,
         phone_type_id,
         address1,
         address2,
         address3,
         address4,
         city,
         county,
         state,
         postal,
         country,
         mq_status
      ) values ( v_vw_rec_3.pid,
                 v_vw_rec_3.first_name || 'Max3',
                 v_vw_rec_3.middle_name,
                 v_vw_rec_3.last_name,
                 v_vw_rec_3.name_suffix_id,
                 v_vw_rec_3.birthdate,
                 v_vw_rec_3.dt_of_death,
                 v_vw_rec_3.email_addr,
                 v_vw_rec_3.gender_id,
                 v_vw_rec_3.mar_status_id,
                 v_vw_rec_3.phone,
                 v_vw_rec_3.phone_type_id,
                 v_vw_rec_3.address1,
                 v_vw_rec_3.address2,
                 v_vw_rec_3.address3,
                 v_vw_rec_3.address4,
                 v_vw_rec_3.city,
                 v_vw_rec_3.county,
                 v_vw_rec_3.state,
                 v_vw_rec_3.postal,
                 v_vw_rec_3.country,
                 202 ) returning mq_id into v_mq_id_3;
      
      -- Call with max_records => 2. Only first 2 should process, third remains 202
      perf.mq_pkg.process_mq_inbound(
         i_starting_mq_id => least(
            v_mq_id_1,
            v_mq_id_2,
            v_mq_id_3
         ),
         i_status         => 202,
         i_max_records    => 2
      );

      select mq_status
        into v_status_1
        from perf.mq_inbound
       where mq_id = v_mq_id_1;
      select mq_status
        into v_status_2
        from perf.mq_inbound
       where mq_id = v_mq_id_2;
      select mq_status
        into v_status_3
        from perf.mq_inbound
       where mq_id = v_mq_id_3;

      if v_status_1 = 202
      or v_status_2 = 202 then
         raise_application_error(
            -20520,
            'Max records test failed: expected first two rows to be processed.'
         );
      end if;

      if v_status_3 != 202 then
         raise_application_error(
            -20521,
            'Max records test failed: expected third row to remain at 202, got ' || v_status_3
         );
      else
         dbms_output.put_line('PASS: Third row remained 202 as expected (throttled).');
      end if;
   end test_process_mq_inbound_max_records;

   /**************************************************************************
      test_process_mq_inbound_chunk_size
   **************************************************************************/
   procedure test_process_mq_inbound_chunk_size (
      i_pid_1 in varchar2,
      i_pid_2 in varchar2
   ) is
      v_vw_rec_1 c_vw_details%rowtype;
      v_vw_rec_2 c_vw_details%rowtype;
      v_mq_id_1  number;
      v_mq_id_2  number;
      v_status_1 number;
      v_status_2 number;
   begin
      dbms_output.put_line('--- Running test_process_mq_inbound_chunk_size ---');
      delete from perf.mq_inbound
       where mq_status = 202;

      open c_vw_details(i_pid_1);
      fetch c_vw_details into v_vw_rec_1;
      close c_vw_details;
      open c_vw_details(i_pid_2);
      fetch c_vw_details into v_vw_rec_2;
      close c_vw_details;
      insert into perf.mq_inbound (
         pid,
         first_name,
         middle_name,
         last_name,
         name_suffix_id,
         birthdate,
         dt_of_death,
         email_addr,
         gender_id,
         mar_status_id,
         phone,
         phone_type_id,
         address1,
         address2,
         address3,
         address4,
         city,
         county,
         state,
         postal,
         country,
         mq_status
      ) values ( v_vw_rec_1.pid,
                 v_vw_rec_1.first_name || 'Chnk1',
                 v_vw_rec_1.middle_name,
                 v_vw_rec_1.last_name,
                 v_vw_rec_1.name_suffix_id,
                 v_vw_rec_1.birthdate,
                 v_vw_rec_1.dt_of_death,
                 v_vw_rec_1.email_addr,
                 v_vw_rec_1.gender_id,
                 v_vw_rec_1.mar_status_id,
                 v_vw_rec_1.phone,
                 v_vw_rec_1.phone_type_id,
                 v_vw_rec_1.address1,
                 v_vw_rec_1.address2,
                 v_vw_rec_1.address3,
                 v_vw_rec_1.address4,
                 v_vw_rec_1.city,
                 v_vw_rec_1.county,
                 v_vw_rec_1.state,
                 v_vw_rec_1.postal,
                 v_vw_rec_1.country,
                 202 ) returning mq_id into v_mq_id_1;

      insert into perf.mq_inbound (
         pid,
         first_name,
         middle_name,
         last_name,
         name_suffix_id,
         birthdate,
         dt_of_death,
         email_addr,
         gender_id,
         mar_status_id,
         phone,
         phone_type_id,
         address1,
         address2,
         address3,
         address4,
         city,
         county,
         state,
         postal,
         country,
         mq_status
      ) values ( v_vw_rec_2.pid,
                 v_vw_rec_2.first_name || 'Chnk2',
                 v_vw_rec_2.middle_name,
                 v_vw_rec_2.last_name,
                 v_vw_rec_2.name_suffix_id,
                 v_vw_rec_2.birthdate,
                 v_vw_rec_2.dt_of_death,
                 v_vw_rec_2.email_addr,
                 v_vw_rec_2.gender_id,
                 v_vw_rec_2.mar_status_id,
                 v_vw_rec_2.phone,
                 v_vw_rec_2.phone_type_id,
                 v_vw_rec_2.address1,
                 v_vw_rec_2.address2,
                 v_vw_rec_2.address3,
                 v_vw_rec_2.address4,
                 v_vw_rec_2.city,
                 v_vw_rec_2.county,
                 v_vw_rec_2.state,
                 v_vw_rec_2.postal,
                 v_vw_rec_2.country,
                 202 ) returning mq_id into v_mq_id_2;
      
      -- Process with commit size (chunk size) = 1
      perf.mq_pkg.process_mq_inbound(
         i_starting_mq_id => least(
            v_mq_id_1,
            v_mq_id_2
         ),
         i_status         => 202,
         i_commit_size    => 1
      );

      select mq_status
        into v_status_1
        from perf.mq_inbound
       where mq_id = v_mq_id_1;
      select mq_status
        into v_status_2
        from perf.mq_inbound
       where mq_id = v_mq_id_2;

      if v_status_1 = 202
      or v_status_2 = 202 then
         raise_application_error(
            -20530,
            'Chunk size test failed: expected both rows to be processed.'
         );
      else
         dbms_output.put_line('PASS: Both rows processed successfully in chunks of 1.');
      end if;
   end test_process_mq_inbound_chunk_size;


    /**************************************************************************
        test_process_mq_inbound_fallback
    **************************************************************************/
   procedure test_process_mq_inbound_fallback (
      i_pid_1 in varchar2,
      i_pid_2 in varchar2,
      i_pid_3 in varchar2
   ) is
      v_vw_rec_1         c_vw_details%rowtype;
      v_vw_rec_2         c_vw_details%rowtype;
      v_vw_rec_3         c_vw_details%rowtype;
      v_first_nm_max_len number;
      v_bad_first_name   varchar2(4000);
      v_mq_id_1          number;
      v_mq_id_2          number;
      v_mq_id_3          number;
      v_status_1         number;
      v_status_2         number;
      v_status_3         number;
      v_log_count        number;
   begin
      dbms_output.put_line('--- Running test_process_mq_inbound_fallback ---');
      delete from perf.mq_inbound
       where mq_status = 202;

      open c_vw_details(i_pid_1);
      fetch c_vw_details into v_vw_rec_1;
      close c_vw_details;
      open c_vw_details(i_pid_2);
      fetch c_vw_details into v_vw_rec_2;
      close c_vw_details;
      open c_vw_details(i_pid_3);
      fetch c_vw_details into v_vw_rec_3;
      close c_vw_details;

      -- Build a value that is guaranteed to violate the target demo first-name
      -- column width during processing (but still stages in mq_inbound).
      select data_length
        into v_first_nm_max_len
        from all_tab_columns
       where owner = 'PERF'
         and table_name = 'PS_CSM_MBR_DEMOGR'
         and column_name = 'CSM_FIRST_NM';

      v_bad_first_name := rpad(
         'B',
         least(
                v_first_nm_max_len + 1,
                4000
             ),
         'B'
      );
      
      -- 1. Good row
      insert into perf.mq_inbound (
         pid,
         first_name,
         middle_name,
         last_name,
         name_suffix_id,
         birthdate,
         dt_of_death,
         email_addr,
         gender_id,
         mar_status_id,
         phone,
         phone_type_id,
         address1,
         address2,
         address3,
         address4,
         city,
         county,
         state,
         postal,
         country,
         mq_status
      ) values ( v_vw_rec_1.pid,
                 v_vw_rec_1.first_name || 'Fbak1',
                 v_vw_rec_1.middle_name,
                 v_vw_rec_1.last_name,
                 v_vw_rec_1.name_suffix_id,
                 v_vw_rec_1.birthdate,
                 v_vw_rec_1.dt_of_death,
                 v_vw_rec_1.email_addr,
                 v_vw_rec_1.gender_id,
                 v_vw_rec_1.mar_status_id,
                 v_vw_rec_1.phone,
                 v_vw_rec_1.phone_type_id,
                 v_vw_rec_1.address1,
                 v_vw_rec_1.address2,
                 v_vw_rec_1.address3,
                 v_vw_rec_1.address4,
                 v_vw_rec_1.city,
                 v_vw_rec_1.county,
                 v_vw_rec_1.state,
                 v_vw_rec_1.postal,
                 v_vw_rec_1.country,
                 202 ) returning mq_id into v_mq_id_1;
      
            -- 2. Bad row (guaranteed CSM_FIRST_NM width violation in target update)
      insert into perf.mq_inbound (
         pid,
         first_name,
         middle_name,
         last_name,
         name_suffix_id,
         birthdate,
         dt_of_death,
         email_addr,
         gender_id,
         mar_status_id,
         phone,
         phone_type_id,
         address1,
         address2,
         address3,
         address4,
         city,
         county,
         state,
         postal,
         country,
         mq_status
      ) values ( v_vw_rec_2.pid,
                 v_bad_first_name,
                 v_vw_rec_2.middle_name,
                 v_vw_rec_2.last_name,
                 v_vw_rec_2.name_suffix_id,
                 v_vw_rec_2.birthdate,
                 v_vw_rec_2.dt_of_death,
                 v_vw_rec_2.email_addr,
                 v_vw_rec_2.gender_id,
                 v_vw_rec_2.mar_status_id,
                 v_vw_rec_2.phone,
                 v_vw_rec_2.phone_type_id,
                 v_vw_rec_2.address1,
                 v_vw_rec_2.address2,
                 v_vw_rec_2.address3,
                 v_vw_rec_2.address4,
                 v_vw_rec_2.city,
                 v_vw_rec_2.county,
                 v_vw_rec_2.state,
                 v_vw_rec_2.postal,
                 v_vw_rec_2.country,
                 202 ) returning mq_id into v_mq_id_2;
      
      -- 3. Good row
      insert into perf.mq_inbound (
         pid,
         first_name,
         middle_name,
         last_name,
         name_suffix_id,
         birthdate,
         dt_of_death,
         email_addr,
         gender_id,
         mar_status_id,
         phone,
         phone_type_id,
         address1,
         address2,
         address3,
         address4,
         city,
         county,
         state,
         postal,
         country,
         mq_status
      ) values ( v_vw_rec_3.pid,
                 v_vw_rec_3.first_name || 'Fbak3',
                 v_vw_rec_3.middle_name,
                 v_vw_rec_3.last_name,
                 v_vw_rec_3.name_suffix_id,
                 v_vw_rec_3.birthdate,
                 v_vw_rec_3.dt_of_death,
                 v_vw_rec_3.email_addr,
                 v_vw_rec_3.gender_id,
                 v_vw_rec_3.mar_status_id,
                 v_vw_rec_3.phone,
                 v_vw_rec_3.phone_type_id,
                 v_vw_rec_3.address1,
                 v_vw_rec_3.address2,
                 v_vw_rec_3.address3,
                 v_vw_rec_3.address4,
                 v_vw_rec_3.city,
                 v_vw_rec_3.county,
                 v_vw_rec_3.state,
                 v_vw_rec_3.postal,
                 v_vw_rec_3.country,
                 202 ) returning mq_id into v_mq_id_3;
      
      -- Clean up old logs for these mq_ids
      delete from perf.mq_pkg_log
       where mq_id in ( v_mq_id_1,
                        v_mq_id_2,
                        v_mq_id_3 );
      
      -- Run with allow partial success => true
      perf.mq_pkg.process_mq_inbound(
         i_starting_mq_id        => least(
            v_mq_id_1,
            v_mq_id_2,
            v_mq_id_3
         ),
         i_status                => 202,
         i_allow_partial_success => true
      );

      select mq_status
        into v_status_1
        from perf.mq_inbound
       where mq_id = v_mq_id_1;
      select mq_status
        into v_status_2
        from perf.mq_inbound
       where mq_id = v_mq_id_2;
      select mq_status
        into v_status_3
        from perf.mq_inbound
       where mq_id = v_mq_id_3;

      if v_status_1 = 202
      or v_status_1 = 500 then
         raise_application_error(
            -20540,
            'Fallback test failed: Row 1 should have succeeded, got status ' || v_status_1
         );
      end if;

      if v_status_2 != 500 then
         raise_application_error(
            -20541,
            'Fallback test failed: Row 2 should have failed with status 500, got ' || v_status_2
         );
      end if;

      if v_status_3 = 202
      or v_status_3 = 500 then
         raise_application_error(
            -20542,
            'Fallback test failed: Row 3 should have succeeded, got status ' || v_status_3
         );
      end if;
      
      -- Check that an autonomous transaction logged the error in mq_pkg_log
      select count(*)
        into v_log_count
        from perf.mq_pkg_log
       where mq_id = v_mq_id_2
         and error_code is not null;

      if v_log_count = 0 then
         raise_application_error(
            -20543,
            'Fallback test failed: No log entry found in mq_pkg_log for failed mq_id ' || v_mq_id_2
         );
      else
         dbms_output.put_line('PASS: Fallback to row-by-row worked. Good rows processed, bad row got 500 and logged.');
      end if;
   end test_process_mq_inbound_fallback;

-- Execution Section Begin
begin
    -- Server name check
   select lower(nvl(
      sys_context(
         'USERENV',
         'SERVER_HOST'
      ),
      'dev'
   ))
     into v_server_name
     from dual;

   if instr(
      v_server_name,
      'dev'
   ) = 0 then
      dbms_output.put_line('Non-dev host detected ('
                           || v_server_name || '). Skipping execution safely.');
      raise e_not_dev_exception;
   end if;

    -- Sub block
    -- Fetch existing PIDs dynamically
   declare
      cursor c_setui_pids is
      select pid
        from (
         select pid
           from perf.mq_vw
          order by pid
      )
       where rownum <= 3;

      type t_setup_rec is record (
         pid varchar2(9)
      );
      type t_setui_pids is
         table of t_setup_rec index by pls_integer;
      v_setui_pids t_setui_pids;
   begin
      open c_setui_pids;
      fetch c_setui_pids
      bulk collect into v_setui_pids;
      close c_setui_pids;
      if v_setui_pids.count < 3 then
         raise_application_error(
            -20600,
            'Test setup failed: Need at least 3 records in perf.mq_vw to run tests, found ' || v_setui_pids.count
         );
      end if;

      v_pid_1 := v_setui_pids(1).pid;
      v_pid_2 := v_setui_pids(2).pid;
      v_pid_3 := v_setui_pids(3).pid;
   end;

    -- Execute older tests 
   dbms_output.put_line('=== Executing older insert loop ===');
   for r in c_mock_data loop
      dbms_output.put_line('Attempting mq_insert for PID: ' || r.pid);
      perf.mq_pkg.mq_insert(
         pid         => r.pid,
         first_name  => r.first_name,
         middle_name => r.middle_name,
         last_name   => r.last_name,
         name_suffix => r.name_suffix,
         birthdate   => r.birthdate,
         dt_of_death => r.dt_of_death,
         email_addr  => r.email_addr,
         sex         => r.sex,
         mar_status  => r.mar_status,
         phone       => r.phone,
         phone_type  => r.phone_type,
         address1    => r.address1,
         address2    => r.address2,
         address3    => r.address3,
         address4    => r.address4,
         city        => r.city,
         county      => r.county,
         state       => r.state,
         postal      => r.postal,
         country     => r.country,
         response    => v_response
      );

        -- Validating response and querying actual status_code for the assertion
      declare
         v_actual_status number;
      begin
         if v_response != 0 then
            raise_application_error(
               -20504,
               'ASSERTION FAILED: mq_insert returned failure response code: ' || v_response
            );
         end if;

         select mq_status
           into v_actual_status
           from (
            select mq_status
              from perf.mq_inbound
             where pid = r.pid
             order by mq_id desc
         )
          where rownum = 1;

         assert_status(
            i_pid  => r.pid,
            p_code => v_actual_status
         );
      exception
         when no_data_found then
            raise_application_error(
               -20505,
               'ASSERTION FAILED: No record found in mq_inbound for PID: ' || r.pid
            );
      end;
      dbms_output.put_line('Success mq_insert completion for PID: ' || r.pid);
   end loop;

    -- Execute new unit tests
   dbms_output.put_line('=== Executing New Unit Tests ===');
   test_mq_insert_census_mode(
      v_pid_2,
      'NewCensusFirst',
      'NewCensusLast'
   );
   test_process_mq_inbound(
      v_pid_1,
      v_pid_2
   );
   test_process_mq_inbound_max_records(
      v_pid_1,
      v_pid_2,
      v_pid_3
   );
   test_process_mq_inbound_chunk_size(
      v_pid_1,
      v_pid_2
   );
   test_process_mq_inbound_fallback(
      v_pid_1,
      v_pid_2,
      v_pid_3
   );
   dbms_output.put_line('Success: All test cases evaluated successfully.');
exception
   when e_not_dev_exception then
      null;
   when others then
      rollback;
      raise;
end;