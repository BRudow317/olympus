CREATE OR REPLACE PROCEDURE test_mq_pkg 
AS
   v_response          NUMBER;
   v_server_name       VARCHAR2(50);
   e_not_dev_exception EXCEPTION;
   e_expected_error    EXCEPTION;
   
   -- 1. Cursors must come first
   CURSOR c_mock_data IS
      WITH mock_rows AS (
         -- Test Case 1: 404
         SELECT 
            '1' AS pid, 'John' AS first_name, 'A' AS middle_name, 'Doe' AS last_name, 1 AS name_suffix,
            TO_DATE('1985-05-12', 'YYYY-MM-DD') AS birthdate, CAST(NULL AS DATE) AS dt_of_death,
            'john.doe@email.com' AS email_addr, 20 AS sex, 21 AS mar_status, '555-0100' AS phone, 1 AS phone_type,
            '123 Main St' AS address1, CAST(NULL AS VARCHAR2(100)) AS address2, CAST(NULL AS VARCHAR2(100)) AS address3, CAST(NULL AS VARCHAR2(100)) AS address4,
            'Austin' AS city, 'Travis' AS county, 'TX' AS state, '78701' AS postal, 'USA' AS country
         FROM dual
         UNION ALL
         -- Test Case 2: adding DoD 
         SELECT 
            (SELECT csm_pen_id FROM perf.ps_csm_mbr_demogr WHERE csm_death_dt IS NULL AND rownum = 1) AS pid, 
            'Jane' AS first_name, CAST(NULL AS VARCHAR2(100)) AS middle_name, 'Smith' AS last_name, CAST(NULL AS NUMBER) AS name_suffix,
            TO_DATE('1940-11-23', 'YYYY-MM-DD') AS birthdate, TO_DATE('2024-02-15', 'YYYY-MM-DD') AS dt_of_death,
            'jane.smith@email.com' AS email_addr, 21 AS sex, 25 AS mar_status, '555-0200' AS phone, 1 AS phone_type,
            '456 Oak Ave' AS address1, 'Apt 2B' AS address2, CAST(NULL AS VARCHAR2(100)) AS address3, CAST(NULL AS VARCHAR2(100)) AS address4,
            'Chicago' AS city, 'Cook' AS county, 'IL' AS state, '60601' AS postal, 'USA' AS country
         FROM dual
         UNION ALL
         -- Test Case 3: Minimal Record (testing default parameters)
         SELECT 
            '003003003' AS pid, 'Alex' AS first_name, CAST(NULL AS VARCHAR2(100)) AS middle_name, 'Jones' AS last_name, CAST(NULL AS NUMBER) AS name_suffix,
            CAST(NULL AS DATE) AS birthdate, CAST(NULL AS DATE) AS dt_of_death,
            CAST(NULL AS VARCHAR2(100)) AS email_addr, CAST(NULL AS NUMBER) AS sex, CAST(NULL AS NUMBER) AS mar_status, CAST(NULL AS VARCHAR2(100)) AS phone, CAST(NULL AS NUMBER) AS phone_type,
            CAST(NULL AS VARCHAR2(100)) AS address1, CAST(NULL AS VARCHAR2(100)) AS address2, CAST(NULL AS VARCHAR2(100)) AS address3, CAST(NULL AS VARCHAR2(100)) AS address4,
            CAST(NULL AS VARCHAR2(100)) AS city, CAST(NULL AS VARCHAR2(100)) AS county, CAST(NULL AS VARCHAR2(100)) AS state, CAST(NULL AS VARCHAR2(100)) AS postal, CAST(NULL AS VARCHAR2(100)) AS country
         FROM dual
         UNION ALL
         -- Test Case 4: Changing new fields for found people
         SELECT 
            v.PID, v.FIRST_NAME, v.MIDDLE_NAME, v.LAST_NAME, v.NAME_SUFFIX_ID AS NAME_SUFFIX,
            v.BIRTHDATE, v.DT_OF_DEATH,
            'test4-changed-data@example.com' AS EMAIL_ADDR,
            v.GENDER_ID AS SEX, v.MAR_STATUS_ID AS MAR_STATUS,
            '1112223344' AS PHONE, v.PHONE_TYPE_ID AS PHONE_TYPE,
            v.ADDRESS1, v.ADDRESS2, v.ADDRESS3,
            'ToWhomItMayConcern' AS ADDRESS4, v.CITY,
            'COOK' AS COUNTY, v.STATE, v.POSTAL, v.COUNTRY
         FROM perf.mq_vw v
         WHERE rownum <= 3
            AND NVL(v.email_addr, 'null') != 'test4-changed-data@example.com'
            AND NVL(v.county, 'null') != 'COOK'
            AND NVL(v.phone, 'null') != '1112223344'
            AND NVL(v.address4, 'null') != 'ToWhomItMayConcern'
      )
      SELECT * FROM mock_rows;

   -- Cursor to fetch details from mq_vw for a given pid
   CURSOR c_vw_details(p_pid VARCHAR2) IS
      SELECT *
        FROM perf.mq_vw
       WHERE pid = p_pid;

   -- Variables for dynamic test setup
   v_pid_1 VARCHAR2(12);
   v_pid_2 VARCHAR2(12);
   v_pid_3 VARCHAR2(12);

   -- 2. Local nested subprograms MUST come dead-last in the declaration section
   FUNCTION assert_source (
      i_pid            IN VARCHAR2,
      i_field          IN VARCHAR2,
      i_expected_value IN VARCHAR2
   ) RETURN BOOLEAN 
   IS
      v_sql_stmt    VARCHAR2(1000);
      v_found_value VARCHAR2(4000);
   BEGIN
      v_sql_stmt := 'SELECT TO_CHAR(' || dbms_assert.enquote_name(i_field) || ') 
                      FROM perf.mq_vw 
                      WHERE pid = :1 AND rownum <= 1';

      BEGIN
          EXECUTE IMMEDIATE v_sql_stmt INTO v_found_value USING i_pid;
      EXCEPTION
          WHEN no_data_found THEN
              v_found_value := NULL;
      END;

      IF NVL(i_expected_value, '###NULL###') = NVL(v_found_value, '###NULL###') THEN
          RETURN TRUE;
      ELSE
          dbms_output.put_line('Assertion Failed for '||i_field||'. Expected: '||i_expected_value||', Got: '||v_found_value);
          RETURN FALSE;
      END IF;
   END assert_source;

   PROCEDURE assert_source_val(
      i_pid            IN VARCHAR2,
      i_field          IN VARCHAR2,
      i_expected_value IN VARCHAR2
   ) IS
   BEGIN
      IF NOT assert_source(i_pid, i_field, i_expected_value) THEN
         raise_application_error(-20502, 'ASSERTION FAILED: Field ' || i_field || ' for PID ' || i_pid || ' did not match expected value: ' || NVL(i_expected_value, 'NULL'));
      ELSE
         dbms_output.put_line('PASS [PID: ' || i_pid || ']: Field ' || i_field || ' matched expected value: ' || NVL(i_expected_value, 'NULL'));
      END IF;
   END assert_source_val;

   PROCEDURE assert_status(p_pid IN VARCHAR2, p_code IN NUMBER) 
   IS
      v_meaning VARCHAR2(50);
   BEGIN
      v_meaning := CASE p_code
          WHEN 200 THEN '200=OK'
          WHEN 202 THEN '202=Accepted'
          WHEN 304 THEN '304=No Change'
          WHEN 404 THEN '404=Not Found'
          WHEN 500 THEN '500=Error'
          ELSE 'UNKNOWN CODE: ' || TO_CHAR(p_code)
      END;

      IF p_code = 500 THEN
          raise_application_error(-20500, 'ASSERTION FAILED: Systemic 500 error encountered for PID ' || p_pid);
      ELSIF p_code NOT IN (200, 202, 304, 404) THEN
          raise_application_error(-20501, 'ASSERTION FAILED: Unexpected status returned for PID ' || p_pid || ' (' || v_meaning || ')');
      ELSE
          dbms_output.put_line('PASS [PID: ' || p_pid || ']: Processed successfully -> ' || v_meaning);
      END IF;
   END assert_status;

   PROCEDURE assert_actual_status(
      p_pid           IN VARCHAR2, 
      p_expected_code IN NUMBER, 
      p_actual_code   IN NUMBER
   ) IS
      v_expected_meaning VARCHAR2(50);
      v_actual_meaning   VARCHAR2(50);
      
      FUNCTION get_meaning(p_code IN NUMBER) RETURN VARCHAR2 IS
      BEGIN
         RETURN CASE p_code
             WHEN 200 THEN '200=OK'
             WHEN 202 THEN '202=Accepted'
             WHEN 304 THEN '304=No Change'
             WHEN 404 THEN '404=Not Found'
             WHEN 500 THEN '500=Error'
             ELSE 'UNKNOWN CODE: ' || TO_CHAR(p_code)
         END;
      END get_meaning;
   BEGIN
      v_expected_meaning := get_meaning(p_expected_code);
      v_actual_meaning := get_meaning(p_actual_code);
      
      IF p_expected_code = p_actual_code THEN
         dbms_output.put_line('PASS [PID: ' || p_pid || ']: Status code matched expected (' || v_expected_meaning || ')');
      ELSE
         raise_application_error(-20503, 'ASSERTION FAILED: Status code mismatch for PID ' || p_pid || '. Expected: ' || v_expected_meaning || ', Got: ' || v_actual_meaning);
      END IF;
   END assert_actual_status;

   PROCEDURE test_mq_insert_census_mode (
      i_pid             IN VARCHAR2,
      i_first_name      IN VARCHAR2,
      i_last_name       IN VARCHAR2
   ) IS
      v_resp            NUMBER;
      v_actual_status   NUMBER;
      v_census_first    VARCHAR2(64);
   BEGIN
      dbms_output.put_line('--- Running test_mq_insert_census_mode ---');
      
      -- Test with i_block_census_mode => true
      DELETE FROM perf.ps_csm_voya_census WHERE csm_pen_id = i_pid;
      INSERT INTO perf.ps_csm_voya_census (csm_pen_id, csm_first_nm, csm_last_nm)
      VALUES (i_pid, 'OldFirst', 'OldLast');
      
      perf.mq_pkg.mq_insert (
          pid                 => i_pid,
          first_name          => i_first_name,
          last_name           => i_last_name,
          response            => v_resp,
          i_block_census_mode => true
      );
      
      IF v_resp != 0 THEN
         raise_application_error(-20504, 'mq_insert returned failure response');
      END IF;
      
      SELECT mq_status INTO v_actual_status
        FROM (SELECT mq_status FROM perf.mq_inbound WHERE pid = i_pid ORDER BY mq_id DESC)
       WHERE rownum = 1;
      
      assert_actual_status(i_pid, 200, v_actual_status);
      
      -- Verify census was updated by census_sentinel
      SELECT csm_first_nm INTO v_census_first
        FROM perf.ps_csm_voya_census
       WHERE csm_pen_id = i_pid;
       
      IF NVL(v_census_first, 'NULL') != i_first_name THEN
         raise_application_error(-20506, 'Census test failed: Expected census first name to be ' || i_first_name || ', got ' || NVL(v_census_first, 'NULL'));
      ELSE
         dbms_output.put_line('PASS: Census table updated correctly when block_census_mode is true.');
      END IF;
      
      -- Test with i_block_census_mode => false
      DELETE FROM perf.ps_csm_voya_census WHERE csm_pen_id = i_pid;
      INSERT INTO perf.ps_csm_voya_census (csm_pen_id, csm_first_nm, csm_last_nm)
      VALUES (i_pid, 'OldFirst', 'OldLast');
      
      perf.mq_pkg.mq_insert (
          pid                 => i_pid,
          first_name          => i_first_name || 'NoChange',
          last_name           => i_last_name,
          response            => v_resp,
          i_block_census_mode => false
      );
      
      SELECT csm_first_nm INTO v_census_first
        FROM perf.ps_csm_voya_census
       WHERE csm_pen_id = i_pid;
       
      IF NVL(v_census_first, 'NULL') = i_first_name || 'NoChange' THEN
         raise_application_error(-20507, 'Census test failed: Census table should NOT have been updated when block_census_mode is false.');
      ELSE
         dbms_output.put_line('PASS: Census table remained unchanged when block_census_mode is false.');
      END IF;
   END test_mq_insert_census_mode;

   PROCEDURE test_process_mq_inbound (
      i_pid_1 IN VARCHAR2,
      i_pid_2 IN VARCHAR2
   ) IS
      v_vw_rec_1 c_vw_details%ROWTYPE;
      v_vw_rec_2 c_vw_details%ROWTYPE;
      v_mq_id_1 NUMBER;
      v_mq_id_2 NUMBER;
      v_mq_id_3 NUMBER;
      v_status_1 NUMBER;
      v_status_2 NUMBER;
      v_status_3 NUMBER;
   BEGIN
      dbms_output.put_line('--- Running test_process_mq_inbound (Bulk test 1) ---');
      DELETE FROM perf.mq_inbound WHERE mq_status = 202;
      
      -- Fetch current state to construct deterministic mock updates
      OPEN c_vw_details(i_pid_1);
      FETCH c_vw_details INTO v_vw_rec_1;
      CLOSE c_vw_details;
      
      OPEN c_vw_details(i_pid_2);
      FETCH c_vw_details INTO v_vw_rec_2;
      CLOSE c_vw_details;
      
      -- 1. Row to update (status 200)
      INSERT INTO perf.mq_inbound (
         pid, first_name, middle_name, last_name, name_suffix_id,
         birthdate, dt_of_death, email_addr, gender_id, mar_status_id,
         phone, phone_type_id,
         address1, address2, address3, address4, city, county, state, postal, country,
         mq_status
      ) VALUES (
         v_vw_rec_1.pid, v_vw_rec_1.first_name || 'Bulk', v_vw_rec_1.middle_name, v_vw_rec_1.last_name, v_vw_rec_1.name_suffix_id,
         v_vw_rec_1.birthdate, v_vw_rec_1.dt_of_death, v_vw_rec_1.email_addr, v_vw_rec_1.gender_id, v_vw_rec_1.mar_status_id,
         v_vw_rec_1.phone, v_vw_rec_1.phone_type_id,
         v_vw_rec_1.address1, v_vw_rec_1.address2, v_vw_rec_1.address3, v_vw_rec_1.address4, v_vw_rec_1.city, v_vw_rec_1.county, v_vw_rec_1.state, v_vw_rec_1.postal, v_vw_rec_1.country,
         202
      ) RETURNING mq_id INTO v_mq_id_1;
      
      -- 2. Row with no changes (status 304)
      INSERT INTO perf.mq_inbound (
         pid, first_name, middle_name, last_name, name_suffix_id,
         birthdate, dt_of_death, email_addr, gender_id, mar_status_id,
         phone, phone_type_id,
         address1, address2, address3, address4, city, county, state, postal, country,
         mq_status
      ) VALUES (
         v_vw_rec_2.pid, v_vw_rec_2.first_name, v_vw_rec_2.middle_name, v_vw_rec_2.last_name, v_vw_rec_2.name_suffix_id,
         v_vw_rec_2.birthdate, v_vw_rec_2.dt_of_death, v_vw_rec_2.email_addr, v_vw_rec_2.gender_id, v_vw_rec_2.mar_status_id,
         v_vw_rec_2.phone, v_vw_rec_2.phone_type_id,
         v_vw_rec_2.address1, v_vw_rec_2.address2, v_vw_rec_2.address3, v_vw_rec_2.address4, v_vw_rec_2.city, v_vw_rec_2.county, v_vw_rec_2.state, v_vw_rec_2.postal, v_vw_rec_2.country,
         202
      ) RETURNING mq_id INTO v_mq_id_2;
      
      -- 3. Row with invalid/non-existent pid (status 404)
      INSERT INTO perf.mq_inbound (pid, first_name, last_name, mq_status)
      VALUES ('999999999999', 'NonExist', 'Person', 202)
      RETURNING mq_id INTO v_mq_id_3;
      
      perf.mq_pkg.process_mq_inbound (
         i_starting_mq_id => LEAST(v_mq_id_1, v_mq_id_2, v_mq_id_3),
         i_status => 202
      );
      
      SELECT mq_status INTO v_status_1 FROM perf.mq_inbound WHERE mq_id = v_mq_id_1;
      SELECT mq_status INTO v_status_2 FROM perf.mq_inbound WHERE mq_id = v_mq_id_2;
      SELECT mq_status INTO v_status_3 FROM perf.mq_inbound WHERE mq_id = v_mq_id_3;
      
      assert_actual_status(i_pid_1, 200, v_status_1);
      assert_actual_status(i_pid_2, 304, v_status_2);
      assert_actual_status('999999999999', 404, v_status_3);
      
      -- Verify the update took effect in the source view for Row 1
      assert_source_val(i_pid_1, 'first_name', v_vw_rec_1.first_name || 'Bulk');
   END test_process_mq_inbound;

   PROCEDURE test_process_mq_inbound_max_records (
      i_pid_1 IN VARCHAR2,
      i_pid_2 IN VARCHAR2,
      i_pid_3 IN VARCHAR2
   ) IS
      v_vw_rec_1 c_vw_details%ROWTYPE;
      v_vw_rec_2 c_vw_details%ROWTYPE;
      v_vw_rec_3 c_vw_details%ROWTYPE;
      v_mq_id_1 NUMBER;
      v_mq_id_2 NUMBER;
      v_mq_id_3 NUMBER;
      v_status_1 NUMBER;
      v_status_2 NUMBER;
      v_status_3 NUMBER;
   BEGIN
      dbms_output.put_line('--- Running test_process_mq_inbound_max_records ---');
      DELETE FROM perf.mq_inbound WHERE mq_status = 202;
      
      OPEN c_vw_details(i_pid_1);
      FETCH c_vw_details INTO v_vw_rec_1;
      CLOSE c_vw_details;
      
      OPEN c_vw_details(i_pid_2);
      FETCH c_vw_details INTO v_vw_rec_2;
      CLOSE c_vw_details;
      
      OPEN c_vw_details(i_pid_3);
      FETCH c_vw_details INTO v_vw_rec_3;
      CLOSE c_vw_details;
      
      INSERT INTO perf.mq_inbound (
         pid, first_name, middle_name, last_name, name_suffix_id,
         birthdate, dt_of_death, email_addr, gender_id, mar_status_id,
         phone, phone_type_id,
         address1, address2, address3, address4, city, county, state, postal, country,
         mq_status
      ) VALUES (
         v_vw_rec_1.pid, v_vw_rec_1.first_name || 'Max1', v_vw_rec_1.middle_name, v_vw_rec_1.last_name, v_vw_rec_1.name_suffix_id,
         v_vw_rec_1.birthdate, v_vw_rec_1.dt_of_death, v_vw_rec_1.email_addr, v_vw_rec_1.gender_id, v_vw_rec_1.mar_status_id,
         v_vw_rec_1.phone, v_vw_rec_1.phone_type_id,
         v_vw_rec_1.address1, v_vw_rec_1.address2, v_vw_rec_1.address3, v_vw_rec_1.address4, v_vw_rec_1.city, v_vw_rec_1.county, v_vw_rec_1.state, v_vw_rec_1.postal, v_vw_rec_1.country,
         202
      ) RETURNING mq_id INTO v_mq_id_1;
      
      INSERT INTO perf.mq_inbound (
         pid, first_name, middle_name, last_name, name_suffix_id,
         birthdate, dt_of_death, email_addr, gender_id, mar_status_id,
         phone, phone_type_id,
         address1, address2, address3, address4, city, county, state, postal, country,
         mq_status
      ) VALUES (
         v_vw_rec_2.pid, v_vw_rec_2.first_name || 'Max2', v_vw_rec_2.middle_name, v_vw_rec_2.last_name, v_vw_rec_2.name_suffix_id,
         v_vw_rec_2.birthdate, v_vw_rec_2.dt_of_death, v_vw_rec_2.email_addr, v_vw_rec_2.gender_id, v_vw_rec_2.mar_status_id,
         v_vw_rec_2.phone, v_vw_rec_2.phone_type_id,
         v_vw_rec_2.address1, v_vw_rec_2.address2, v_vw_rec_2.address3, v_vw_rec_2.address4, v_vw_rec_2.city, v_vw_rec_2.county, v_vw_rec_2.state, v_vw_rec_2.postal, v_vw_rec_2.country,
         202
      ) RETURNING mq_id INTO v_mq_id_2;
      
      INSERT INTO perf.mq_inbound (
         pid, first_name, middle_name, last_name, name_suffix_id,
         birthdate, dt_of_death, email_addr, gender_id, mar_status_id,
         phone, phone_type_id,
         address1, address2, address3, address4, city, county, state, postal, country,
         mq_status
      ) VALUES (
         v_vw_rec_3.pid, v_vw_rec_3.first_name || 'Max3', v_vw_rec_3.middle_name, v_vw_rec_3.last_name, v_vw_rec_3.name_suffix_id,
         v_vw_rec_3.birthdate, v_vw_rec_3.dt_of_death, v_vw_rec_3.email_addr, v_vw_rec_3.gender_id, v_vw_rec_3.mar_status_id,
         v_vw_rec_3.phone, v_vw_rec_3.phone_type_id,
         v_vw_rec_3.address1, v_vw_rec_3.address2, v_vw_rec_3.address3, v_vw_rec_3.address4, v_vw_rec_3.city, v_vw_rec_3.county, v_vw_rec_3.state, v_vw_rec_3.postal, v_vw_rec_3.country,
         202
      ) RETURNING mq_id INTO v_mq_id_3;
      
      -- Call with max_records => 2. Only first 2 should process, third remains 202
      perf.mq_pkg.process_mq_inbound (
         i_starting_mq_id => LEAST(v_mq_id_1, v_mq_id_2, v_mq_id_3),
         i_status => 202,
         i_max_records => 2
      );
      
      SELECT mq_status INTO v_status_1 FROM perf.mq_inbound WHERE mq_id = v_mq_id_1;
      SELECT mq_status INTO v_status_2 FROM perf.mq_inbound WHERE mq_id = v_mq_id_2;
      SELECT mq_status INTO v_status_3 FROM perf.mq_inbound WHERE mq_id = v_mq_id_3;
      
      IF v_status_1 = 202 OR v_status_2 = 202 THEN
         raise_application_error(-20520, 'Max records test failed: expected first two rows to be processed.');
      END IF;
      
      IF v_status_3 != 202 THEN
         raise_application_error(-20521, 'Max records test failed: expected third row to remain at 202, got ' || v_status_3);
      ELSE
         dbms_output.put_line('PASS: Third row remained 202 as expected (throttled).');
      END IF;
   END test_process_mq_inbound_max_records;

   PROCEDURE test_process_mq_inbound_chunk_size (
      i_pid_1 IN VARCHAR2,
      i_pid_2 IN VARCHAR2
   ) IS
      v_vw_rec_1 c_vw_details%ROWTYPE;
      v_vw_rec_2 c_vw_details%ROWTYPE;
      v_mq_id_1 NUMBER;
      v_mq_id_2 NUMBER;
      v_status_1 NUMBER;
      v_status_2 NUMBER;
   BEGIN
      dbms_output.put_line('--- Running test_process_mq_inbound_chunk_size ---');
      DELETE FROM perf.mq_inbound WHERE mq_status = 202;
      
      OPEN c_vw_details(i_pid_1);
      FETCH c_vw_details INTO v_vw_rec_1;
      CLOSE c_vw_details;
      
      OPEN c_vw_details(i_pid_2);
      FETCH c_vw_details INTO v_vw_rec_2;
      CLOSE c_vw_details;
      
      INSERT INTO perf.mq_inbound (
         pid, first_name, middle_name, last_name, name_suffix_id,
         birthdate, dt_of_death, email_addr, gender_id, mar_status_id,
         phone, phone_type_id,
         address1, address2, address3, address4, city, county, state, postal, country,
         mq_status
      ) VALUES (
         v_vw_rec_1.pid, v_vw_rec_1.first_name || 'Chnk1', v_vw_rec_1.middle_name, v_vw_rec_1.last_name, v_vw_rec_1.name_suffix_id,
         v_vw_rec_1.birthdate, v_vw_rec_1.dt_of_death, v_vw_rec_1.email_addr, v_vw_rec_1.gender_id, v_vw_rec_1.mar_status_id,
         v_vw_rec_1.phone, v_vw_rec_1.phone_type_id,
         v_vw_rec_1.address1, v_vw_rec_1.address2, v_vw_rec_1.address3, v_vw_rec_1.address4, v_vw_rec_1.city, v_vw_rec_1.county, v_vw_rec_1.state, v_vw_rec_1.postal, v_vw_rec_1.country,
         202
      ) RETURNING mq_id INTO v_mq_id_1;
      
      INSERT INTO perf.mq_inbound (
         pid, first_name, middle_name, last_name, name_suffix_id,
         birthdate, dt_of_death, email_addr, gender_id, mar_status_id,
         phone, phone_type_id,
         address1, address2, address3, address4, city, county, state, postal, country,
         mq_status
      ) VALUES (
         v_vw_rec_2.pid, v_vw_rec_2.first_name || 'Chnk2', v_vw_rec_2.middle_name, v_vw_rec_2.last_name, v_vw_rec_2.name_suffix_id,
         v_vw_rec_2.birthdate, v_vw_rec_2.dt_of_death, v_vw_rec_2.email_addr, v_vw_rec_2.gender_id, v_vw_rec_2.mar_status_id,
         v_vw_rec_2.phone, v_vw_rec_2.phone_type_id,
         v_vw_rec_2.address1, v_vw_rec_2.address2, v_vw_rec_2.address3, v_vw_rec_2.address4, v_vw_rec_2.city, v_vw_rec_2.county, v_vw_rec_2.state, v_vw_rec_2.postal, v_vw_rec_2.country,
         202
      ) RETURNING mq_id INTO v_mq_id_2;
      
      -- Process with commit size (chunk size) = 1
      perf.mq_pkg.process_mq_inbound (
         i_starting_mq_id => LEAST(v_mq_id_1, v_mq_id_2),
         i_status => 202,
         i_commit_size => 1
      );
      
      SELECT mq_status INTO v_status_1 FROM perf.mq_inbound WHERE mq_id = v_mq_id_1;
      SELECT mq_status INTO v_status_2 FROM perf.mq_inbound WHERE mq_id = v_mq_id_2;
      
      IF v_status_1 = 202 OR v_status_2 = 202 THEN
         raise_application_error(-20530, 'Chunk size test failed: expected both rows to be processed.');
      ELSE
         dbms_output.put_line('PASS: Both rows processed successfully in chunks of 1.');
      END IF;
   END test_process_mq_inbound_chunk_size;

   PROCEDURE test_process_mq_inbound_fallback (
      i_pid_1 IN VARCHAR2,
      i_pid_2 IN VARCHAR2,
      i_pid_3 IN VARCHAR2
   ) IS
      v_vw_rec_1 c_vw_details%ROWTYPE;
      v_vw_rec_2 c_vw_details%ROWTYPE;
      v_vw_rec_3 c_vw_details%ROWTYPE;
      v_mq_id_1 NUMBER;
      v_mq_id_2 NUMBER;
      v_mq_id_3 NUMBER;
      v_status_1 NUMBER;
      v_status_2 NUMBER;
      v_status_3 NUMBER;
      v_log_count NUMBER;
   BEGIN
      dbms_output.put_line('--- Running test_process_mq_inbound_fallback ---');
      DELETE FROM perf.mq_inbound WHERE mq_status = 202;
      
      OPEN c_vw_details(i_pid_1);
      FETCH c_vw_details INTO v_vw_rec_1;
      CLOSE c_vw_details;
      
      OPEN c_vw_details(i_pid_2);
      FETCH c_vw_details INTO v_vw_rec_2;
      CLOSE c_vw_details;
      
      OPEN c_vw_details(i_pid_3);
      FETCH c_vw_details INTO v_vw_rec_3;
      CLOSE c_vw_details;
      
      -- 1. Good row
      INSERT INTO perf.mq_inbound (
         pid, first_name, middle_name, last_name, name_suffix_id,
         birthdate, dt_of_death, email_addr, gender_id, mar_status_id,
         phone, phone_type_id,
         address1, address2, address3, address4, city, county, state, postal, country,
         mq_status
      ) VALUES (
         v_vw_rec_1.pid, v_vw_rec_1.first_name || 'Fbak1', v_vw_rec_1.middle_name, v_vw_rec_1.last_name, v_vw_rec_1.name_suffix_id,
         v_vw_rec_1.birthdate, v_vw_rec_1.dt_of_death, v_vw_rec_1.email_addr, v_vw_rec_1.gender_id, v_vw_rec_1.mar_status_id,
         v_vw_rec_1.phone, v_vw_rec_1.phone_type_id,
         v_vw_rec_1.address1, v_vw_rec_1.address2, v_vw_rec_1.address3, v_vw_rec_1.address4, v_vw_rec_1.city, v_vw_rec_1.county, v_vw_rec_1.state, v_vw_rec_1.postal, v_vw_rec_1.country,
         202
      ) RETURNING mq_id INTO v_mq_id_1;
      
      -- 2. Bad row (causes exception due to 100 character first name violating column constraint)
      INSERT INTO perf.mq_inbound (
         pid, first_name, middle_name, last_name, name_suffix_id,
         birthdate, dt_of_death, email_addr, gender_id, mar_status_id,
         phone, phone_type_id,
         address1, address2, address3, address4, city, county, state, postal, country,
         mq_status
      ) VALUES (
         v_vw_rec_2.pid, RPAD('B', 100, 'B'), v_vw_rec_2.middle_name, v_vw_rec_2.last_name, v_vw_rec_2.name_suffix_id,
         v_vw_rec_2.birthdate, v_vw_rec_2.dt_of_death, v_vw_rec_2.email_addr, v_vw_rec_2.gender_id, v_vw_rec_2.mar_status_id,
         v_vw_rec_2.phone, v_vw_rec_2.phone_type_id,
         v_vw_rec_2.address1, v_vw_rec_2.address2, v_vw_rec_2.address3, v_vw_rec_2.address4, v_vw_rec_2.city, v_vw_rec_2.county, v_vw_rec_2.state, v_vw_rec_2.postal, v_vw_rec_2.country,
         202
      ) RETURNING mq_id INTO v_mq_id_2;
      
      -- 3. Good row
      INSERT INTO perf.mq_inbound (
         pid, first_name, middle_name, last_name, name_suffix_id,
         birthdate, dt_of_death, email_addr, gender_id, mar_status_id,
         phone, phone_type_id,
         address1, address2, address3, address4, city, county, state, postal, country,
         mq_status
      ) VALUES (
         v_vw_rec_3.pid, v_vw_rec_3.first_name || 'Fbak3', v_vw_rec_3.middle_name, v_vw_rec_3.last_name, v_vw_rec_3.name_suffix_id,
         v_vw_rec_3.birthdate, v_vw_rec_3.dt_of_death, v_vw_rec_3.email_addr, v_vw_rec_3.gender_id, v_vw_rec_3.mar_status_id,
         v_vw_rec_3.phone, v_vw_rec_3.phone_type_id,
         v_vw_rec_3.address1, v_vw_rec_3.address2, v_vw_rec_3.address3, v_vw_rec_3.address4, v_vw_rec_3.city, v_vw_rec_3.county, v_vw_rec_3.state, v_vw_rec_3.postal, v_vw_rec_3.country,
         202
      ) RETURNING mq_id INTO v_mq_id_3;
      
      -- Clean up old logs for these mq_ids
      DELETE FROM perf.mq_pkg_log WHERE mq_id IN (v_mq_id_1, v_mq_id_2, v_mq_id_3);
      
      -- Run with allow partial success => true
      perf.mq_pkg.process_mq_inbound (
         i_starting_mq_id        => LEAST(v_mq_id_1, v_mq_id_2, v_mq_id_3),
         i_status                => 202,
         i_allow_partial_success => true
      );
      
      SELECT mq_status INTO v_status_1 FROM perf.mq_inbound WHERE mq_id = v_mq_id_1;
      SELECT mq_status INTO v_status_2 FROM perf.mq_inbound WHERE mq_id = v_mq_id_2;
      SELECT mq_status INTO v_status_3 FROM perf.mq_inbound WHERE mq_id = v_mq_id_3;
      
      IF v_status_1 = 202 OR v_status_1 = 500 THEN
         raise_application_error(-20540, 'Fallback test failed: Row 1 should have succeeded, got status ' || v_status_1);
      END IF;
      
      IF v_status_2 != 500 THEN
         raise_application_error(-20541, 'Fallback test failed: Row 2 should have failed with status 500, got ' || v_status_2);
      END IF;
      
      IF v_status_3 = 202 OR v_status_3 = 500 THEN
         raise_application_error(-20542, 'Fallback test failed: Row 3 should have succeeded, got status ' || v_status_3);
      END IF;
      
      -- Check that an autonomous transaction logged the error in mq_pkg_log
      SELECT COUNT(*) INTO v_log_count 
        FROM perf.mq_pkg_log 
       WHERE mq_id = v_mq_id_2 
         AND error_code IS NOT NULL;
         
      IF v_log_count = 0 THEN
         raise_application_error(-20543, 'Fallback test failed: No log entry found in mq_pkg_log for failed mq_id ' || v_mq_id_2);
      ELSE
         dbms_output.put_line('PASS: Fallback to row-by-row worked. Good rows processed, bad row got 500 and logged.');
      END IF;
   END test_process_mq_inbound_fallback;

-- 3. Execution Section Begin
BEGIN
   -- 1. Server name check
   SELECT lower(NVL(sys_context('USERENV', 'SERVER_HOST'), 'dev')) INTO v_server_name FROM dual;
   
   IF instr(v_server_name, 'dev') = 0 THEN
      dbms_output.put_line('Non-dev host detected (' || v_server_name || '). Skipping execution safely.');
      RAISE e_not_dev_exception;
   END IF;

   -- 2. Fetch existing PIDs dynamically
   DECLARE
      CURSOR c_setup_pids IS
         SELECT pid
           FROM perf.mq_vw
          WHERE rownum <= 3;
      
      TYPE t_setup_rec IS RECORD (
         pid        VARCHAR2(12)
      );
      TYPE t_setup_pids IS TABLE OF t_setup_rec INDEX BY PLS_INTEGER;
      v_setup_pids t_setup_pids;
   BEGIN
      OPEN c_setup_pids;
      FETCH c_setup_pids BULK COLLECT INTO v_setup_pids;
      CLOSE c_setup_pids;
      
      IF v_setup_pids.COUNT < 3 THEN
         raise_application_error(-20600, 'Test setup failed: Need at least 3 records in perf.mq_vw to run tests, found ' || v_setup_pids.COUNT);
      END IF;
      
      v_pid_1 := v_setup_pids(1).pid;
      v_pid_2 := v_setup_pids(2).pid;
      v_pid_3 := v_setup_pids(3).pid;
   END;

   -- 3. Execute legacy tests (with corrected status assertion)
   dbms_output.put_line('=== Executing Legacy Inserts Loop ===');
   FOR r IN c_mock_data LOOP
      dbms_output.put_line('Attempting mq_insert for PID: ' || r.pid);
      
      perf.mq_pkg.mq_insert (
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
      DECLARE
         v_actual_status NUMBER;
      BEGIN
         IF v_response != 0 THEN
            raise_application_error(-20504, 'ASSERTION FAILED: mq_insert returned failure response code: ' || v_response);
         END IF;
         
         SELECT mq_status 
           INTO v_actual_status 
           FROM (
              SELECT mq_status 
                FROM perf.mq_inbound 
               WHERE pid = r.pid 
               ORDER BY mq_id DESC
           ) 
          WHERE rownum = 1;
          
         assert_status(p_pid => r.pid, p_code => v_actual_status);
      EXCEPTION
         WHEN no_data_found THEN
            raise_application_error(-20505, 'ASSERTION FAILED: No record found in mq_inbound for PID: ' || r.pid);
      END;
      dbms_output.put_line('Success mq_insert completion for PID: ' || r.pid);
   END LOOP;

   -- 4. Execute new unit tests
   dbms_output.put_line('=== Executing New Unit Tests ===');
   
   test_mq_insert_census_mode(v_pid_2, 'NewCensusFirst', 'NewCensusLast');
   
   test_process_mq_inbound(v_pid_1, v_pid_2);
   
   test_process_mq_inbound_max_records(v_pid_1, v_pid_2, v_pid_3);
   
   test_process_mq_inbound_chunk_size(v_pid_1, v_pid_2);
   
   test_process_mq_inbound_fallback(v_pid_1, v_pid_2, v_pid_3);
   
   dbms_output.put_line('Success: All legacy and new test cases evaluated successfully.');

EXCEPTION
   WHEN e_not_dev_exception THEN 
      NULL; -- Exits quietly without flagging a failure trace
   WHEN others THEN
      ROLLBACK;
      RAISE;
END test_mq_pkg;
/
