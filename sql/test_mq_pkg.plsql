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
         -- Test Case 4: Changing new fields for found people (Fixed missing schema/table alias qualifications)
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

    FUNCTION assert_source (
            i_pid            IN VARCHAR2,
            i_field          IN VARCHAR2,
            i_expected_value IN VARCHAR2 -- Pass your expected value as a string for generic mapping
            ) RETURN BOOLEAN 
            IS
            v_sql_stmt    VARCHAR2(1000);
            v_found_value VARCHAR2(4000);
            BEGIN
            -- 1. Construct dynamic SQL safely using system identifiers
            -- We wrap the targeted dynamic column in TO_CHAR to uniformly handle Dates, Numbers, and Strings
            v_sql_stmt := 'SELECT TO_CHAR(' || dbms_assert.enquote_name(i_field) || ') 
                            FROM perf.mq_vw 
                            WHERE pid = :1 AND rownum <= 1';

            -- 2. Execute the query dynamically, binding the i_pid parameter securely
            BEGIN
                EXECUTE IMMEDIATE v_sql_stmt INTO v_found_value USING i_pid;
            EXCEPTION
                WHEN no_data_found THEN
                    v_found_value := NULL;
            END;

            -- 3. Perform the assertion validation comparison check
            -- Handles NULL-safe comparisons cleanly using NVL loops
            IF NVL(i_expected_value, '###NULL###') = NVL(v_found_value, '###NULL###') THEN
                RETURN TRUE;
            ELSE
                -- Optional diagnostic tracing for easier test debugging
                dbms_output.put_line('Assertion Failed for '||i_field||'. Expected: '||i_expected_value||', Got: '||v_found_value);
                RETURN FALSE;
            END IF;
            
    END assert_source;


   -- 2. Local nested subprograms MUST come dead-last in the declaration section
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

-- 3. Execution Section Begin
BEGIN
   -- Fixed variable assignment target to match declaration
   SELECT lower(NVL(sys_context('USERENV', 'SERVER_HOST'), 'dev')) INTO v_server_name FROM dual;
   
   IF instr(v_server_name, 'dev') = 0 THEN
      dbms_output.put_line('Non-dev host detected (' || v_server_name || '). Skipping execution safely.');
      RAISE e_not_dev_exception; -- Fixed exception name rule mismatch
   END IF;

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

      -- Validating status_code using our fixed assertion rules
      assert_status(p_pid => r.pid, p_code => v_response);
      dbms_output.put_line('Success mq_insert completion for PID: ' || r.pid);

   END LOOP;
   
   dbms_output.put_line('Success: All test inserts evaluated successfully.');

EXCEPTION
   WHEN e_not_dev_exception THEN 
      NULL; -- Exits quietly without flagging a failure trace
   WHEN others THEN
      ROLLBACK;
      RAISE;
END test_mq_pkg;
/
