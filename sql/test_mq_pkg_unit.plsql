set serveroutput on size unlimited
set define off
/**************************************************************************
   Unit tests for perf.mq_pkg -- dev-only

   single-record path
      1. mq_insert(...)
      2. mqi_constructor -> 
      3. mqi_loader (stage) 
      4. process_batch
      5. (hydrate hashes + keys -> MERGE demo/addr/phone -> finalize
      status -> optional census sync)
      6. commit.
   
   bulk path:
      1. stage rows into perf.mq_inbound at mq_status = 202
      2. process_mq_inbound(...)
      3. process_batch
      4. (backup) with a row by row fallback if a row errors.

**************************************************************************/
declare
   g_owner constant varchar2(30) := 'PERF';
   c_pass  constant varchar2(6)  := 'PASS';

   e_not_dev exception;

   -- live fixture: three real, distinct, single-row pids from perf.mq_vw
   v_pid_1     perf.mq_inbound.pid%type;
   v_pid_2     perf.mq_inbound.pid%type;
   v_pid_3     perf.mq_inbound.pid%type;
   -- a <=9 char pid guaranteed absent from perf.mq_vw (dummy pids stay <=9)
   v_dummy_pid perf.mq_inbound.pid%type;

   -- every mq_inbound row this script creates, for surgical cleanup
   type t_num is table of number;
   g_created t_num := t_num();

   /**********************************************************************
       output + assertions
   **********************************************************************/
   procedure log (i_msg in varchar2) is
   begin
      dbms_output.put_line(i_msg);
   end log;

   procedure fail (i_msg in varchar2) is
   begin
      raise_application_error(-20900, i_msg);
   end fail;

   function nn (i_val in varchar2) return varchar2 is
   begin
      return nvl(i_val, '<NULL>');
   end nn;

   procedure assert_eq_num (
      i_what     in varchar2,
      i_expected in number,
      i_actual   in number
   ) is
   begin
      if nvl(i_actual, -987654321) = nvl(i_expected, -987654321) then
         log('   ' || c_pass || '  ' || i_what || ' = ' || nn(to_char(i_expected)));
      else
         fail(i_what || ': expected ' || nn(to_char(i_expected))
              || ' but got ' || nn(to_char(i_actual)));
      end if;
   end assert_eq_num;

   procedure assert_eq_str (
      i_what     in varchar2,
      i_expected in varchar2,
      i_actual   in varchar2
   ) is
   begin
      if nvl(i_actual, '##NULL##') = nvl(i_expected, '##NULL##') then
         log('   ' || c_pass || '  ' || i_what || ' = [' || nn(i_expected) || ']');
      else
         fail(i_what || ': expected [' || nn(i_expected)
              || '] but got [' || nn(i_actual) || ']');
      end if;
   end assert_eq_str;

   procedure assert_null (
      i_what   in varchar2,
      i_actual in varchar2
   ) is
   begin
      if i_actual is null then
         log('   ' || c_pass || '  ' || i_what || ' IS NULL');
      else
         fail(i_what || ': expected NULL but got [' || i_actual || ']');
      end if;
   end assert_null;

   procedure track (i_mq_id in number) is
   begin
      g_created.extend;
      g_created(g_created.last) := i_mq_id;
   end track;

   /**********************************************************************
       reads
   **********************************************************************/
   function vw_row (i_pid in varchar2) return perf.mq_vw%rowtype is
      r perf.mq_vw%rowtype;
   begin
      select *
        into r
        from perf.mq_vw
       where pid = i_pid;
      return r;
   end vw_row;

   function status_of (i_mq_id in number) return number is
      v number;
   begin
      select mq_status
        into v
        from perf.mq_inbound
       where mq_id = i_mq_id;
      return v;
   end status_of;

   function census_first (i_pid in varchar2) return varchar2 is
      v varchar2(64 char);
   begin
      select csm_first_nm
        into v
        from perf.ps_csm_voya_census
       where csm_pen_id = i_pid;
      return v;
   exception
      when no_data_found then
         return null;
   end census_first;

   -- A value guaranteed different from i_current and within the 64-char
   -- name width, so a name swap is always a REAL change (-> 200) and never
   -- overflows on staging or on the ERM update.
   function toggled (i_current in varchar2) return varchar2 is
   begin
      return case
                when nvl(i_current, '~') = 'MQTEST_A' then 'MQTEST_B'
                else 'MQTEST_A'
             end;
   end toggled;

   /**********************************************************************
      actions -- push a member snapshot through the public APIs
   **********************************************************************/
   -- Single-record API. mq_insert is a full record upsert (unspecified
   -- demographic fields default as null), so the whole snapshot is
   -- always sent; mutate one field of it beforehand. Returns the
   -- row's terminal mq_status.
   function apply (
      i_vw    in perf.mq_vw%rowtype,
      i_block in boolean := false
   ) return number is
      v_resp number;
      v_id   number;
   begin
      perf.mq_pkg.mq_insert(
         pid                 => i_vw.pid,
         first_name          => i_vw.first_name,
         middle_name         => i_vw.middle_name,
         last_name           => i_vw.last_name,
         name_suffix         => i_vw.name_suffix_id,
         birthdate           => i_vw.birthdate,
         dt_of_death         => i_vw.dt_of_death,
         email_addr          => i_vw.email_addr,
         sex                 => i_vw.gender_id,
         mar_status          => i_vw.mar_status_id,
         phone               => i_vw.phone,
         phone_type          => i_vw.phone_type_id,
         address1            => i_vw.address1,
         address2            => i_vw.address2,
         address3            => i_vw.address3,
         address4            => i_vw.address4,
         city                => i_vw.city,
         county              => i_vw.county,
         state               => i_vw.state,
         postal              => i_vw.postal,
         country             => i_vw.country,
         response            => v_resp,
         i_block_census_mode => i_block
      );
      if v_resp != 0 then
         fail('mq_insert returned failure response (' || v_resp || ') for pid ' || i_vw.pid);
      end if;
      -- the row just staged for this pid carries the highest mq_id
      select max(mq_id) into v_id from perf.mq_inbound where pid = i_vw.pid;
      track(v_id);
      return status_of(v_id);
   end apply;

   -- Bulk staging. Map a mq_vw snapshot onto a perf.mq_inbound%rowtype and
   -- stage it at 202 -- the documented "insert then process_mq_inbound"
   -- contract. mq_id / hashes / csm_* ids are left to the DB defaults and
   -- to process_batch's pass-1 hydration. Returns the new mq_id.
   function stage (i_vw in perf.mq_vw%rowtype) return number is
      r       perf.mq_inbound%rowtype;
      v_mq_id number;
   begin
      r.pid            := i_vw.pid;
      r.first_name     := i_vw.first_name;
      r.middle_name    := i_vw.middle_name;
      r.last_name      := i_vw.last_name;
      r.name_suffix_id := i_vw.name_suffix_id;
      r.birthdate      := i_vw.birthdate;
      r.dt_of_death    := i_vw.dt_of_death;
      r.email_addr     := i_vw.email_addr;
      r.gender_id      := i_vw.gender_id;
      r.mar_status_id  := i_vw.mar_status_id;
      r.phone          := i_vw.phone;
      r.phone_type_id  := i_vw.phone_type_id;
      r.address1       := i_vw.address1;
      r.address2       := i_vw.address2;
      r.address3       := i_vw.address3;
      r.address4       := i_vw.address4;
      r.city           := i_vw.city;
      r.county         := i_vw.county;
      r.state          := i_vw.state;
      r.postal         := i_vw.postal;
      r.country        := i_vw.country;
      r.mq_status      := 202;

      insert into perf.mq_inbound values r
      returning mq_id into v_mq_id;

      track(v_mq_id);
      return v_mq_id;
   end stage;

   -- Configure a snapshot that STAGES cleanly into mq_inbound but makes the
   -- ERM demographic UPDATE fail at PROCESSING time, so the set-based window
   -- throws and the row-by-row fallback is exercised. Tries an enforced FK
   -- on csm_nm_suf_id first, then an mq_inbound column wider than its ERM
   -- target. Returns a description, or 'SKIP' when neither injection is
   -- possible in this schema (so the test never fabricates a failure).
   function poison (io_vw in out perf.mq_vw%rowtype) return varchar2 is
      v_cnt     number;
      v_tgt_len number;
      v_src_len number;
      v_bad_suf number;
   begin
      select count(*)
        into v_cnt
        from all_constraints c
        join all_cons_columns cc
          on cc.owner = c.owner
         and cc.constraint_name = c.constraint_name
       where c.owner = g_owner
         and c.table_name = 'PS_CSM_MBR_DEMOGR'
         and c.constraint_type = 'R'
         and cc.column_name = 'CSM_NM_SUF_ID';

      if v_cnt > 0 then
         select nvl(max(csm_nm_suf_id), 0) + 100000
           into v_bad_suf
           from perf.ps_csm_nm_suffix;
         io_vw.name_suffix_id := v_bad_suf;  -- valid NUMBER, no such suffix -> FK error on apply
         return 'FK csm_nm_suf_id=' || v_bad_suf;
      end if;

      select nvl(nullif(char_length, 0), data_length)
        into v_tgt_len
        from all_tab_columns
       where owner = g_owner and table_name = 'PS_CSM_MBR_DEMOGR' and column_name = 'CSM_FIRST_NM';
      select nvl(nullif(char_length, 0), data_length)
        into v_src_len
        from all_tab_columns
       where owner = g_owner and table_name = 'MQ_INBOUND' and column_name = 'FIRST_NAME';

      if v_tgt_len < v_src_len then
         io_vw.first_name := rpad('X', v_tgt_len + 1, 'X');  -- fits mq_inbound, overflows ERM target
         return 'WIDTH first_name=' || (v_tgt_len + 1) || ' > target ' || v_tgt_len;
      end if;

      return 'SKIP';
   end poison;

   /**********************************************************************
       TEST: single record -- change (200), identical resend (304)
   **********************************************************************/
   procedure test_single_change_then_nochange (i_pid in varchar2) is
      v_orig   perf.mq_vw%rowtype;
      v_mut    perf.mq_vw%rowtype;
      v_status number;
      v_new_fn varchar2(64 char);
   begin
      log('--- single: real change -> 200, identical resend -> 304 ---');
      v_orig   := vw_row(i_pid);
      v_new_fn := toggled(v_orig.first_name);

      -- a genuine demographic change: status 200 AND the view shows the new value
      v_mut := v_orig;
      v_mut.first_name := v_new_fn;
      v_status := apply(v_mut);
      assert_eq_num('status after real change', 200, v_status);
      assert_eq_str('mq_vw.first_name after change', v_new_fn, vw_row(i_pid).first_name);

      -- resend the identical record: nothing differs against ERM -> 304
      v_status := apply(v_mut);
      assert_eq_num('status after identical resend', 304, v_status);

      -- restore (a real change back -> 200)
      v_status := apply(v_orig);
      assert_eq_num('status after restore', 200, v_status);
      assert_eq_str('mq_vw.first_name restored', v_orig.first_name, vw_row(i_pid).first_name);
   end test_single_change_then_nochange;

   /**********************************************************************
       TEST: single record -- set a populated field to NULL
   **********************************************************************/
   procedure test_single_set_null (i_pid in varchar2) is
      v_orig   perf.mq_vw%rowtype;
      v_mut    perf.mq_vw%rowtype;
      v_status number;
   begin
      log('--- single: clear a populated field to NULL -> 200 + NULL in view ---');
      v_orig := vw_row(i_pid);

      -- prime: make sure middle_name is populated so clearing it is a real change
      v_mut := v_orig;
      v_mut.middle_name := 'MQTEST_MID';
      v_status := apply(v_mut);
      assert_eq_str('mq_vw.middle_name primed', 'MQTEST_MID', vw_row(i_pid).middle_name);

      -- clear it: NULL is a genuine change -> 200, and the view reads back NULL
      v_mut.middle_name := null;
      v_status := apply(v_mut);
      assert_eq_num('status after set-to-null', 200, v_status);
      assert_null('mq_vw.middle_name after set-to-null', vw_row(i_pid).middle_name);

      -- restore original middle_name
      v_status := apply(v_orig);
      assert_eq_str('mq_vw.middle_name restored', v_orig.middle_name, vw_row(i_pid).middle_name);
   end test_single_set_null;

   /**********************************************************************
       TEST: single record -- unknown pid -> 404 (response still 0)
   **********************************************************************/
   procedure test_single_404 is
      v_resp number;
      v_id   number;
   begin
      log('--- single: unknown pid -> 404 (a rejection, not a failure) ---');
      perf.mq_pkg.mq_insert(
         pid        => v_dummy_pid,
         first_name => 'GHOST',
         last_name  => 'NONE',
         response   => v_resp
      );
      assert_eq_num('mq_insert response (unknown pid)', 0, v_resp);

      select max(mq_id) into v_id from perf.mq_inbound where pid = v_dummy_pid;
      track(v_id);
      assert_eq_num('status (unknown pid)', 404, status_of(v_id));
   end test_single_404;

   /**********************************************************************
       TEST: single record -- census sync gated by i_block_census_mode
   **********************************************************************/
   procedure test_single_census_block (i_pid in varchar2) is
      v_orig     perf.mq_vw%rowtype;
      v_mut      perf.mq_vw%rowtype;
      v_had_cen  pls_integer;
      v_cen_orig perf.ps_csm_voya_census%rowtype;
      v_status   number;
      v_fn_true  varchar2(64 char);
      v_fn_false varchar2(64 char);
   begin
      log('--- single: census synced only when i_block_census_mode => true ---');
      v_orig := vw_row(i_pid);

      -- snapshot any existing census row so we can put it back exactly
      select count(*) into v_had_cen from perf.ps_csm_voya_census where csm_pen_id = i_pid;
      if v_had_cen > 0 then
         select * into v_cen_orig from perf.ps_csm_voya_census where csm_pen_id = i_pid;
      end if;

      -- seed a known census marker to observe sync against
      delete from perf.ps_csm_voya_census where csm_pen_id = i_pid;
      insert into perf.ps_csm_voya_census (csm_pen_id, csm_first_nm, csm_last_nm)
      values (i_pid, 'CENSUS_OLD', 'CENSUS_OLD');
      commit;

      v_fn_true  := toggled(v_orig.first_name);
      v_fn_false := case when v_fn_true = 'MQTEST_A' then 'MQTEST_B' else 'MQTEST_A' end;

      -- block_census_mode => TRUE: census IS synced to the new first name
      v_mut := v_orig;
      v_mut.first_name := v_fn_true;
      v_status := apply(v_mut, i_block => true);
      assert_eq_num('status (census block=true, real change)', 200, v_status);
      assert_eq_str('census synced when block=true', v_fn_true, census_first(i_pid));

      -- reset the marker, then block_census_mode => FALSE: census NOT synced
      update perf.ps_csm_voya_census set csm_first_nm = 'CENSUS_OLD' where csm_pen_id = i_pid;
      commit;
      v_mut.first_name := v_fn_false;
      v_status := apply(v_mut, i_block => false);
      assert_eq_num('status (census block=false, real change)', 200, v_status);
      assert_eq_str('census NOT synced when block=false', 'CENSUS_OLD', census_first(i_pid));

      -- restore ERM demographic
      v_status := apply(v_orig);

      -- restore the census table to its exact pre-test state
      delete from perf.ps_csm_voya_census where csm_pen_id = i_pid;
      if v_had_cen > 0 then
         insert into perf.ps_csm_voya_census values v_cen_orig;
      end if;
      commit;
   end test_single_census_block;

   /**********************************************************************
       TEST: bulk -- mixed window resolves 200 / 304 / 404
   **********************************************************************/
   procedure test_bulk_mixed (
      i_pid_changed in varchar2,
      i_pid_same    in varchar2
   ) is
      v_orig   perf.mq_vw%rowtype;
      v_mut    perf.mq_vw%rowtype;
      v_id_chg number;
      v_id_sam number;
      v_id_404 number;
      r404     perf.mq_inbound%rowtype;
      v_status number;
   begin
      log('--- bulk: one window -> changed=200, unchanged=304, unknown=404 ---');
      v_orig := vw_row(i_pid_changed);

      -- changed member (mutated demographic) -> 200
      v_mut := v_orig;
      v_mut.first_name := toggled(v_orig.first_name);
      v_id_chg := stage(v_mut);

      -- untouched snapshot of another member -> 304
      v_id_sam := stage(vw_row(i_pid_same));

      -- unknown pid -> 404
      r404.pid        := v_dummy_pid;
      r404.first_name := 'GHOST';
      r404.mq_status  := 202;
      insert into perf.mq_inbound values r404 returning mq_id into v_id_404;
      track(v_id_404);

      commit;  -- documented contract: stage + COMMIT, then process

      perf.mq_pkg.process_mq_inbound(
         i_starting_mq_id => least(v_id_chg, v_id_sam, v_id_404),
         i_status         => 202
      );

      assert_eq_num('bulk changed row',   200, status_of(v_id_chg));
      assert_eq_num('bulk unchanged row', 304, status_of(v_id_sam));
      assert_eq_num('bulk unknown pid',   404, status_of(v_id_404));
      assert_eq_str('mq_vw reflects the bulk change',
                    v_mut.first_name, vw_row(i_pid_changed).first_name);

      v_status := apply(v_orig);  -- restore the changed member
   end test_bulk_mixed;

   /**********************************************************************
       TEST: bulk -- i_max_records throttles rows processed this run
   **********************************************************************/
   procedure test_bulk_max_records (
      i_p1 in varchar2,
      i_p2 in varchar2,
      i_p3 in varchar2
   ) is
      v_o1   perf.mq_vw%rowtype;
      v_o2   perf.mq_vw%rowtype;
      v_o3   perf.mq_vw%rowtype;
      v_m    perf.mq_vw%rowtype;
      v_id1  number;
      v_id2  number;
      v_id3  number;
      v_st   number;
   begin
      log('--- bulk: i_max_records => 2 processes two, leaves the third at 202 ---');
      v_o1 := vw_row(i_p1);
      v_o2 := vw_row(i_p2);
      v_o3 := vw_row(i_p3);

      v_m := v_o1; v_m.first_name := toggled(v_o1.first_name); v_id1 := stage(v_m);
      v_m := v_o2; v_m.first_name := toggled(v_o2.first_name); v_id2 := stage(v_m);
      v_m := v_o3; v_m.first_name := toggled(v_o3.first_name); v_id3 := stage(v_m);
      commit;

      perf.mq_pkg.process_mq_inbound(
         i_starting_mq_id => least(v_id1, v_id2, v_id3),
         i_status         => 202,
         i_max_records    => 2
      );

      -- the two lowest mq_ids leave 202; the third is held back
      if status_of(v_id1) = 202 or status_of(v_id2) = 202 then
         fail('max_records: expected the first two staged rows to be processed');
      end if;
      log('   ' || c_pass || '  first two rows processed (status '
          || status_of(v_id1) || ', ' || status_of(v_id2) || ')');
      assert_eq_num('third row throttled (stays 202)', 202, status_of(v_id3));

      -- drain the held row, then restore all three members
      perf.mq_pkg.process_mq_inbound(i_starting_mq_id => v_id3, i_status => 202);
      v_st := apply(v_o1);
      v_st := apply(v_o2);
      v_st := apply(v_o3);
   end test_bulk_max_records;

   /**********************************************************************
       TEST: bulk -- i_commit_size windows the run into per-chunk commits
   **********************************************************************/
   procedure test_bulk_chunk (
      i_p1 in varchar2,
      i_p2 in varchar2
   ) is
      v_o1  perf.mq_vw%rowtype;
      v_o2  perf.mq_vw%rowtype;
      v_m   perf.mq_vw%rowtype;
      v_id1 number;
      v_id2 number;
      v_st  number;
   begin
      log('--- bulk: i_commit_size => 1 still resolves every row ---');
      v_o1 := vw_row(i_p1);
      v_o2 := vw_row(i_p2);

      v_m := v_o1; v_m.first_name := toggled(v_o1.first_name); v_id1 := stage(v_m);
      v_m := v_o2; v_m.first_name := toggled(v_o2.first_name); v_id2 := stage(v_m);
      commit;

      perf.mq_pkg.process_mq_inbound(
         i_starting_mq_id => least(v_id1, v_id2),
         i_status         => 202,
         i_commit_size    => 1
      );

      assert_eq_num('chunked row 1 processed', 200, status_of(v_id1));
      assert_eq_num('chunked row 2 processed', 200, status_of(v_id2));

      v_st := apply(v_o1);
      v_st := apply(v_o2);
   end test_bulk_chunk;

   /**********************************************************************
       TEST: bulk -- a poison row falls back to row-by-row (500 + logged),
                     good rows in the same window still succeed
   **********************************************************************/
   procedure test_bulk_fallback (
      i_p1 in varchar2,
      i_p2 in varchar2,
      i_p3 in varchar2
   ) is
      v_o1     perf.mq_vw%rowtype;
      v_o2     perf.mq_vw%rowtype;
      v_o3     perf.mq_vw%rowtype;
      v_bad    perf.mq_vw%rowtype;
      v_m      perf.mq_vw%rowtype;
      v_id1    number;
      v_id2    number;
      v_id3    number;
      v_method varchar2(200);
      v_logs   number;
      v_st     number;
   begin
      log('--- bulk: row-by-row fallback isolates a poison row as 500 ---');
      v_o1 := vw_row(i_p1);
      v_o2 := vw_row(i_p2);
      v_o3 := vw_row(i_p3);

      v_bad    := v_o2;
      v_method := poison(v_bad);
      if v_method = 'SKIP' then
         log('   SKIP  no valid-staging row can fail on apply in this schema '
             || '(mq_inbound columns are not wider than their ERM targets and '
             || 'no csm_nm_suf_id FK is enforced); fallback path not exercised.');
         return;
      end if;
      log('   (injecting a processing-time failure via ' || v_method || ')');

      -- good, poison, good
      v_m := v_o1; v_m.first_name := toggled(v_o1.first_name); v_id1 := stage(v_m);
      v_id2 := stage(v_bad);
      v_m := v_o3; v_m.first_name := toggled(v_o3.first_name); v_id3 := stage(v_m);
      commit;

      perf.mq_pkg.process_mq_inbound(
         i_starting_mq_id        => least(v_id1, v_id2, v_id3),
         i_status                => 202,
         i_allow_partial_success => true
      );

      assert_eq_num('fallback good row 1 -> 200', 200, status_of(v_id1));
      assert_eq_num('fallback poison row -> 500', 500, status_of(v_id2));
      assert_eq_num('fallback good row 3 -> 200', 200, status_of(v_id3));

      select count(*)
        into v_logs
        from perf.mq_pkg_log
       where mq_id = v_id2 and error_code is not null;
      if v_logs = 0 then
         fail('fallback: no mq_pkg_log entry for failed mq_id ' || v_id2);
      end if;
      log('   ' || c_pass || '  error logged for the poison row (' || v_logs || ' row(s))');

      -- the poison row never mutated ERM (the demographic MERGE is the first
      -- statement and it threw); restore only the two good members
      v_st := apply(v_o1);
      v_st := apply(v_o3);
   end test_bulk_fallback;

   /**********************************************************************
       cleanup -- delete exactly the mq_inbound (+log) rows we created
   **********************************************************************/
   procedure cleanup is
   begin
      if g_created.count > 0 then
         forall i in 1 .. g_created.count
            delete from perf.mq_pkg_log where mq_id = g_created(i);
         forall i in 1 .. g_created.count
            delete from perf.mq_inbound where mq_id = g_created(i);
      end if;
      commit;
   end cleanup;

begin
   /* dev-only guard: never mutate ERM outside a dev host */
   declare
      v_host varchar2(200);
   begin
      v_host := lower(nvl(sys_context('USERENV', 'SERVER_HOST'), 'dev'));
      if instr(v_host, 'dev') = 0 then
         log('Non-dev host (' || v_host || ') detected; skipping mq_pkg tests.');
         raise e_not_dev;
      end if;
   end;

   /*  pick three real, distinct pids */
   declare
      type t_pid_tab is table of perf.mq_inbound.pid%type;
      v t_pid_tab;
   begin
      select pid
        bulk collect into v
        from ( select pid
                 from perf.mq_vw
                where pid is not null
                  and length(pid) <= 9
                group by pid
               having count(*) = 1   -- ensure exactly one view row per pid
                order by pid )
       where rownum <= 3;

      if v.count < 3 then
         fail('need >= 3 single-row pids in perf.mq_vw, found ' || v.count);
      end if;
      v_pid_1 := v(1);
      v_pid_2 := v(2);
      v_pid_3 := v(3);
   end;

   /* a pid guaranteed absent from the view */
   v_dummy_pid := 'DUMMY0404';
   declare
      n number;
   begin
      select count(*) into n from perf.mq_vw where pid = v_dummy_pid;
      if n > 0 then
         fail('dummy pid ' || v_dummy_pid || ' unexpectedly exists in perf.mq_vw; pick another');
      end if;
   end;

   log('================ perf.mq_pkg unit tests ================');
   log('pids: ' || v_pid_1 || ', ' || v_pid_2 || ', ' || v_pid_3 || '   dummy: ' || v_dummy_pid);

   test_single_change_then_nochange(v_pid_1);
   test_single_set_null(v_pid_1);
   test_single_404;
   test_single_census_block(v_pid_2);
   test_bulk_mixed(v_pid_1, v_pid_2);
   test_bulk_max_records(v_pid_1, v_pid_2, v_pid_3);
   test_bulk_chunk(v_pid_1, v_pid_2);
   test_bulk_fallback(v_pid_1, v_pid_2, v_pid_3);

   cleanup;

   log('================ ALL TESTS PASSED ================');
exception
   when e_not_dev then
      null;
   when others then
      -- best-effort: drop any rows we created but had not yet cleaned up
      begin
         cleanup;
      exception
         when others then null;
      end;
      log('################ TEST FAILURE ################');
      log(dbms_utility.format_error_stack);
      log(dbms_utility.format_error_backtrace);
      raise;
end;
/
