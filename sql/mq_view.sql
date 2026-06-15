create or replace force view qbl.mq_vw as
   select d.csm_mbr_demogr_id,
          d.csm_pen_id as pid,
  
  /* Demographics */
          d.csm_first_nm as first_name,
          d.csm_middle_nm as middle_name,
          d.csm_last_nm as last_name,
          d.csm_nm_pre_id as name_prefix_id,
          prefix.csm_nm_pre_desc as name_prefix,
          d.csm_nm_suf_id as name_suffix_id,
          suffix.csm_nm_suf_desc as name_suffix,
          d.csm_birth_dt as birthdate,
          d.csm_death_dt as dt_of_death,
  
  /* Additional Demographics */
          d.csm_email as email_addr,
          d.csm_sex as gender_id,
          gender.csm_mbr_lov_desc as gender,
          d.csm_maritial_st as mar_status_id,
          mar.csm_mbr_lov_desc as mar_status,
          d.csm_ssn as ssn,
  
  /* Tertiary Demographics Fields */
          d.csm_trf_pre96_flg as demogr_trf_pre96_flg,
          d.csm_status_id as demogr_status_id,
          d.created_by as demogr_created_by,
          d.create_date as demogr_create_date,
          d.updated_by as demogr_updated_by,
          d.update_date as demogr_update_date,
  
  /* Standardized Hash to match hydrate_mq_hash package routine */
          rawtohex(standard_hash(
             nvl(
                d.csm_first_nm,
                chr(124)
             )
             || nvl(
                d.csm_middle_nm,
                chr(124)
             )
             || nvl(
                d.csm_last_nm,
                chr(124)
             )
             || nvl(
                to_char(d.csm_nm_suf_id),
                chr(124)
             )
             || nvl(
                to_char(
                   d.csm_birth_dt,
                   'YYYY-MM-DD'
                ),
                chr(124)
             )
             || nvl(
                to_char(
                   d.csm_death_dt,
                   'YYYY-MM-DD'
                ),
                chr(124)
             )
             || nvl(
                d.csm_email,
                chr(124)
             )
             || nvl(
                to_char(d.csm_sex),
                chr(124)
             )
             || nvl(
                to_char(d.csm_maritial_st),
                chr(124)
             ),
             'MD5'
          )) as mq_demogr_hash,

  /* Phone */
          p.csm_mbr_phn_id,
          p.csm_phn as phone,
          p.csm_phn_type_id as phone_type_id,
          phtyp.csm_phone_type as phone_type,
  
  /* Phone Audit Fields */
          p.phone_ext,
          p.created_by as phone_created_by,
          p.create_date as phone_create_date,
          p.updated_by as phone_updated_by,
          p.update_date as phone_update_date,
  /* Standardized Hash to match hydrate_mq_hash package routine.
     Only columns present on qbl.mq_inbound participate so the inbound
     side can reproduce this exact hash. */
          rawtohex(standard_hash(
             nvl(
                p.csm_phn,
                chr(124)
             )
             || nvl(
                to_char(p.csm_phn_type_id),
                chr(0)
             ),
             'MD5'
          )) as mq_phone_hash,

  /* Address */
          addr.csm_mbr_addr_id,
          addr.csm_addr1 as address1,
          addr.csm_addr2 as address2,
          addr.csm_addr3 as address3,
          addr.csm_addr4 as address4,
          addr.csm_city as city,
          addr.csm_county as county,
          addr.csm_state as state,
          addr.csm_postal as postal,
          addr.csm_country as country,
          addr.csm_addr_type as address_type, 
  
  /* Address Audit Fields */
          addr.created_by as address_created_by,
          addr.create_date as address_create_date,
          addr.updated_by as address_updated_by,
          addr.update_date as address_update_date,
          rawtohex(standard_hash(
             nvl(
                addr.csm_addr1,
                chr(124)
             )
             || nvl(
                addr.csm_addr2,
                chr(124)
             )
             || nvl(
                addr.csm_addr3,
                chr(124)
             )
             || nvl(
                addr.csm_addr4,
                chr(124)
             )
             || nvl(
                addr.csm_city,
                chr(124)
             )
             || nvl(
                addr.csm_county,
                chr(124)
             )
             || nvl(
                addr.csm_state,
                chr(124)
             )
             || nvl(
                to_char(addr.csm_postal),
                chr(124)
             )
             || nvl(
                addr.csm_country,
                chr(124)
             ),
             'MD5'
          )) as mq_address_hash,

  /* Audit Details */
          systimestamp as retrieved_at,
          nvl(
             sys_context(
                'userenv',
                'client_identifier'
             ),
             user
          ) as retrieved_by
     from qbl.ps_csm_mbr_demogr d
     left join qbl.ps_csm_mbr_lov mar
   on d.csm_maritial_st = mar.csm_mbr_lov_id
      and mar.csm_mbr_lov_type = 'CSM_MARITAL_ST'
     left join qbl.ps_csm_mbr_lov gender
   on d.csm_sex = gender.csm_mbr_lov_id
      and gender.csm_mbr_lov_type = 'CSM_SEX'
     left join qbl.ps_csm_nm_prefix prefix
   on prefix.csm_nm_pre_id = d.csm_nm_pre_id
     left join qbl.ps_csm_nm_suffix suffix
   on suffix.csm_nm_suf_id = d.csm_nm_suf_id
     left join qbl.ps_csm_mbr_addr addr
   on d.csm_mbr_demogr_id = addr.csm_mbr_demogr_id
     left join qbl.ps_csm_mbr_phn p
   on d.csm_mbr_demogr_id = p.csm_mbr_demogr_id
      and p.csm_phn_type_id = 1             -- Fixed variable target pointer bug
     left join qbl.ps_csm_phn_typ_ref phtyp
   on phtyp.phone_type_id = p.csm_phn_type_id
    where d.csm_status_id = '37'
      and d.csm_pen_id <> 'N/A';