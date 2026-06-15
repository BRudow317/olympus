# MQ Package Quick Use

`qbl.mq_pkg` exists to take inbound member data, compare it to `qbl.mq_vw`, and mark each row as updated, unchanged, not found, or failed.

## Story Rules

- The SQL package is the source of truth for this integration flow.
- External integrations call package procedures, not ad hoc table update logic.
- `mq_insert` is the public single-record API for the integration team.
- Single-record processing must compare the inbound golden record to current ERM state, apply changes when needed, return `0` on success, and raise on failure.
- Bulk processing stages rows in `qbl.mq_inbound` with `mq_status = 202`, then processes them through the package.
- Bulk processing must stay simple; no scheduler jobs, DBMS jobs, worker pools, or multithreaded queue design.
- `process_mq_inbound` is the public bulk processing entrypoint for staged rows.
- `census_sentinel` behavior is a required part of the bulk story and must be preserved.
- Public APIs should be direct and clear; unnecessary wrapper/facade procedures should be avoided.
- The inbound payload is intentionally minimal and should stay aligned with `qbl.mq_inbound` and the package hash/compare logic.

## Single Record

Use `qbl.mq_pkg.mq_insert(...)` when the integration sends one record and expects an immediate result.

```sql
declare
	v_response number;
begin
	qbl.mq_pkg.mq_insert(pid => '123456', first_name => 'JANE', last_name => 'DOE', response => v_response);
end;
/
```

Happy path: `response = 0` and the inbound row finishes in status `200` (updated) or `304` (no change). Failure path: `response = 1` and the procedure raises an exception.

## Bulk Load

Use bulk load when you already have many rows to stage. Insert them directly into `qbl.mq_inbound` with `mq_status = 202` and the payload columns populated, then call `qbl.mq_pkg.process_mq_inbound(...)`.

```sql
insert into qbl.mq_inbound (pid, first_name, last_name, mq_status)
values ('123456', 'JANE', 'DOE', 202);

begin
	qbl.mq_pkg.process_mq_inbound(i_status => 202, i_bulk_mode => 1);
end;
/
```

Happy path: staged `202` rows are processed against `qbl.mq_vw` and each row lands in one of these terminal statuses:

- `200` updated
- `304` no change
- `404` pid not found
- `500` processing error

Use `qbl.mq_pkg_log` to inspect errors for `500` rows.
