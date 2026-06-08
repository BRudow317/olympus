# charon

charon runs a Python job the same way on a Windows dev machine and on the
Oracle Linux 8 server, even though the server has no internet and runs a
different Python (3.11 vs 3.13). It carries its dependencies as pre-downloaded
wheel files and builds a local virtualenv from them.

Nobody runs `pip install` anywhere, least of all on the server. Dependencies
live in `pyproject.toml`. charon turns that list into wheels and a venv for you.

---

## The single source of truth

`pyproject.toml` `[project].dependencies` is the only place you declare what the
job needs. The `[tool.charon]` table is the on/off switch: with it, charon
manages an offline venv; without it, charon does nothing special (so other jobs
that share this same `charon.py` are unaffected).

```toml
[project]
dependencies = ["httpx", "tzdata", "polars", "oracledb", "pyarrow"]

[tool.charon]
managed = true
```

---

## The rule that makes the air-gap work

The operating system decides whether charon may reach PyPI. This is not a guess
or a network probe - it is fixed:

- Windows = online. charon may download wheels and (re)build the wheelhouse itself.
- Oracle Linux 8 = offline. charon may only use wheels already in the repo. If
  they are missing or stale, it stops with a precise message. It never tries the
  network, because it cannot reach it.

So the wheels are produced on Windows, committed to the repo, and shipped inside
the zip. The server consumes them. Nothing crosses the air-gap at run time.

---

## Repo layout

```
charon/                         repo root, deploys to /stage/rundeck-scripts/charon
  charon.py                     the program Rundeck runs (generic; do not edit per job)
  pyproject.toml                dependencies + [tool.charon] marker  <- you edit this
  configs/
    build_wheelhouse.py         optional manual/CI trigger for the vendoring
  requirements.lock             GENERATED - commit it
  vendor/wheels/
    linux_py311/                GENERATED - OL8 wheels - commit
    win_py313/                  GENERATED - Windows wheels - commit
    .charon-deps-hash           GENERATED - drift stamp - commit
  .venv/                        built at run time - NOT committed
  .logs/                        run logs - NOT committed
  src/ (or jobs/)               your application code
```

Commit everything under `vendor/wheels/` (including `.charon-deps-hash`),
`requirements.lock`, and `pyproject.toml`. `.gitignore` already drops `.venv/`
and `.logs/`.

---

## Who does what, in order

### Role A - Developer changes dependencies (only when the dep list changes)

On Windows, on the machine that can reach the internet.

1. Edit `pyproject.toml` `[project].dependencies`.
2. Just run the job (see Role B). charon notices the dependency list changed,
   re-downloads both platforms' wheels, and rewrites `requirements.lock`
   automatically. No separate build command is required.
   - If you prefer to vendor without running the job (e.g. in CI):
     `python configs\build_wheelhouse.py`
3. Commit the refreshed files:
   ```
   git add pyproject.toml requirements.lock vendor/wheels
   git commit -m "deps: refresh"
   git push
   ```

If a dependency has no prebuilt wheel (source-only), the Windows build fails
immediately and names the package. Fix the dependency; a half-vendored repo is
never produced.

### Role B - Any developer runs or tests the job on Windows

```
python charon.py -v -l ./.logs --exec python src\app.py [your app args]
```
No `--venv`, no manual setup. First run (or first run after a dep change) builds
`.venv` from the vendored wheels - downloading them first if needed, because
Windows is online. Later runs reuse `.venv`.

### Role C - Linux admin deploys

1. Get the zip. It MUST contain `vendor/wheels/`, `requirements.lock`, and
   `pyproject.toml` (see the checklist below).
2. WinSCP it over, unzip to `/stage/rundeck-scripts/charon`.
3. Done. No commands, no pip, no internet.

### Rundeck runs the job

```
python3.11 /stage/rundeck-scripts/charon/charon.py -v -l ./.logs \
  --exec python /stage/rundeck-scripts/charon/src/app.py [your app args]
```
First click builds `.venv` from `vendor/wheels/linux_py311`, offline, then runs.
Later clicks reuse it. Must be `python3.11`, not `python3` (which is 3.6);
charon refuses the wrong version with a clear message.

---

## Behavior at a glance

| State | Windows (online) | OL8 (offline) |
|---|---|---|
| wheels present, match pyproject | build/reuse venv, run | build/reuse venv, run |
| venv missing or broken | rebuild venv from wheels | rebuild venv from wheels |
| wheels missing or stale vs pyproject | re-download, rebuild, run | STOP: rebuild on Windows, redeploy |
| a dependency has no wheel | build fails, names the package | STOP: same packaging error |

The only thing charon cannot do is fetch a missing wheel on OL8 - that is the
air-gap, not a limitation of the script. On the server, a missing or stale
wheelhouse is always a packaging miss made earlier on the Windows side.

---

## Checklist before you hand a zip to the admin

On Windows, from the repo root:
```
dir vendor\wheels\linux_py311      (~19 .whl files)
dir vendor\wheels\win_py313        (~19 .whl files)
dir requirements.lock
type vendor\wheels\.charon-deps-hash
```
Zip INCLUDING `vendor/wheels/`; EXCLUDE `.venv/` and `.logs/`.

---

## Failure messages and fixes

- `wheelhouse not found for this platform: ...` - the zip lacks the wheels for
  the running platform. Re-vendor on Windows, re-commit, re-zip.
- `pyproject.toml dependencies do not match the vendored wheels` - someone
  edited deps without re-vendoring on Windows before zipping. Rebuild, redeploy.
- `requires Python >= 3.11 ... running under 3.6` - the Rundeck command used
  `python3`; change it to `python3.11`.
- `ModuleNotFoundError` inside your app - means the child ran on a Python that is
  not charon's venv. With this charon and a `[tool.charon]` table present, that
  should not happen; if it does, confirm `pyproject.toml` sits next to `charon.py`.

---

## The one thing only the server can confirm

The compiled wheels (polars especially) are built for standard x86-64 CPUs. On
older or heavily virtualized server hardware, polars can fail at import with an
"Illegal instruction" error - something you only learn on the real box.

If it happens, change `polars` to `polars-lts-cpu` in `pyproject.toml`, re-run
the job on Windows to re-vendor, commit, redeploy. It still imports as `polars`
in your code.