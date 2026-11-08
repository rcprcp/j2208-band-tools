# j2208-band-tools

Raw BLE access to **J2208-family fitness bands** — sold under several brands,
including the Hume Band V2 and bare ODM model codes `2301B` and `X3B`. Reads
stored history and live sensor values directly, with no companion app and no
cloud account. See [PROTOCOL.md](PROTOCOL.md) for the protocol itself.

These bands use **no encryption and no authentication** — any BLE central can
connect and issue commands.

## Which devices

The hardware is a Shenzhen ODM reference design that brands rebadge; the
companion app (`com.elink.fittrackhealth.pro`, "FitTrack Health") ships one
binary serving several of them. Its own device resolver keys off the
advertised name:

```
startsWith "hume band v2" | "2301b" | "x3b"  ->  V2 generation
otherwise                                    ->  V1 generation
```

Both generations share this data protocol; only the firmware-update path
differs. If your band advertises one of those names — or answers on the
`fff0/fff6/fff7` GATT profile with 16-byte checksummed frames — this should
work.

**Verified on:** one Hume Band V2, firmware v0.0.3.9. Everything else in the
family is an inference from the app's own model list, not something I have
tested. Reports from other units are welcome.

`AA:BB:CC:DD:EE:FF` in the examples below is a placeholder; substitute your own
band's address from `j2208.py scan`.

## Setup

```bash
python -m pip install bleak
```

## Usage

Only one central can hold the band at a time. **Close the companion app (or
turn the phone's Bluetooth off) first**, otherwise the band stays connected to
the phone and won't advertise.

Find the band:

```bash
python tools/j2208.py scan --seconds 20
```

Read version, battery, MAC and clock:

```bash
python tools/j2208.py info AA:BB:CC:DD:EE:FF
```

Dump the GATT tree:

```bash
python tools/j2208.py probe AA:BB:CC:DD:EE:FF
```

Download stored history (no wearing required — this is the main event):

```bash
python tools/j2208.py history AA:BB:CC:DD:EE:FF --type hr
```

Types: `total`, `steps`, `sleep`, `hr`, `hrv`, `spo2_auto`, `temp_axil`
(plus `hr_dynamic`, `spo2`, `temp`, which are empty on V2 firmware). Add
`--since 2026-07-25T00:00:00` to restrict the range; the default pulls
everything the band holds — roughly the last 4–5 days.

Grab the lot:

```bash
for t in total steps sleep hr hrv spo2_auto temp_axil; do python tools/j2208.py history AA:BB:CC:DD:EE:FF --type $t; done
```

Read or change how often the band logs to its own history. This is the
cheapest way to raise resolution — the band records on its own schedule
whether or not anything is connected:

```bash
python tools/j2208.py auto AA:BB:CC:DD:EE:FF
python tools/j2208.py auto AA:BB:CC:DD:EE:FF --set hr --interval 5
```

Factory defaults are HR 10 min, SpO2 30 min, temperature 5 min, HRV 60 min.
Shorter intervals mean more optical measurements and so more battery drain.

Stream live readings (~0.7 Hz while worn):

```bash
python tools/j2208.py live AA:BB:CC:DD:EE:FF --type heart --seconds 60 --out captures/live.csv
```

Types: `heart` (fastest, HR only), `spo2`, `temp`, `hrv`. The `spo2` and
`temp` modes take ~30 s to warm up, then report HR, SpO2, HRV, stress and
temperature together.

Raw PPG — **does not work on V2 firmware**, `0x3A` returns all zeros even
with a confirmed pulse. The command is kept for testing other units:

```bash
python tools/j2208.py ppg AA:BB:CC:DD:EE:FF --seconds 60 --out captures/ppg.csv
```

Send arbitrary commands while exploring:

```bash
python tools/j2208.py send AA:BB:CC:DD:EE:FF --cmd 0x99:1,0,0,0 --cmd 0x3a:1
```

## Desktop collector

`tools/j2208_daemon.py` holds a persistent connection, reconnects on drop, and
writes to SQLite (`live`, `history`, `battery`, `events` tables). Nothing about
it touches the phone or the vendor's servers.

```bash
python tools/j2208_daemon.py AA:BB:CC:DD:EE:FF --mode history --interval 60
```

Heart-rate only, synced every 5 minutes to match a 5-minute logging interval
(a full sweep of all seven types takes ~90 s, so narrow it):

```bash
python tools/j2208_daemon.py AA:BB:CC:DD:EE:FF --mode history --types hr --interval 300 --history-every 1
```

Two modes, with very different battery cost:

| Mode | Resolution | Cost |
| --- | --- | --- |
| `history` | HR 10 min, temp 5 min | negligible — the band logs this anyway |
| `live` | ~0.7 Hz during each burst | powers the optical sensor; this is the expensive one |

`--interval` controls how often it polls; in `history` mode it does **not**
improve resolution, because the sampling rate is fixed in firmware. Polling
faster than the firmware records just re-reads the same rows (they are
deduplicated on insert).

For `live`, the sensor needs **~9 s to lock on** before HR stops reading zero,
so `--burst` below ~15 s returns nothing. `--burst 0` leaves the sensor on
permanently (no repeated warm-up, maximum drain).

Battery percentage is recorded every cycle, so run it for a few hours and read
the actual drain rather than guessing — printed on exit, or:

```sql
SELECT ts, pct FROM battery ORDER BY ts;
```

## PostgreSQL backend (optional)

SQLite remains the default. The collector can write to PostgreSQL instead,
which is useful for concurrent readers, notebooks, Grafana, and remote
collection. The database layout is a dedicated schema (default `j2208`) in an
existing database; it does not touch `public`.

Install the optional driver:

```bash
python -m pip install -r requirements-postgres.txt
```

Create the schema and tables:

```bash
export J2208_PG_SCHEMA=j2208
export J2208_PG_USER=bob
export J2208_PG_DATABASE=bob
export J2208_PG_HOST=127.0.0.1
export J2208_PG_PASSWORD='...'
python tools/j2208_pg_setup.py
python tools/j2208_pg_setup.py --check
```

`J2208_PG_PASSWORD` falls back to `PGPASSWORD`. If you connect through the
local Unix socket with peer authentication, omit the password and set
`J2208_PG_HOST=/var/run/postgresql`. The password is read from the environment
only; it is never a CLI argument and is never written to the `events` table.

Run the collector:

```bash
J2208_DB_BACKEND=postgres python tools/j2208_daemon.py <address> \
    --backend postgres --once --types hr
```

Then drop `--once` for continuous collection. `J2208_DB_BACKEND` and
`--backend` are equivalent; `--db` is ignored for PostgreSQL.

Environment variables:

| Variable | Default | Purpose |
| --- | --- | --- |
| `J2208_DB_BACKEND` | `sqlite` | `sqlite` or `postgres` |
| `J2208_PG_SCHEMA` | `j2208` | schema name (validated identifier) |
| `J2208_PG_USER` | `$USER` / `PGUSER` | role |
| `J2208_PG_PASSWORD` | `PGPASSWORD` | password; omit for peer auth |
| `J2208_PG_HOST` | libpq default | host; omit for Unix socket |
| `J2208_PG_PORT` | `5432` | port |
| `J2208_PG_DATABASE` | `$USER` | database |
| `J2208_PG_SSLMODE` | `prefer` | `require` for remote |
| `J2208_PG_POOL_MIN` / `MAX` | `1` / `4` | async pool sizing |
| `J2208_DEVICE_ID` | BLE address | device key in the database |

Tables: `devices`, `history` (lossless JSONB plus `bytea` raw packets), `live`,
`battery`, `events`, and `schema_version`. Typed views `v_hr`, `v_spo2`,
`v_temp`, `v_hrv`, `v_steps`, `v_sleep`, and `v_total` project the common
fields. Band timestamps are naive local time (`timestamp`); host-side times are
UTC (`timestamptz`).

Import an existing SQLite capture:

```bash
python tools/j2208_migrate_sqlite.py --sqlite captures/j2208.db
```

Back up or drop the schema independently:

```bash
pg_dump -d bob -n j2208 > j2208.sql
python tools/j2208_pg_setup.py --drop --yes
```

## Repo layout

| Path | What |
| --- | --- |
| `tools/j2208.py` | BLE client — scan / probe / info / live / history / ppg / send |
| `tools/j2208_daemon.py` | Long-running collector, SQLite or PostgreSQL output |
| `tools/j2208_store.py` | Storage backends, env parsing, identifier validation |
| `tools/j2208_pg_setup.py` | Create / verify / drop the PostgreSQL schema |
| `tools/j2208_migrate_sqlite.py` | Copy an existing SQLite capture into PostgreSQL |
| `migrations/001_init.sql` | PostgreSQL schema, tables, indexes, and views |
| `tools/probe190e.py` | Probe for the unused `0x190e` vendor service |
| `tools/dexstrings.py` | Pure-Python DEX string + UUID miner (no Java needed) |
| `PROTOCOL.md` | Protocol documentation |
| `apk/`, `jadx-out/` | Pulled APK and decompiled sources (gitignored, never published) |
| `captures/` | Capture output, i.e. personal health data (gitignored, never published) |

## What is deliberately not here

- **No APK, no decompiled sources, no firmware images.** Those belong to the
  app publisher and the ODMs behind it. They are gitignored and have never been
  committed. Reproduce them yourself from your own device with the steps below.
- **No health data.** Every capture stays local. Sample readings in the docs
  are illustrative placeholders, not real measurements.
- **No device identifiers.** MAC addresses and advertised names in the docs are
  placeholders.

## Reproducing the decompile

```bash
adb shell pm path com.elink.fittrackhealth.pro
adb pull <base.apk path> apk/base.apk
winget install Microsoft.OpenJDK.21
winget install Skylot.jadx
java -cp "$env:LOCALAPPDATA\Microsoft\WinGet\Packages\Skylot.jadx_*\lib\jadx-gui-1.5.5-all.jar" jadx.cli.JadxCLI -d jadx-out --no-res apk/base.apk
```

jadx exits 3 with ~100 failed classes out of 13,000; the band's BLE package
decompiles cleanly. The interesting code is
`jadx-out/sources/com/example/watch_flutter_plugin/humeBandJ2208/`.

## Scope and legal notes

Not affiliated with, endorsed by, or connected to Hume Health, eLink, FitTrack,
or any ODM in the supply chain. Brand and model names appear here only to
identify which hardware this code talks to, which is the whole point of a
compatibility project.

Worth stating plainly, because it shapes everything below: **the protocol
documented here is not any one brand's creation.** It is a Shenzhen ODM
reference design, resold under multiple badges, running a stock SoC stack on
the generic `fff0` profile. The brand on the strap licensed it.

- **Purpose is interoperability.** This exists so the owner of a band can read
  the data their own device records about their own body, on a platform the
  vendor does not support. Reverse engineering for interoperability is
  expressly permitted in many jurisdictions — s47D of the Australian Copyright
  Act, Art. 6 of the EU Software Directive, and 17 U.S.C. §1201(f) in the US.
- **Nothing was circumvented.** The band ships with no encryption, no pairing
  requirement and no authentication handshake. There is no technological
  protection measure here to bypass; the protocol was recovered from an
  unobfuscated app and confirmed by observing an open, plaintext radio link.
- **No vendor code or assets are redistributed.** See the section above.
- **Your warranty and account are your risk.** Whichever brand sold you the
  band, its terms of service may prohibit this regardless of the above. That is
  a contract question between you and them. Changing device settings
  (`auto --set`) writes to firmware; the rest is read-only.
- **Not a medical device, and neither is the band.** Do not use any of this for
  diagnosis or treatment.

## Licence

MIT — see [LICENSE](LICENSE). Applies to the code and documentation in this
repository only, not to anything produced by decompiling the vendor's app.
