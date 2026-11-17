# SEM6000 wrapper → MQTT / Home Assistant / InfluxDB

Wraps around [python3-voltcraft-sem6000](https://github.com/moormaster/python3-voltcraft-sem6000) to read **Voltcraft SEM6000** Bluetooth LE energy meters (sold by Conrad Electronics) and publishes the readings to:

- **MQTT**: plain topics and a JSON message per meter;
- **Home Assistant**: through MQTT discovery, with power, voltage, current, frequency, power factor,
  daily energy and the relay (a switch, or a read-only sensor);
- **InfluxDB 1.x** (optional): live readings plus hourly, daily and monthly consumption history.

Each instance handles one or more meters. Several instances can run side by side through a systemd
template unit, each with its own settings file.

## How it works

Every `SEM6000_INTERVAL` seconds (aligned to the wall clock), the bridge reads each meter's
voltage, current, power, frequency and relay state.

- **Daily energy total:** the meter's own counters plus an estimate for the current hour, made by
  integrating the power readings. At the start of every hour the estimate is replaced by the
  meter's value for that hour.
- **UTC:** the meter clocks are set to UTC, and all hour, day and month handling runs
  in UTC. The daylight-saving switch can't skip or double an hour. See [Time zones](#time-zones-utc).
- **Fault isolation:** if a meter is out of range or Bluetooth fails, only that meter goes
  *unavailable*. It is retried with backoff (interval × 2ⁿ, at most 5 min), and its counters are
  re-read from the meter when it comes back.
- **InfluxDB outages:** points are queued in memory and written once InfluxDB is back. MQTT and
  Home Assistant are not affected.

### Input sanitation

The main goal is that the **totals never decrease** except when a new period starts, because a
drop is what disturbs Home Assistant.

| Value | Rejected when |
|---|---|
| Voltage | ≤ 0 V. No upper limit, so real overvoltage (e.g. 280 V) is still reported |
| Current | < 0 |
| Frequency | more than 5 Hz from 50 Hz (the meter reports whole Hz) |
| Power | < 0 or > `SEM6000_MAX_POWER_W` + 10 %, because it is integrated into the daily total |
| Hourly / daily / monthly counters | more than `SEM6000_MAX_POWER_W` could consume in that period, e.g. a corrupted `0xFFFF` hour |

A rejected measurement is skipped for that interval. A rejected counter is never published, never
written to InfluxDB and never added to the daily total. For the previous hour, the bridge's own
estimate is used instead.

The published **daily total** is also protected:

- **It never decreases within a UTC day, not even across a restart.** The JSON message is retained,
  and at startup the bridge reads back the last published value.
- **It can't exceed max power × hours since midnight.** If it does, the last good value is kept and
  the counters are re-read from the meter.

## Installation

Requires Python ≥ 3.9 and a Bluetooth LE adapter supported by BlueZ. Everything lives under
`/opt/sem6000-wrapper`:

```
/opt/sem6000-wrapper/
├── .venv/              # virtualenv
├── sem6000/            # Put the python3-voltcraft-sem6000 library package here
├── sem6000-wrapper.py
├── requirements.txt
└── README.md
```

The `sem6000` package (python3-voltcraft-sem6000) is not on PyPI. In the source tree it sits next
to this repository and is linked through the `sem6000` symlink. On install it is copied:

```sh
cd ~/src/sem6000-wrapper
sudo install -d /opt/sem6000-wrapper
sudo cp sem6000-wrapper.py requirements.txt README.md /opt/sem6000-wrapper/
sudo cp -r ../python3-voltcraft-sem6000/sem6000 /opt/sem6000-wrapper/      # the library, not the symlink
sudo python3 -m venv /opt/sem6000-wrapper/.venv
sudo /opt/sem6000-wrapper/.venv/bin/pip install -r /opt/sem6000-wrapper/requirements.txt   # bluepy needs libglib2.0-dev
sudo chown -R root:root /opt/sem6000-wrapper && sudo chmod -R go+rX /opt/sem6000-wrapper
```

Find your meters (prints MAC address and name; needs root or the capabilities below):

```sh
sudo /opt/sem6000-wrapper/.venv/bin/python /opt/sem6000-wrapper/sem6000-wrapper.py --discover    # 30 s scan on hci0
```

## Configuration

Settings are environment variables:

- **Under systemd:** they come from `/etc/homeautomation/sem6000-wrapper-<instance>.env` via
  `EnvironmentFile=`. systemd reads that file as root, so it can be private:
  ```sh
  sudo install -d -m 755 /etc/homeautomation
  sudo install -m 600 -o root -g root sem6000-wrapper.env.example /etc/homeautomation/sem6000-wrapper-heatpump.env
  sudoedit /etc/homeautomation/sem6000-wrapper-heatpump.env
  ```
- **For manual runs:** pass the same file with `-e FILE`. Real environment variables take
  precedence over the file.

See [`sem6000-wrapper.env.example`](sem6000-wrapper.env.example) for a commented template. Values may be quoted.
`$` is taken literally, both by systemd and by `-e`.

| Key | Default | Description |
|---|---|---|
| `SEM6000_METERS` | *(required)* | Comma-separated `MAC=PIN` pairs, e.g. `10:CE:A9:E3:79:4B=0000,AA:BB:CC:DD:EE:FF=1234` |
| `SEM6000_INTERVAL` | `10` | Seconds between readings (5–3600) |
| `SEM6000_BLUETOOTH_ADAPTER` | `hci0` | Bluetooth adapter |
| `SEM6000_MAX_POWER_W` | `3680` | Plausibility limit for power and energy counters |
| `SEM6000_ALLOW_SWITCHING` | `true` | Relay switchable from HA/MQTT. `false` = read-only binary sensor (recommended for e.g. a heat pump) |
| `SEM6000_ENTITY_IDS` | empty | Home Assistant entity ids for new entities, as comma-separated `[MAC/]key=entity_id`, e.g. `energy_today=sensor.heatpump_today`. See [Keeping the history of YAML sensors](#keeping-the-history-of-yaml-sensors) |
| `SEM6000_INSTANCE` | `%i` / from file name | Instance name used in topics and the client id. The unit sets it to `%i`. With `-e`, `sem6000-wrapper-heatpump.env` → `heatpump` |
| `MQTT_HOST` | *(required)* | Broker host |
| `MQTT_PORT` | `1883` | |
| `MQTT_USERNAME` / `MQTT_PASSWORD` | empty | |
| `MQTT_CLIENT_ID` | `sem6000-<instance>-<hostname>` | Must be unique per instance |
| `MQTT_TOPIC` | `energymeter` | Base topic for readings |
| `MQTT_SWITCHTOPIC` | `switches/sem6000` | Base topic for relay state and commands |
| `HA_DISCOVERY` | `true` | Publish Home Assistant discovery configs |
| `HA_DISCOVERY_PREFIX` | `homeassistant` | |
| `INFLUXDB_HOST` | empty | Empty disables InfluxDB |
| `INFLUXDB_PORT` | `8086` | |
| `INFLUXDB_SSL` | `false` | |
| `INFLUXDB_USERNAME` / `INFLUXDB_PASSWORD` | empty | |
| `INFLUXDB_DATABASE` | *(required if host set)* | |
| `INFLUXDB_SERIES` | `energymeter` | Measurement name |
| `INFLUXDB_MAX_QUEUE` | `250000` | Points kept in memory while InfluxDB is unreachable. That's about 29 days for one meter at 10 s, roughly 50 MB. Oldest points are dropped first |

At startup the bridge validates every value: MAC format, 4-digit PINs, numeric ranges, and MQTT
topics without `+` or `#`. On an error it exits with code 2, and systemd won't restart it in a
loop. Unknown `SEM6000_*`, `MQTT_*`, `INFLUXDB_*` and `HA_*` keys are logged as warnings to catch
typos.

## Running

### systemd (one or more instances)

[`sem6000-wrapper@.service`](sem6000-wrapper@.service) is a template unit. The part after `@` selects
`/etc/homeautomation/sem6000-wrapper-<instance>.env`:

```sh
sudo cp sem6000-wrapper@.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now sem6000-wrapper@heatpump sem6000-wrapper@wasmachine
systemctl status sem6000-wrapper@heatpump          # shows e.g. "1/1 meter(s) online, last reading 13:37:10Z"
journalctl -u sem6000-wrapper@heatpump -f
```

The unit runs from the base path `WorkingDirectory=/opt/sem6000-wrapper`, and everything else is relative
to it. If you install elsewhere, change that line and the `Documentation=` line.

What the unit does:

- **Unprivileged user:** `DynamicUser=yes` gives each start a transient user, so no system account
  is needed.
- **Bluetooth access:** `AmbientCapabilities=CAP_NET_RAW CAP_NET_ADMIN` and
  `AF_BLUETOOTH` in `RestrictAddressFamilies=`. The capabilities are inherited by `bluepy-helper`,
  so **no `setcap`** on it is needed (file capabilities would be ignored under `NoNewPrivileges`
  anyway).
- **Hardening:** a read-only filesystem, no home directories, no devices, a system call filter and
  more. `systemd-analyze security sem6000-wrapper@heatpump` rates it **1.8 (OK)**.
- **Watchdog and status:** `Type=notify` with a 10 min watchdog, which catches a Bluetooth call that
  really hangs. `systemctl status` shows the live status line.

  Each instance gets its own MQTT client id, availability topic and log identifier
  (`sem6000-wrapper-<instance>`).
- **`PrivateUsers=` and `PrivateNetwork=` are deliberately off.** Capabilities inside a user
  namespace don't count for Bluetooth sockets, and the adapter only exists in the host network
  namespace.
- **If Bluetooth works with `sudo` but not under the unit,** look for `Operation not permitted` in
  the journal. Relax the hardening with a drop-in (`systemctl edit sem6000-wrapper@heatpump`), trying in
  this order: `SystemCallFilter=` (empty resets it), then `PrivateDevices=no`.

Notes on multiple instances:

- **Don't put the same meter in two instances.** A meter accepts only one Bluetooth connection.
- **Concurrent connects on one adapter can fail.** All instances share the adapter. Established
  connections work in parallel, but simultaneous *connection attempts* can fail with "busy". The
  library retries and the bridge backs off, so this sorts itself out after a few seconds.
- **Consider one meter per instance.** A call to an unreachable meter can block for a few minutes
  (the library retries and reconnects). Meanwhile, other meters in the *same* instance are not read.
  Separate instances keep them independent.

### Manually

```sh
sudo /opt/sem6000-wrapper/.venv/bin/python /opt/sem6000-wrapper/sem6000-wrapper.py -e /etc/homeautomation/sem6000-wrapper-heatpump.env -d -o -L DEBUG
```

| Option | |
|---|---|
| `-e, --env-file FILE` | Load settings from a `.env` file (otherwise from the environment) |
| `-L, --loglevel LEVEL` | `CRITICAL`, `ERROR`, `WARNING`, `INFO` (default), `DEBUG`. `DEBUG` also prints the raw Bluetooth traffic |
| `-l, --logfile FILE` | Also log to a file. The file is reopened when an external logrotate moves it, so no `copytruncate` is needed. Under systemd, see `LogsDirectory=` in the unit |
| `-d, --dryrun` | Read the meters but don't connect to MQTT or write to InfluxDB. Everything is logged at `DEBUG` instead |
| `-o, --oneshot` | Do one full read and exit |
| `--discover [SECONDS]` | Scan for meters and exit (`--adapter hciN` to choose an adapter) |

Under journald (`JOURNAL_STREAM` set), log lines carry no timestamp of their own.

## MQTT topics

With the defaults, for meter `10:ce:a9:e3:79:4b` named *heatpump* in instance `heatpump`:

| Topic | Payload |
|---|---|
| `energymeter/10:ce:a9:e3:79:4b` | JSON, see below (retained) |
| `energymeter/10:ce:a9:e3:79:4b/volt`, `/amp`, `/pdw`, `/freq`, `/pf` | V, A, W, Hz, power factor (0–1) |
| `energymeter/10:ce:a9:e3:79:4b/edtoday` | Energy since UTC midnight, Wh |
| `energymeter/10:ce:a9:e3:79:4b/edtoday_lastreset` | ISO timestamp of the last reset (UTC midnight) |
| `energymeter/10:ce:a9:e3:79:4b/availability` | `online` / `offline` (retained) |
| `energymeter/heatpump/availability` | Instance `online` / `offline` (retained, MQTT last will) |
| `switches/sem6000/10:ce:a9:e3:79:4b/state` | `ON` / `OFF` |
| `switches/sem6000/heatpump/state` | Same, keyed by device name (lower case) |
| `switches/sem6000/10:ce:a9:e3:79:4b/set` | **Command**: `ON` / `OFF` (only with `SEM6000_ALLOW_SWITCHING=true`) |

```json
{
  "device": {"mac": "10:ce:a9:e3:79:4b", "name": "heatpump"},
  "status": {"power": 1, "voltage": 230, "ampere": 6.6, "watts": 1500.0, "frequency": 50, "power_factor": 0.99},
  "stats":  {"edtoday": 1234.56, "edtoday_lastreset": "2026-11-14T00:00:00+00:00"}
}
```

`stats` is left out if no plausible daily total is available yet. Anyone who can publish to the
`/set` topics can switch the relays, so restrict them with broker ACLs, or set
`SEM6000_ALLOW_SWITCHING=false` for loads that must never be switched off.

## Home Assistant

You don't need to configure anything in Home Assistant, as long as the MQTT integration is set up
with discovery enabled (the default). Each meter shows up as a device (*Voltcraft SEM6000*) with:

| Entity | Device class | State class | Unit |
|---|---|---|---|
| Power | `power` | `measurement` | W |
| Voltage | `voltage` | `measurement` | V |
| Current | `current` | `measurement` | A |
| Frequency | `frequency` | `measurement` | Hz |
| Power factor | `power_factor` | `measurement` | (ratio) |
| Energy today (UTC) | `energy` | `total_increasing` | Wh |
| Relay | switch `outlet`, or binary_sensor `power` when switching is disabled | | |

Use **Energy today (UTC)** in the Energy dashboard. Entities go *unavailable* when the instance
stops (last will) or when that meter can't be reached. If you change `SEM6000_ALLOW_SWITCHING`, the
relay entity of the other kind is removed automatically.

### Keeping the history of YAML sensors

Home Assistant stores the recorder history and the long-term statistics (the Energy dashboard)
by **entity id**. If you had `mqtt: sensor:` entries in `configuration.yaml` for an older version
of this script, the discovered entities get new entity ids, and the history stays behind. To
continue it, give the new entities the old entity ids with `SEM6000_ENTITY_IDS`.

Keys: `power`, `voltage`, `current`, `frequency`, `power_factor`, `energy_today` (sensor) and
`relay` (switch, or binary_sensor with `SEM6000_ALLOW_SWITCHING=false`). With more than one meter
in an instance, prefix the key with the MAC: `10:CE:A9:E3:79:4B/power=sensor.heatpump_power`.

The setting only takes effect when Home Assistant **creates** an entity, so the order matters:

1. **Look up the old entity ids** in *Settings → Devices & services → Entities* (search for the
   name). A sensor named `heatpump (Today)` usually has `sensor.heatpump_today`.
2. **Check that the units match.** `energy_today` is in Wh with state class `total_increasing`,
   `power` in W with `measurement`. If an old sensor used other units (e.g. kWh), Home Assistant
   reports a statistics unit problem instead of continuing them.
3. **Remove the old sensors from `configuration.yaml`** and restart Home Assistant.
4. **Delete the old entities** in *Settings → Entities*. They now show as no longer provided.
   Deleting them only frees the entity id: the history and statistics stay in the database. Don't
   delete the statistics if *Developer tools → Statistics* offers to.
5. **If this version already ran,** its entities exist with other ids. Stop the service and delete
   the meter's device in *Settings → Devices & services → MQTT*. Home Assistant also clears its
   retained discovery messages; the service republishes them when it starts.
6. **Set `SEM6000_ENTITY_IDS`** in the env file and start the service:
   ```sh
   SEM6000_ENTITY_IDS=energy_today=sensor.heatpump_today,power=sensor.heatpump_power
   ```

The new entities then write to the old entity ids, and the history continues. The old
`energymeter/<MAC>` JSON topic is still published, so the YAML sensors keep working until you
switch.

## InfluxDB

Measurement `INFLUXDB_SERIES` with tags `host`, `meter` (MAC) and `metername`. Tag values are
escaped, so names with spaces or commas are fine. Field types are the same as in the previous
version, so existing data stays compatible.

| Field | When | Timestamp |
|---|---|---|
| `sw`, `volt`, `amp`, `pdw`, `freq`, `pf` | every interval | interval-aligned |
| `edlh` (Wh) | every hour | start of the previous hour |
| `edld` (Wh) | every day | previous UTC midnight |
| `edlm` (Wh) | every month | start of the previous UTC month |

At startup, and after a meter has been unreachable, the bridge reads the history the meter holds
(23 hours, 30 days, 12 months) and writes:

- **The current hour, day and month.** These are partial totals; the rollover at the end of the
  period writes the final value.
- **Previous hours, days and months the meter was seen in.** A period counts if it started at or
  before the meter's last live reading in InfluxDB (the latest `pdw` point for its MAC, from any
  `host` or `metername`). Periods it was only partly seen in, for example because the bridge or the
  meter was down for a while, are updated with the meter's full total.
- **Nothing for the periods after the meter disappeared.** A meter has no battery-backed clock:
  after it lost power and its clock is set again, it keeps its old totals, which could land on the
  wrong hours, days and months.
- **Nothing for a previous period the meter reports as 0 Wh.** That most likely means the meter's
  history was reset.

Two rules apply to every `edlh`/`edld`/`edlm` write, also at the rollover:

- **A total is never lowered.** If InfluxDB already has a higher value for that period (for that
  MAC), it is kept and a warning is logged.
- **A finished period with 0 Wh is not written.** For an idle device that leaves a gap instead of a
  0.

Edge cases:

- **No live reading in InfluxDB at all** (a new meter or a new database): only the current hour, day
  and month are written; the meter's history is not.
- **InfluxDB can't be queried:** at startup only the current periods are written. At the rollover the
  finished period is written anyway (and queued while InfluxDB is down), since there is nothing to
  compare it with.
- **The bridge was down while the meter kept running:** the meter's history for that time is
  correct, but the bridge can't tell that apart from a power loss, so those periods are not filled in.

How the queue behaves:

- **While InfluxDB is unreachable,** points are kept in memory, up to `INFLUXDB_MAX_QUEUE`. Retries
  back off up to 5 min. When InfluxDB is back, the queue is written in chunks of 5000.
- **Rejected points are dropped.** If InfluxDB rejects points as invalid (HTTP 400, e.g. a field
  type conflict), they are dropped so they can't block the queue.
- **On shutdown,** the queue is flushed once. Points still unwritten are lost but logged.

## Time zones (UTC)

The meter itself has no time zone or DST support. It buckets consumption by its own clock, which
the bridge sets at startup and every midnight. Everything runs on **UTC**.

- **No DST bugs:** hours, days and months always have their normal length, so the night the clocks
  change has no skipped or doubled hour.
- **Home Assistant works fine:** *Energy today (UTC)* resets at 00:00 UTC, which is 01:00 local in
  winter and 02:00 in summer. The `total_increasing` state class treats a drop as a reset, whatever
  time it happens, and HA's statistics and Energy dashboard are built from hourly increases. Your
  daily totals in HA still follow *local* days. Only the raw entity value resets at the "odd" local
  time.
- **InfluxDB daily and monthly values are UTC days and months:**
  - When using Grafana, group daily values with `GROUP BY time(1d)` (UTC) to keep them in one bucket each.
  - **Upgrade note:** older `edld`/`edlm` points have local-midnight timestamps (e.g. 23:00Z). The
    first sync after the upgrade writes UTC-midnight points, so for up to 30 days (daily) or
    12 months (monthly) both kinds exist. Use `last()`/`max()` rather than `sum()` over that
    period, or delete the old history points.

**Note:**
The downside of this is that the daily and monthly counters become UTC based which is a problem especially if your time zone deviates significantly from UTC.
I still prefer this behavior since, as long as you don't lose connection to the meters for more than a day, your hourly series can be used as a basis for aggregating longer time periods instead and this is probably more reliable, since i doubt these meters can take things like leap days into account.

If you live in a time zone with a half hour UTC offset, well, your country is beautiful but this script is not for you. But, feel free to ask your friendly neigborhood Claude to add local timezone support back.

## Troubleshooting

- **`Configuration error ...`:** the message names the offending key. Fix it, then
  `systemctl restart sem6000-wrapper@<instance>`.
- **`could not connect to MQTT broker within 30 s`:** wrong host, port or credentials. A refused
  login is logged as `MQTT connection refused: ...`.
- **Meter keeps failing:** check the range and that no other app (for example the phone app) is
  connected to it. Run manually with `-L DEBUG -o` to see the Bluetooth traffic. Also try to position your hardware closer to the SEM6000 meter, since their bluetooth has very poor range.
- **`implausible ...` warnings:** a reading or counter failed the sanity checks and was discarded.
  An occasional one is normal on a weak link. For a load above 3680 W, raise `SEM6000_MAX_POWER_W`.

## License

GPL-3.0, see [LICENSE](LICENSE).
