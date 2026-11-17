#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Read Voltcraft SEM6000 Bluetooth energy meters (sold by Conrad) and publish the readings to MQTT
(including Home Assistant MQTT discovery) and InfluxDB 1.x.

All meter clocks and all hour/day/month bookkeeping run on UTC, so daylight saving time changes
never produce a skipped or doubled hour.
"""
import argparse
import collections
import itertools
import json
import logging
import logging.handlers
import os
import queue
import re
import signal
import socket
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

import paho.mqtt.client as mqtt
from dotenv import dotenv_values
from sem6000 import sem6000

__version__ = '2.0.0'

scriptname = os.path.splitext(os.path.basename(__file__))[0]
hostname   = socket.gethostname()

logger = logging.getLogger()

# The meter reports whole Hz, so on a 50 Hz grid anything further off than this is garbage.
NOMINAL_FREQUENCY_HZ   = 50
FREQUENCY_TOLERANCE_HZ = 5
# Allowed headroom on top of SEM6000_MAX_POWER_W before power or a counter is considered implausible.
MARGIN = 1.1


# --------------------------------------------------------------------------------------------
# Configuration (.env file)
# --------------------------------------------------------------------------------------------

class ConfigError(Exception):
  pass


MAC_RE      = re.compile(r'^([0-9A-F]{2}:){5}[0-9A-F]{2}$')
PIN_RE      = re.compile(r'^\d{4}$')
INSTANCE_RE = re.compile(r'^[A-Za-z0-9_-]{1,64}$')
ADAPTER_RE  = re.compile(r'^hci\d{1,2}$')
SERIES_RE   = re.compile(r'^[A-Za-z0-9_.-]{1,64}$')
OBJECT_RE   = re.compile(r'^[a-z0-9_]{1,128}$')
# Entities per meter, for SEM6000_ENTITY_IDS. The relay is a switch, or a binary_sensor when
# SEM6000_ALLOW_SWITCHING=false.
SENSOR_KEYS = ('power', 'voltage', 'current', 'frequency', 'power_factor', 'energy_today')

KNOWN_KEYS = {
  'SEM6000_INSTANCE', 'SEM6000_METERS', 'SEM6000_INTERVAL', 'SEM6000_BLUETOOTH_ADAPTER',
  'SEM6000_MAX_POWER_W', 'SEM6000_ALLOW_SWITCHING', 'SEM6000_ENTITY_IDS',
  'INFLUXDB_HOST', 'INFLUXDB_PORT', 'INFLUXDB_SSL', 'INFLUXDB_USERNAME', 'INFLUXDB_PASSWORD',
  'INFLUXDB_DATABASE', 'INFLUXDB_SERIES', 'INFLUXDB_MAX_QUEUE',
  'MQTT_HOST', 'MQTT_PORT', 'MQTT_USERNAME', 'MQTT_PASSWORD', 'MQTT_CLIENT_ID', 'MQTT_TOPIC',
  'MQTT_SWITCHTOPIC',
  'HA_DISCOVERY', 'HA_DISCOVERY_PREFIX',
}


@dataclass
class MeterConfig:
  mac: str
  pin: str
  entity_ids: dict = field(default_factory=dict)  # key -> Home Assistant entity id (SEM6000_ENTITY_IDS)


@dataclass
class Config:
  instance: str
  meters: list
  interval: int
  bluetooth_adapter: str
  max_power_w: int
  allow_switching: bool
  influx_host: str
  influx_port: int
  influx_ssl: bool
  influx_username: str
  influx_password: str
  influx_database: str
  influx_series: str
  influx_max_queue: int
  mqtt_host: str
  mqtt_port: int
  mqtt_username: str
  mqtt_password: str
  mqtt_client_id: str
  mqtt_topic: str
  mqtt_switchtopic: str
  ha_discovery: bool
  ha_prefix: str


def _str(raw, key, default=None, required=False):
  value = raw.get(key)
  if value is None or value.strip() == '':
    if required:
      raise ConfigError(key + ' is required')
    return default
  return value.strip()


def _int(raw, key, default, lo, hi):
  value = _str(raw, key)
  if value is None:
    return default
  try:
    number = int(value)
  except ValueError:
    raise ConfigError('{}={!r} is not an integer'.format(key, value)) from None
  if not lo <= number <= hi:
    raise ConfigError('{}={} is outside the allowed range {}..{}'.format(key, number, lo, hi))
  return number


def _bool(raw, key, default):
  value = _str(raw, key)
  if value is None:
    return default
  if value.lower() in ('1', 'true', 'yes', 'on'):
    return True
  if value.lower() in ('0', 'false', 'no', 'off'):
    return False
  raise ConfigError('{}={!r} is not a boolean (use true/false)'.format(key, value))


def _topic(raw, key, default):
  value = _str(raw, key, default).strip('/')
  if value == '' or any(c in value for c in '+#\0'):
    raise ConfigError('{}={!r} is not a valid MQTT topic prefix (must be non-empty, no + or #)'.format(key, value))
  return value


def _meters(raw):
  meters = []
  for item in _str(raw, 'SEM6000_METERS', required=True).split(','):
    item = item.strip()
    if item == '':
      continue
    mac, sep, pin = item.partition('=')
    mac = mac.strip().upper().replace('-', ':')
    pin = pin.strip()
    if not MAC_RE.match(mac):
      raise ConfigError('SEM6000_METERS: {!r} is not a MAC address like AA:BB:CC:DD:EE:FF'.format(mac))
    if not sep or not PIN_RE.match(pin):
      raise ConfigError('SEM6000_METERS: the PIN for {} must be exactly 4 digits (format MAC=PIN)'.format(mac))
    if any(m.mac == mac for m in meters):
      raise ConfigError('SEM6000_METERS: {} is listed twice'.format(mac))
    meters.append(MeterConfig(mac, pin))
  if not meters:
    raise ConfigError('SEM6000_METERS does not contain any meter')
  return meters


def _entity_ids(raw, meters, allow_switching):
  """
  SEM6000_ENTITY_IDS: comma-separated [MAC/]key=entity_id. Sets the entity id Home Assistant gives an
  entity when it first creates it, e.g. to take over the entity id (and so the recorder history and
  statistics) of a sensor defined in configuration.yaml. The MAC may be left out with a single meter.
  """
  value = _str(raw, 'SEM6000_ENTITY_IDS')
  if value is None:
    return
  domains = dict.fromkeys(SENSOR_KEYS, 'sensor')
  domains['relay'] = 'switch' if allow_switching else 'binary_sensor'
  by_mac = {m.mac: m for m in meters}
  for item in value.split(','):
    item = item.strip()
    if item == '':
      continue
    target, sep, entity_id = item.partition('=')
    mac, slash, key = target.strip().rpartition('/')
    key, entity_id = key.strip(), entity_id.strip()
    if not sep or not entity_id:
      raise ConfigError('SEM6000_ENTITY_IDS: {!r} must look like key=entity_id or MAC/key=entity_id'.format(item))
    if key not in domains:
      raise ConfigError('SEM6000_ENTITY_IDS: unknown key {!r} (use one of: {})'.format(key, ', '.join(domains)))
    if slash:
      mac = mac.strip().upper().replace('-', ':')
      if mac not in by_mac:
        raise ConfigError('SEM6000_ENTITY_IDS: {} is not in SEM6000_METERS'.format(mac))
      meter = by_mac[mac]
    elif len(meters) == 1:
      meter = meters[0]
    else:
      raise ConfigError('SEM6000_ENTITY_IDS: {!r} needs a MAC/ prefix, this instance has several meters'.format(item))
    domain, dot, object_id = entity_id.partition('.')
    if domain != domains[key] or not dot or not OBJECT_RE.match(object_id):
      raise ConfigError('SEM6000_ENTITY_IDS: {!r} for {} must look like {}.lower_case_name'.format(
        entity_id, key, domains[key]))
    if key in meter.entity_ids:
      raise ConfigError('SEM6000_ENTITY_IDS: {} of {} is listed twice'.format(key, meter.mac))
    meter.entity_ids[key] = entity_id
  all_ids = [e for m in meters for e in m.entity_ids.values()]
  if len(all_ids) != len(set(all_ids)):
    raise ConfigError('SEM6000_ENTITY_IDS: the same entity id is used twice')


def read_environment(path=None):
  """
  Settings come from environment variables: under systemd via EnvironmentFile=, for manual runs
  optionally from a .env file (-e). Real environment variables take precedence over the file.
  """
  env = {}
  if path:
    if not os.path.isfile(path):
      raise ConfigError('config file not found: ' + path)
    # No interpolation: a '$' in a password must stay a '$' (systemd's EnvironmentFile= doesn't expand either).
    env.update((k, v) for k, v in dotenv_values(path, interpolate=False).items() if v is not None)
  env.update(os.environ)
  return env


def load_config(raw, source=None):
  """Build and validate the configuration from a mapping of environment variables."""
  for key in sorted(k for k in raw if k.startswith(('SEM6000_', 'MQTT_', 'INFLUXDB_', 'HA_')) and k not in KNOWN_KEYS):
    logger.warning("Ignoring unknown configuration key '%s'", key)

  # Instance name: SEM6000_INSTANCE (the systemd unit sets it to %i), else the -e file name without
  # extension and 'sem6000-wrapper-' (or old 'sem6000-') prefix (sem6000-wrapper-heatpump.env ->
  # heatpump), else 'default'.
  default_instance = 'default'
  if source:
    default_instance = os.path.splitext(os.path.basename(source))[0]
    for prefix in ('sem6000-wrapper-', 'sem6000-'):
      if default_instance.startswith(prefix):
        default_instance = default_instance[len(prefix):]
        break
    default_instance = re.sub(r'[^A-Za-z0-9_-]', '_', default_instance)[:64] or 'default'
  instance = _str(raw, 'SEM6000_INSTANCE', default_instance)
  if not INSTANCE_RE.match(instance):
    raise ConfigError('SEM6000_INSTANCE={!r} may only contain letters, digits, _ and -'.format(instance))

  adapter = _str(raw, 'SEM6000_BLUETOOTH_ADAPTER', 'hci0')
  if not ADAPTER_RE.match(adapter):
    raise ConfigError('SEM6000_BLUETOOTH_ADAPTER={!r} must look like hci0'.format(adapter))

  influx_host = _str(raw, 'INFLUXDB_HOST', '')
  influx_ssl = _bool(raw, 'INFLUXDB_SSL', False)
  series = _str(raw, 'INFLUXDB_SERIES', 'energymeter')
  if not SERIES_RE.match(series):
    raise ConfigError('INFLUXDB_SERIES={!r} may only contain letters, digits, _ . and -'.format(series))

  cfg = Config(
    instance          = instance,
    meters            = _meters(raw),
    interval          = _int(raw, 'SEM6000_INTERVAL', 10, 5, 3600),
    bluetooth_adapter = adapter,
    max_power_w       = _int(raw, 'SEM6000_MAX_POWER_W', 3680, 100, 10000),
    allow_switching   = _bool(raw, 'SEM6000_ALLOW_SWITCHING', True),
    influx_host       = influx_host,
    influx_port       = _int(raw, 'INFLUXDB_PORT', 8086, 1, 65535),
    influx_ssl        = influx_ssl,
    influx_username   = _str(raw, 'INFLUXDB_USERNAME', ''),
    influx_password   = _str(raw, 'INFLUXDB_PASSWORD', ''),
    influx_database   = _str(raw, 'INFLUXDB_DATABASE', '', required=bool(influx_host)),
    influx_series     = series,
    influx_max_queue  = _int(raw, 'INFLUXDB_MAX_QUEUE', 250000, 0, 10000000),
    mqtt_host         = _str(raw, 'MQTT_HOST', required=True),
    mqtt_port         = _int(raw, 'MQTT_PORT', 1883, 1, 65535),
    mqtt_username     = _str(raw, 'MQTT_USERNAME', ''),
    mqtt_password     = _str(raw, 'MQTT_PASSWORD', ''),
    mqtt_client_id    = _str(raw, 'MQTT_CLIENT_ID', 'sem6000-{}-{}'.format(instance, hostname)),
    mqtt_topic        = _topic(raw, 'MQTT_TOPIC', 'energymeter'),
    mqtt_switchtopic  = _topic(raw, 'MQTT_SWITCHTOPIC', 'switches/sem6000'),
    ha_discovery      = _bool(raw, 'HA_DISCOVERY', True),
    ha_prefix         = _topic(raw, 'HA_DISCOVERY_PREFIX', 'homeassistant'),
  )
  _entity_ids(raw, cfg.meters, cfg.allow_switching)
  return cfg


# --------------------------------------------------------------------------------------------
# Input sanitation
# --------------------------------------------------------------------------------------------

def clean_name(name):
  """Device names come from the meter; strip padding and control characters."""
  return re.sub(r'[\x00-\x1f\x7f]', '', str(name or '')).strip()[:64]


def topic_level(value):
  """Make a string usable as a single MQTT topic level (no wildcards, separators, control chars)."""
  return re.sub(r'[+#/\x00-\x1f\x7f]', '_', value)


def influx_tag(value):
  """Escape a tag value for the InfluxDB line protocol."""
  value = re.sub(r'[\x00-\x1f\x7f]', '_', str(value)).replace('\\', '/')
  return re.sub(r'([ ,=])', r'\\\1', value) or '_'


class ImplausibleMeasurement(Exception):
  pass


@dataclass
class Measurement:
  is_power_active: bool
  voltage: int          # V
  current: float        # A
  power: float          # W
  frequency: int        # Hz
  power_factor: float


def sanitize_measurement(n, max_power_w):
  v, ma, mw, hz = n.voltage_in_volt, n.current_in_milliampere, n.power_in_milliwatt, n.frequency_in_hertz
  problems = []
  # No upper limit on voltage or current: real overvoltage (e.g. 280 V) must still be reported.
  if not isinstance(v, int) or v <= 0:
    problems.append('voltage={} V'.format(v))
  if not isinstance(ma, int) or ma < 0:
    problems.append('current={} mA'.format(ma))
  # Power is integrated into the daily total, so it is bounded by SEM6000_MAX_POWER_W.
  if not isinstance(mw, int) or not 0 <= mw <= max_power_w * 1000 * MARGIN:
    problems.append('power={} mW'.format(mw))
  if not isinstance(hz, int) or abs(hz - NOMINAL_FREQUENCY_HZ) > FREQUENCY_TOLERANCE_HZ:
    problems.append('frequency={} Hz'.format(hz))
  if problems:
    raise ImplausibleMeasurement(', '.join(problems))

  # Power factor = P / (V * I). Guard against division by zero and rounding pushing it above 1.
  power_factor = 0.0 if mw == 0 or ma == 0 else min(1.0, round(mw / (v * ma), 2))
  return Measurement(bool(n.is_power_active), v, ma / 1000.0, mw / 1000.0, hz, power_factor)


def sanitize_counters(values, max_wh, what, meter):
  """
  Validate a list of Wh counters from the meter. Index i means "i periods ago" (index 0 is the
  current period). A leading None (added by the library for days/months) is dropped, as before.
  Implausible entries are replaced by None so they are never published or added to a total.
  """
  values = list(values or [])
  if values and values[0] is None:
    values.pop(0)
  result = []
  for i, value in enumerate(values):
    if isinstance(value, int) and not isinstance(value, bool) and 0 <= value <= max_wh:
      result.append(value)
    else:
      logger.warning('%s: ignoring implausible %s consumption [%d] = %r Wh (max %.0f Wh)', meter, what, i, value, max_wh)
      result.append(None)
  return result


def first(values, index=0):
  return values[index] if len(values) > index else None


# --------------------------------------------------------------------------------------------
# Time helpers (always UTC)
# --------------------------------------------------------------------------------------------

def utcnow():
  return datetime.now(timezone.utc)


def start_of_hour(dt):
  return dt.replace(minute=0, second=0, microsecond=0)


def start_of_day(dt):
  return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def start_of_previous_month(dt):
  return (start_of_day(dt).replace(day=1) - timedelta(days=1)).replace(day=1)


# --------------------------------------------------------------------------------------------
# Energy meter
# --------------------------------------------------------------------------------------------

class Meter:
  def __init__(self, mcfg, cfg, debug=False):
    self.cfg = cfg
    self.debug = debug
    self.mac = mcfg.mac
    self.mac_lower = mcfg.mac.lower()
    self.pin = mcfg.pin
    self.entity_ids = mcfg.entity_ids
    self.uid = 'sem6000_' + self.mac_lower.replace(':', '')
    self.name = self.mac
    self.device = None

    self.ready = False             # connected, authorized and name known
    self.needs_clock_sync = True
    self.needs_full_sync = True    # re-read all history and rebuild the daily total
    self.available = None          # last availability published to MQTT
    self.failures = 0
    self.retry_at = 0.0            # time.monotonic() before which we don't retry
    self.last_measurement = None
    self.last_mono = None          # time.monotonic() of the last valid measurement

    self.dailytotal_base = 0.0     # Wh in completed hours of today (UTC)
    self.dailytotal_lasthour = 0.0 # Wh in the current hour (device value + integration)
    self.reset_dt = None           # UTC midnight the daily total started at
    self.published_total = None    # (reset_dt, Wh) of the last published daily total
    self.retained_total = None     # (reset_dt, Wh) published before a restart (retained MQTT message)

  def __str__(self):
    return '{} ({})'.format(self.name, self.mac) if self.name != self.mac else self.mac

  @property
  def max_hour_wh(self):
    return self.cfg.max_power_w * MARGIN

  @property
  def dailytotal_lastreset(self):
    return self.reset_dt.isoformat() if self.reset_dt else None

  def init_device(self):
    if self.device is None:
      self.device = sem6000.SEM6000(bluetooth_device=self.cfg.bluetooth_adapter, debug=self.debug)
    self.device.connect(self.mac)
    self.name = clean_name(self.device.request_device_name().device_name) or self.mac
    self.device.authorize(self.pin)
    self.ready = True
    self.needs_clock_sync = True
    self.needs_full_sync = True
    logger.info('%s: connected and authorized', self)

  def sync_clock(self):
    # The meter has no notion of time zones. Give it UTC so its hour/day/month buckets are UTC.
    self.device.change_date_and_time(utcnow().replace(tzinfo=None).isoformat(timespec='seconds'))
    self.needs_clock_sync = False

  def read_measurement(self):
    return sanitize_measurement(self.device.request_measurement(), self.cfg.max_power_w)

  def read_hours(self):
    return sanitize_counters(self.device.request_consumption_of_last_23_hours().consumption_n_hours_ago_in_watt_hour,
                             self.max_hour_wh, 'hourly', self)

  def read_days(self):
    return sanitize_counters(self.device.request_consumption_of_last_30_days().consumption_n_days_ago_in_watt_hour,
                             self.max_hour_wh * 24, 'daily', self)

  def read_months(self):
    return sanitize_counters(self.device.request_consumption_of_last_12_months().consumption_n_months_ago_in_watt_hour,
                             self.max_hour_wh * 24 * 31, 'monthly', self)

  def mark_failed(self, exc):
    self.failures += 1
    delay = min(300, self.cfg.interval * 2 ** min(self.failures - 1, 10))
    self.retry_at = time.monotonic() + delay
    self.needs_clock_sync = True
    self.needs_full_sync = True
    self.last_mono = None
    logger.error('%s: %s: %s (failure %d, retrying in %d s)', self, type(exc).__name__, exc, self.failures, delay)
    logger.debug('Traceback:', exc_info=exc)

  def mark_ok(self):
    if self.failures:
      logger.info('%s: communication restored after %d failure(s)', self, self.failures)
    self.failures = 0

  def integrate(self, measurement, mono):
    """Estimate energy in the current hour from the power reading until the meter reports it."""
    if self.last_mono is not None:
      dt = max(0.0, mono - self.last_mono)
      self.dailytotal_lasthour += dt * measurement.power / 3600.0
    self.last_mono = mono

  def daily_total(self):
    """
    The daily total that gets published, sanitized:
    - never negative and never more than max power * time since the reset;
    - never decreasing within a day, not even across a restart of this script, because Home
      Assistant's total_increasing state class treats a drop as a meter reset (or warns about it).
      It only drops at the start of a new (UTC) day.
    Returns None if no plausible value is available.
    """
    if self.reset_dt is None:
      return None
    raw = self.dailytotal_base + self.dailytotal_lasthour
    hours = max(1.0, (utcnow() - self.reset_dt).total_seconds() / 3600.0)
    upper = self.cfg.max_power_w * MARGIN * hours
    candidates = []
    if self.published_total and self.published_total[0] == self.reset_dt:
      candidates.append(self.published_total[1])
    retained = self.retained_total
    if retained and retained[0] == self.reset_dt and 0 <= retained[1] <= upper:
      candidates.append(retained[1])
    last = max(candidates) if candidates else None

    if not 0 <= raw <= upper:
      logger.warning('%s: implausible daily total %.1f Wh (allowed 0..%.0f Wh); resynchronising', self, raw, upper)
      self.needs_full_sync = True
      value = last
    elif last is not None and raw < last:
      if last - raw > 1:
        logger.debug('%s: daily total would decrease from %.2f to %.2f Wh; holding', self, last, raw)
      value = last
    else:
      value = raw

    if value is None:
      return None
    value = round(value, 2)
    self.published_total = (self.reset_dt, value)
    return value

  def close(self):
    if self.device is not None:
      try:
        self.device.disconnect()
      except Exception as e:
        logger.debug('%s: disconnect failed: %s', self, e)


# --------------------------------------------------------------------------------------------
# InfluxDB
# --------------------------------------------------------------------------------------------

class InfluxUnavailable(Exception):
  """InfluxDB could not be queried."""


def micros(ts):
  return int(round(ts * 1000000))


class InfluxWriter:
  """
  Writes line-protocol points. While InfluxDB is unreachable, points are queued in memory (up to
  INFLUXDB_MAX_QUEUE, oldest dropped first) and written once it is back. Points InfluxDB rejects as
  invalid (HTTP 400, e.g. a field type conflict) are dropped instead of blocking the queue.
  """
  CHUNK = 5000

  def __init__(self, cfg, dryrun):
    from influxdb import InfluxDBClient
    from influxdb.exceptions import InfluxDBClientError
    self._client_error = InfluxDBClientError
    self.cfg = cfg
    self.dryrun = dryrun
    self.queue = collections.deque()
    self.failures = 0
    self.retry_at = 0.0
    self.dropped = 0
    self.client = InfluxDBClient(
      host=cfg.influx_host, port=cfg.influx_port, username=cfg.influx_username,
      password=cfg.influx_password, database=cfg.influx_database,
      ssl=cfg.influx_ssl, verify_ssl=cfg.influx_ssl, timeout=5, retries=1)  # don't stall the loop

  def line(self, meter, fields, ts):
    tags = {'host': hostname, 'meter': meter.mac_lower, 'metername': meter.name}
    return '{series},{tags} {fields} {ts}'.format(
      series=self.cfg.influx_series,
      tags=','.join('{}={}'.format(k, influx_tag(v)) for k, v in tags.items()),
      # str() keeps the field types of the existing series: ints without 'i' are floats, bools 'True'.
      fields=','.join('{}={}'.format(k, v) for k, v in fields.items()),
      ts=micros(ts))

  def _query(self, query):
    """Run an InfluxQL query (timestamps in microseconds). Raises InfluxUnavailable if that fails."""
    logger.debug('InfluxDB query: %s', query)
    try:
      return list(self.client.query(query, epoch='u').get_points())
    except Exception as e:  # pylint: disable=broad-exception-caught  # requests/influxdb raise many types
      raise InfluxUnavailable(str(e)) from e

  def _where(self, meter, start_ts, end_ts=None):
    # Any host or meter name: only the MAC identifies the meter.
    where = '"meter" = \'{}\' AND time >= {}u'.format(meter.mac_lower, micros(start_ts))
    return where + (' AND time <= {}u'.format(micros(end_ts)) if end_ts is not None else '')

  def last_live_reading(self, meter, since_ts):
    """Timestamp (s) of this meter's last live reading since since_ts, or None if there is none."""
    points = self._query('SELECT "pdw" FROM "{}" WHERE {} ORDER BY time DESC LIMIT 1'.format(
      self.cfg.influx_series, self._where(meter, since_ts)))
    return points[0]['time'] / 1000000 if points else None

  def existing_values(self, meter, fieldname, start_ts, end_ts):
    """{timestamp in microseconds: value} of field between start_ts and end_ts (inclusive)."""
    values = {}
    for point in self._query('SELECT "{}" FROM "{}" WHERE {}'.format(
        fieldname, self.cfg.influx_series, self._where(meter, start_ts, end_ts))):
      if point.get(fieldname) is not None:
        values[point['time']] = max(point[fieldname], values.get(point['time'], point[fieldname]))
    return values

  def write(self, lines):
    for line in lines:
      logger.debug('%s data to Influx: %s', 'Would have written' if self.dryrun else 'Writing', line)
    if self.dryrun:
      return
    self.queue.extend(lines)
    overflow = len(self.queue) - self.cfg.influx_max_queue
    if overflow > 0:
      for _ in range(overflow):
        self.queue.popleft()
      if self.dropped == 0:
        logger.warning('InfluxDB queue full (%d points); dropping the oldest points', self.cfg.influx_max_queue)
      self.dropped += overflow
    if self.queue and time.monotonic() >= self.retry_at:
      self.flush()

  def flush(self):
    """Write the queue in chunks. Returns True if it is empty afterwards."""
    while self.queue:
      chunk = list(itertools.islice(self.queue, self.CHUNK))
      try:
        self.client.write_points(chunk, time_precision='u', protocol='line')
      except self._client_error as e:
        if e.code == 400:
          logger.error('InfluxDB rejected %d point(s), dropping them: %s', len(chunk), e)
        else:
          return self._failed(e)
      except Exception as e:
        return self._failed(e)
      for _ in range(len(chunk)):
        self.queue.popleft()
    if self.failures:
      logger.info('InfluxDB reachable again, queued points written%s',
                  ' ({} oldest points were dropped)'.format(self.dropped) if self.dropped else '')
    self.failures = 0
    self.dropped = 0
    return True

  def _failed(self, exc):
    # Don't let an InfluxDB outage stop MQTT / Home Assistant: keep the points and retry later.
    self.failures += 1
    delay = min(300, 10 * 2 ** min(self.failures - 1, 5))
    self.retry_at = time.monotonic() + delay
    logger.error('InfluxDB write failed: %s; %d point(s) queued, retrying in %d s', exc, len(self.queue), delay)
    return False

  def close(self):
    if self.queue and not self.dryrun and not self.flush():
      logger.warning('Exiting with %d unwritten InfluxDB point(s); they are lost', len(self.queue))
    try:
      self.client.close()
    except Exception:
      pass


# --------------------------------------------------------------------------------------------
# MQTT + Home Assistant discovery (paho-mqtt 2.x API)
# --------------------------------------------------------------------------------------------

class MqttBridge:
  def __init__(self, cfg, meters, dryrun):
    self.cfg = cfg
    self.dryrun = dryrun
    self.meters = meters
    self.by_mac = {m.mac_lower: m for m in meters}
    self.commands = queue.Queue(maxsize=100)
    self.connected = threading.Event()
    self.resync_needed = threading.Event()  # set on (re)connect: republish discovery/availability
    self.last_error = None
    self.availability_topic = '{}/{}/availability'.format(cfg.mqtt_topic, cfg.instance)
    # The JSON message is retained. Read it back once at startup so the daily total continues from
    # the last published value instead of possibly dropping a little after a restart.
    self._seed_topics = {self.meter_topic(m): m for m in meters}
    self._seed_subscribed = False

    self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=cfg.mqtt_client_id)
    if cfg.mqtt_username:
      self.client.username_pw_set(cfg.mqtt_username, cfg.mqtt_password)
    self.client.will_set(self.availability_topic, 'offline', qos=1, retain=True)
    self.client.reconnect_delay_set(min_delay=1, max_delay=60)
    # Bound the QoS>0 queue during broker outages so memory can't grow without limit.
    self.client.max_queued_messages_set(1000)
    self.client.on_connect = self._on_connect
    self.client.on_disconnect = self._on_disconnect
    self.client.on_message = self._on_message

  # Topics -----------------------------------------------------------------------------------

  def meter_topic(self, m):
    return '{}/{}'.format(self.cfg.mqtt_topic, m.mac_lower)

  def meter_availability_topic(self, m):
    return self.meter_topic(m) + '/availability'

  def switch_topics(self, m):
    base = self.cfg.mqtt_switchtopic
    return ('{}/{}/state'.format(base, m.mac_lower),
            '{}/{}/state'.format(base, topic_level(m.name.lower())))

  def command_topic(self, m):
    return '{}/{}/set'.format(self.cfg.mqtt_switchtopic, m.mac_lower)

  # Connection -------------------------------------------------------------------------------

  def start(self, timeout=30):
    if self.dryrun:
      logger.info('Dry run: not connecting to MQTT')
      return
    logger.info('Connecting to MQTT broker %s:%d as %s', self.cfg.mqtt_host, self.cfg.mqtt_port, self.cfg.mqtt_client_id)
    self.client.connect_async(self.cfg.mqtt_host, self.cfg.mqtt_port, keepalive=60)
    self.client.loop_start()
    if not self.connected.wait(timeout):
      self.client.loop_stop()
      raise RuntimeError('could not connect to MQTT broker within {} s: {}'.format(timeout, self.last_error or 'no response'))

  def stop(self):
    if self.dryrun:
      return
    try:
      if self.connected.is_set():
        for m in self.meters:
          self.publish_availability(m, False)
        self.client.publish(self.availability_topic, 'offline', qos=1, retain=True).wait_for_publish(2)
      self.client.disconnect()
    except Exception as e:
      logger.debug('MQTT shutdown: %s', e)
    self.client.loop_stop()

  def _on_connect(self, client, userdata, flags, reason_code, properties):
    if reason_code.is_failure:
      self.last_error = 'connection refused: {}'.format(reason_code)
      logger.error('MQTT %s', self.last_error)
      return
    logger.info('MQTT client connected (%s)', reason_code)
    client.publish(self.availability_topic, 'online', qos=1, retain=True)
    if not self._seed_subscribed:
      self._seed_subscribed = True
      for topic in list(self._seed_topics):
        client.subscribe(topic, qos=1)
    if self.cfg.allow_switching:
      for m in self.meters:
        client.subscribe(self.command_topic(m), qos=1)
    self.resync_needed.set()
    self.connected.set()

  def _on_disconnect(self, client, userdata, disconnect_flags, reason_code, properties):
    self.connected.clear()
    if reason_code.is_failure:
      logger.warning('MQTT client disconnected: %s (will reconnect)', reason_code)
    else:
      logger.info('MQTT client disconnected')

  def _on_message(self, client, userdata, message):
    # Runs in the paho network thread: only parse and queue; Bluetooth is handled by the main loop.
    seed_meter = self._seed_topics.pop(message.topic, None)
    if seed_meter is not None:
      client.unsubscribe(message.topic)
      self._seed(seed_meter, message.payload)
      return
    parts = message.topic.split('/')
    meter = self.by_mac.get(parts[-2]) if len(parts) >= 2 and parts[-1] == 'set' else None
    payload = message.payload.decode('utf-8', 'replace').strip().upper()
    if meter is None:
      logger.debug('Ignoring MQTT message on %s', message.topic)
      return
    if payload in ('ON', '1', 'TRUE'):
      turn_on = True
    elif payload in ('OFF', '0', 'FALSE'):
      turn_on = False
    else:
      logger.warning('Ignoring invalid switch command %r for %s', payload[:20], meter)
      return
    try:
      self.commands.put_nowait((meter, turn_on))
    except queue.Full:
      logger.warning('Switch command queue full; dropping command for %s', meter)

  def _seed(self, meter, payload):
    try:
      stats = json.loads(payload.decode('utf-8')).get('stats') or {}
      reset_dt = datetime.fromisoformat(stats['edtoday_lastreset'])
      value = float(stats['edtoday'])
    except (ValueError, KeyError, TypeError, AttributeError, UnicodeDecodeError):
      return
    if reset_dt.tzinfo is None or value < 0:
      return
    meter.retained_total = (reset_dt, value)
    logger.info('%s: last published daily total was %.2f Wh (since %s)', meter, value, reset_dt.isoformat())

  # Publishing -------------------------------------------------------------------------------

  def _publish(self, topic, payload, qos=0, retain=False):
    if self.dryrun:
      logger.debug('Would publish to MQTT %s: %s', topic, payload)
      return
    info = self.client.publish(topic, payload, qos=qos, retain=retain)
    if info.rc != mqtt.MQTT_ERR_SUCCESS:
      logger.debug('MQTT publish to %s failed: %s', topic, mqtt.error_string(info.rc))

  def publish_availability(self, m, available, force=False):
    if force or m.available != available:
      self._publish(self.meter_availability_topic(m), 'online' if available else 'offline', qos=1, retain=True)
      m.available = available

  def publish_switch_state(self, m, is_on):
    for topic in self.switch_topics(m):
      self._publish(topic, 'ON' if is_on else 'OFF')

  def publish_data(self, m):
    meas = m.last_measurement
    total = m.daily_total()
    topic = self.meter_topic(m)
    jsonmessage = {
      'device': {'mac': m.mac_lower, 'name': m.name},
      'status': {
        'power': 1 if meas.is_power_active else 0,
        'voltage': meas.voltage,
        'ampere': meas.current,
        'watts': meas.power,
        'frequency': meas.frequency,
        'power_factor': meas.power_factor,
      },
    }
    if total is not None:
      jsonmessage['stats'] = {'edtoday': total, 'edtoday_lastreset': m.dailytotal_lastreset}
    logger.debug('%s data to MQTT: %s', 'Would publish' if self.dryrun else 'Publishing', json.dumps(jsonmessage))

    self.publish_switch_state(m, meas.is_power_active)
    self._publish(topic, json.dumps(jsonmessage), retain=True)
    self._publish(topic + '/volt', str(meas.voltage))
    self._publish(topic + '/amp', str(meas.current))
    self._publish(topic + '/pdw', str(meas.power))
    self._publish(topic + '/freq', str(meas.frequency))
    self._publish(topic + '/pf', str(meas.power_factor))
    if total is not None:
      self._publish(topic + '/edtoday', str(total))
      self._publish(topic + '/edtoday_lastreset', m.dailytotal_lastreset)
    # use edlh, edld, edlm for hourly, daily, monthly totals (InfluxDB)

  def publish_discovery(self, m):
    if not self.cfg.ha_discovery:
      return
    prefix = self.cfg.ha_prefix
    topic = self.meter_topic(m)
    common = {
      'device': {
        'identifiers': [m.uid],
        'connections': [['mac', m.mac_lower]],
        'name': m.name,
        'manufacturer': 'Voltcraft',
        'model': 'SEM6000',
      },
      'origin': {'name': scriptname, 'sw_version': __version__},
      'availability': [{'topic': self.availability_topic}, {'topic': self.meter_availability_topic(m)}],
      'availability_mode': 'all',
    }
    sensors = [
      # key,          name,                 topic,      unit,  device_class,   state_class
      ('power',        'Power',              '/pdw',     'W',   'power',        'measurement'),
      ('voltage',      'Voltage',            '/volt',    'V',   'voltage',      'measurement'),
      ('current',      'Current',            '/amp',     'A',   'current',      'measurement'),
      ('frequency',    'Frequency',          '/freq',    'Hz',  'frequency',    'measurement'),
      ('power_factor', 'Power factor',       '/pf',      None,  'power_factor', 'measurement'),
      # Resets at UTC midnight. total_increasing works for the Energy dashboard regardless of when
      # the reset happens: Home Assistant only uses the hourly increases.
      ('energy_today', 'Energy today (UTC)', '/edtoday', 'Wh',  'energy',       'total_increasing'),
    ]
    for key, name, suffix, unit, device_class, state_class in sensors:
      config = dict(common, name=name, unique_id='{}_{}'.format(m.uid, key), state_topic=topic + suffix,
                    device_class=device_class, state_class=state_class)
      if unit:
        config['unit_of_measurement'] = unit
      if key in m.entity_ids:
        # Only used when Home Assistant creates the entity; renaming it in HA later still works.
        config['default_entity_id'] = m.entity_ids[key]
      self._publish('{}/sensor/{}/{}/config'.format(prefix, m.uid, key), json.dumps(config), qos=1, retain=True)

    relay = dict(common, name='Relay', unique_id=m.uid + '_relay', state_topic=self.switch_topics(m)[0],
                 payload_on='ON', payload_off='OFF')
    if self.cfg.allow_switching:
      relay.update(command_topic=self.command_topic(m), state_on='ON', state_off='OFF', device_class='outlet')
      component, other = 'switch', 'binary_sensor'
    else:
      relay.update(device_class='power')
      component, other = 'binary_sensor', 'switch'
    if 'relay' in m.entity_ids:
      relay['default_entity_id'] = m.entity_ids['relay']
    self._publish('{}/{}/{}/relay/config'.format(prefix, component, m.uid), json.dumps(relay), qos=1, retain=True)
    # Remove the entity of the other kind in case SEM6000_ALLOW_SWITCHING was changed.
    self._publish('{}/{}/{}/relay/config'.format(prefix, other, m.uid), '', qos=1, retain=True)

  def resync(self):
    """After an MQTT (re)connect: republish discovery and availability (the broker may have lost them)."""
    self.resync_needed.clear()
    for m in self.meters:
      if m.ready:
        self.publish_discovery(m)
      if m.available is not None:
        self.publish_availability(m, m.available, force=True)


# --------------------------------------------------------------------------------------------
# systemd integration (sd_notify protocol, no dependency needed)
# --------------------------------------------------------------------------------------------

class Notifier:
  """Minimal sd_notify(3): READY/STATUS/STOPPING/WATCHDOG over $NOTIFY_SOCKET. A no-op outside systemd."""

  def __init__(self, env=None):
    env = os.environ if env is None else env
    self.sock = None
    self.addr = env.get('NOTIFY_SOCKET', '')
    if self.addr:
      if self.addr.startswith('@'):
        self.addr = '\0' + self.addr[1:]  # abstract namespace socket
      self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM | socket.SOCK_CLOEXEC)
    self.watchdog_interval = None
    try:
      usec = int(env.get('WATCHDOG_USEC', '0') or 0)
      pid = int(env.get('WATCHDOG_PID', '0') or 0)
    except ValueError:
      usec = pid = 0
    if usec > 0 and pid in (0, os.getpid()):
      self.watchdog_interval = usec / 1e6 / 2  # sd_watchdog_enabled(3): ping at half the timeout
    self.last_ping = 0.0

  def notify(self, *messages):
    if self.sock is None:
      return
    try:
      self.sock.sendto('\n'.join(messages).encode(), self.addr)
    except OSError as e:
      logger.debug('sd_notify failed: %s', e)

  def ping(self):
    """Called from the main loop. If the loop hangs (e.g. in a Bluetooth call), systemd restarts us."""
    if self.watchdog_interval is not None and time.monotonic() - self.last_ping >= min(5.0, self.watchdog_interval):
      self.notify('WATCHDOG=1')
      self.last_ping = time.monotonic()


notifier = Notifier({})  # replaced in main()


# --------------------------------------------------------------------------------------------
# Main loop
# --------------------------------------------------------------------------------------------

def full_sync(m, now, influx, lines):
  """Read all history from the meter and rebuild the daily total (also used after failures)."""
  hours, days, months = m.read_hours(), m.read_days(), m.read_months()

  m.reset_dt = start_of_day(now)
  m.dailytotal_lasthour = first(hours) or 0.0
  # Energy consumed up until the last hour.
  if first(days) is not None:
    m.dailytotal_base = first(days)
  else:
    # Fallback: sum today's completed hours (index 1..now.hour).
    m.dailytotal_base = sum(h for h in hours[1:now.hour + 1] if h is not None)
    logger.warning('%s: no valid daily total from meter; using sum of hours: %.0f Wh', m, m.dailytotal_base)
  logger.info('%s: synchronised counters, today so far %.0f Wh', m, m.dailytotal_base + m.dailytotal_lasthour)

  if influx:
    startofhour = start_of_hour(now)
    startofday = start_of_day(now)
    monthstarts = [startofday.replace(day=1)]
    while len(monthstarts) < len(months):
      monthstarts.append(start_of_previous_month(monthstarts[-1]))
    try:
      last_seen = influx.last_live_reading(m, monthstarts[-1].timestamp())
    except InfluxUnavailable as e:
      logger.warning('%s: InfluxDB could not be queried (%s); only writing the current hour, day and month', m, e)
      last_seen = UNKNOWN
    if last_seen is None:
      logger.info('%s: no earlier live readings in InfluxDB; only writing the current hour, day and month', m)
    elif last_seen is not UNKNOWN:
      logger.info('%s: last live reading in InfluxDB at %s', m,
                  datetime.fromtimestamp(last_seen, timezone.utc).isoformat(timespec='seconds'))
    write_history(m, influx, lines, 'edlh', hours, [startofhour - timedelta(hours=i) for i in range(len(hours))],
                  last_seen)
    write_history(m, influx, lines, 'edld', days, [startofday - timedelta(days=i) for i in range(len(days))],
                  last_seen)
    write_history(m, influx, lines, 'edlm', months, monthstarts, last_seen)
  m.needs_full_sync = False


UNKNOWN = object()  # last_seen when InfluxDB could not be queried


def write_history(m, influx, lines, fieldname, values, starts, last_seen):
  """
  Write one series of hourly/daily/monthly totals after a (re)sync. values[i] belongs to the period
  starting at starts[i]; index 0 is the current period.

  - The current period is written (its partial total is overwritten at the rollover).
  - A previous period is only written if the meter was seen in it: it started at or before the meter's
    last live reading in InfluxDB (last_seen). A meter that lost power keeps its old totals after its
    clock is set again; for the periods after it disappeared they would land on the wrong
    hours/days/months. Periods it was only partly seen in are updated with the meter's full total.
  - A previous period with 0 Wh is skipped: it most likely means the meter's history was reset.
  - A total is never lowered below the value already in InfluxDB (warning instead).
  """
  candidates = []
  zero = gap = 0
  if values and values[0] is not None:
    candidates.append((starts[0].timestamp(), values[0]))
  for start, value in zip(starts[1:], values[1:]):
    if value is None:
      continue  # implausible, already logged
    if value == 0:
      zero += 1
    elif last_seen is None or last_seen is UNKNOWN or start.timestamp() > last_seen:
      gap += 1
    else:
      candidates.append((start.timestamp(), value))
  if not candidates:
    return
  try:
    existing = influx.existing_values(m, fieldname, min(ts for ts, _ in candidates), max(ts for ts, _ in candidates))
  except InfluxUnavailable as e:
    # Can't compare. The current period is still written (queued while InfluxDB is down), the rest isn't.
    logger.warning('%s: cannot read existing %s values from InfluxDB (%s); only writing the current period',
                   m, fieldname, e)
    existing = {}
    has_current = bool(values) and values[0] is not None
    candidates = candidates[:1] if has_current else []
  written = unchanged = lower = 0
  for ts, value in candidates:
    old = existing.get(micros(ts))
    if old is not None and value < old:
      lower += 1
      logger.warning('%s: %s for %s would go down from %s to %s Wh; keeping the value in InfluxDB',
                     m, fieldname, datetime.fromtimestamp(ts, timezone.utc).isoformat(), old, value)
    elif old is not None and value == old:
      unchanged += 1
    else:
      written += 1
      lines.append(influx.line(m, {fieldname: value}, ts))
  logger.info('%s: %s: %d written, %d unchanged, %d not lowered, %d skipped as 0 Wh, %d skipped (meter not seen '
              'then)', m, fieldname, written, unchanged, lower, zero, gap)


def write_total(m, influx, lines, fieldname, value, start):
  """Write the total of a period that just ended (at the rollover), with the same 0 Wh and never-lower rules."""
  if value is None:
    return
  ts = start.timestamp()
  if value == 0:
    # Normal for an idle device, or the meter's history was reset: either way, don't write it.
    logger.debug('%s: meter reports 0 Wh for %s %s; not writing it', m, fieldname, start.isoformat())
    return
  try:
    old = influx.existing_values(m, fieldname, ts, ts).get(micros(ts))
  except InfluxUnavailable as e:
    logger.warning('%s: cannot read the existing %s value from InfluxDB (%s); writing it anyway', m, fieldname, e)
    old = None
  if old is not None and value < old:
    logger.warning('%s: %s for %s would go down from %s to %s Wh; keeping the value in InfluxDB',
                   m, fieldname, start.isoformat(), old, value)
    return
  lines.append(influx.line(m, {fieldname: value}, ts))


def rollover(m, now, newday, newmonth, influx, lines):
  """Called at the start of every new UTC hour; newday/newmonth imply a new hour as well."""
  hours = m.read_hours()
  previous_hour = first(hours, 1)
  if previous_hour is None:
    previous_hour = m.dailytotal_lasthour
    logger.warning('%s: no valid consumption for the previous hour; using estimate %.1f Wh', m, previous_hour)
  m.dailytotal_base += previous_hour
  m.dailytotal_lasthour = first(hours) or 0.0
  if influx:
    write_total(m, influx, lines, 'edlh', first(hours, 1), start_of_hour(now) - timedelta(hours=1))

  if newday:
    days = m.read_days()
    m.sync_clock()  # keep the meter's clock from drifting
    m.reset_dt = start_of_day(now)
    m.dailytotal_base = 0.0
    if influx:
      write_total(m, influx, lines, 'edld', first(days, 1), start_of_day(now) - timedelta(days=1))

  if newmonth:
    months = m.read_months()
    if influx:
      write_total(m, influx, lines, 'edlm', first(months, 1), start_of_previous_month(now))


def switch_relay(m, turn_on, mq):
  if not m.ready or time.monotonic() < m.retry_at:
    logger.warning('%s: not reachable, ignoring switch command', m)
    return
  logger.info('%s: switching %s (requested via MQTT)', m, 'on' if turn_on else 'off')
  try:
    if turn_on:
      m.device.power_on()
    else:
      m.device.power_off()
  except Exception as e:
    m.mark_failed(e)
    mq.publish_availability(m, False)
    return
  mq.publish_switch_state(m, turn_on)


def wait_for_next_tick(interval, mq):
  """Sleep until the next multiple of the interval, handling switch commands meanwhile."""
  deadline = time.time() + interval - time.time() % interval
  while True:
    notifier.ping()
    if mq.resync_needed.is_set():
      mq.resync()
    remaining = deadline - time.time()
    if remaining <= 0:
      return
    try:
      meter, turn_on = mq.commands.get(timeout=min(remaining, 1.0))
    except queue.Empty:
      continue
    switch_relay(meter, turn_on, mq)


def run(cfg, meters, mq, influx, oneshot=False):
  prevnow = None
  wait_for_next_tick(cfg.interval, mq)
  while True:
    now = utcnow()
    mono = time.monotonic()
    ts = now.timestamp() - now.timestamp() % cfg.interval
    newhour = newday = newmonth = False
    if prevnow is not None:
      if now < prevnow or now - prevnow > timedelta(hours=1):
        logger.warning('Clock jumped from %s to %s; resynchronising all meters', prevnow.isoformat(), now.isoformat())
        for m in meters:
          m.needs_full_sync = True
      elif (now.year, now.month) != (prevnow.year, prevnow.month):
        newmonth = newday = newhour = True
      elif now.date() != prevnow.date():
        newday = newhour = True
      elif now.hour != prevnow.hour:
        newhour = True
    prevnow = now
    lines = []

    # Do the regular update first
    reachable = []
    for m in meters:
      notifier.ping()
      if mono < m.retry_at:
        continue
      try:
        if not m.ready:
          m.init_device()
          mq.publish_discovery(m)
        if m.needs_clock_sync:
          m.sync_clock()
        measurement = m.read_measurement()
      except ImplausibleMeasurement as e:
        logger.warning('%s: discarding implausible measurement: %s', m, e)
        if newhour:
          m.needs_full_sync = True  # we skip the rollover for this meter, so catch up later
        continue
      except Exception as e:
        m.mark_failed(e)
        mq.publish_availability(m, False)
        continue
      m.mark_ok()
      mq.publish_availability(m, True)
      m.integrate(measurement, mono)
      m.last_measurement = measurement
      reachable.append(m)
      if influx:
        lines.append(influx.line(m, {
          'sw':   measurement.is_power_active,
          'volt': measurement.voltage,
          'amp':  measurement.current,
          'pdw':  measurement.power,
          'freq': measurement.frequency,
          'pf':   measurement.power_factor,
        }, ts))
      if not (newhour or m.needs_full_sync):
        mq.publish_data(m)

    # Read the consumption counters at the start of every hour, or when (re)synchronising.
    due = [m for m in reachable if newhour or m.needs_full_sync]
    if due:
      # Give the meters some time to update their internal counters.
      # Yes, at short intervals this may mean skipping a beat. Deal with it.
      time.sleep(5)
    for m in due:
      notifier.ping()
      try:
        if m.needs_full_sync:
          full_sync(m, now, influx, lines)
        else:
          rollover(m, now, newday, newmonth, influx, lines)
      except Exception as e:
        m.mark_failed(e)
        mq.publish_availability(m, False)
        continue
      mq.publish_data(m)

    if influx:
      influx.write(lines)
    online = sum(1 for m in meters if m.available)
    notifier.notify('STATUS={}/{} meter(s) online, last reading {}{}'.format(
      online, len(meters), now.strftime('%H:%M:%SZ'),
      ', {} InfluxDB point(s) queued'.format(len(influx.queue)) if influx and influx.queue else ''))
    if oneshot:
      return
    wait_for_next_tick(cfg.interval, mq)


def discover(adapter, seconds):
  logger.info('Scanning for SEM6000 meters on %s for %d s...', adapter, seconds)
  devices = sem6000.SEM6000.discover(timeout=seconds, bluetooth_device=adapter)
  for device in devices:
    print('{}  {}'.format(device['address'].upper(), device.get('name') or ''))
  if not devices:
    print('No SEM6000 meters found.')


def _sigterm(signum, frame):
  # systemd stops services with SIGTERM; turn it into SystemExit so cleanup in 'finally' runs.
  raise SystemExit(0)


def main():
  global notifier
  parser = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
  parser.add_argument('-l', '--logfile',  help="Also log to this file (reopened when logrotate moves it)")
  parser.add_argument('-L', '--loglevel', help="Logging level (default: INFO)", type=str.upper, default='INFO',
                      choices=['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG'])
  parser.add_argument('-d', '--dryrun',   help="Dry run (do not write to InfluxDB or MQTT)", action='store_true')
  parser.add_argument('-e', '--env-file', help="Load settings from this .env file (real environment variables take "
                                               "precedence). Under systemd, EnvironmentFile= is used instead.")
  parser.add_argument('-o', '--oneshot',  help="Run once", action='store_true')
  parser.add_argument('--discover',       help="Scan for meters for SECONDS (default 30) and exit", nargs='?',
                      const=30, type=int, metavar='SECONDS')
  parser.add_argument('--adapter',        help="Bluetooth adapter for --discover (default: hci0)", default='hci0')
  parser.add_argument('--version',        action='version', version='%(prog)s ' + __version__)
  args = parser.parse_args()

  own_loglevel = getattr(logging, args.loglevel)
  logger.setLevel(own_loglevel)
  formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
  # journald adds its own timestamps
  stream_formatter = logging.Formatter('%(levelname)s - %(message)s') if os.environ.get('JOURNAL_STREAM') else formatter
  handlers = [(logging.StreamHandler(), stream_formatter)]
  if args.logfile:
    # WatchedFileHandler reopens the file after an external logrotate moved it.
    handlers.append((logging.handlers.WatchedFileHandler(args.logfile), formatter))
  for handler, fmt in handlers:
    handler.setLevel(own_loglevel)
    handler.setFormatter(fmt)
    logger.addHandler(handler)

  if args.discover is not None:
    discover(args.adapter, args.discover)
    return 0

  logger.info('*** %s %s starting... ***', scriptname, __version__)
  try:
    if args.env_file:
      logger.info('Loading configuration file: %s', os.path.abspath(args.env_file))
    cfg = load_config(read_environment(args.env_file), args.env_file)
  except ConfigError as e:
    logger.critical('Configuration error: %s%s', e, '' if args.env_file else ' (set it in the environment or use -e FILE)')
    return 2
  logger.info('Instance %s: %d meter(s), interval %d s, InfluxDB %s, Home Assistant discovery %s',
              cfg.instance, len(cfg.meters), cfg.interval, 'enabled' if cfg.influx_host else 'disabled',
              'enabled' if cfg.ha_discovery else 'disabled')

  debug = args.loglevel == 'DEBUG'
  meters = [Meter(mcfg, cfg, debug) for mcfg in cfg.meters]
  influx = InfluxWriter(cfg, args.dryrun) if cfg.influx_host else None
  mq = MqttBridge(cfg, meters, args.dryrun)

  notifier = Notifier()
  if notifier.watchdog_interval:
    logger.info('systemd watchdog enabled, pinging every %.0f s at most', min(5.0, notifier.watchdog_interval))
  signal.signal(signal.SIGTERM, _sigterm)
  try:
    mq.start()
    notifier.notify('READY=1', 'STATUS=Connected to MQTT, reading meters')
    run(cfg, meters, mq, influx, args.oneshot)
  except KeyboardInterrupt:
    pass
  except SystemExit:
    raise
  except Exception:
    logger.critical('Unexpected error, exiting', exc_info=True)
    return 1
  finally:
    logger.info('Shutting down')
    notifier.notify('STOPPING=1', 'STATUS=Shutting down')
    mq.stop()
    for m in meters:
      m.close()
    if influx:
      influx.close()
  return 0


if __name__ == '__main__':
  sys.exit(main())
