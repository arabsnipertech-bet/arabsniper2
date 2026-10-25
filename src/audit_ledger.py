"""Disposable, disk-backed working index for immutable forecast JSON.

The durable format remains checksummed JSON. SQLite exists only in a temporary
directory and bounds RAM while importing, merging, settling and exporting it.
"""
from collections.abc import Mapping
from datetime import datetime
import json
from pathlib import Path
import sqlite3
import tempfile
from zoneinfo import ZoneInfo

from audit_storage import open_verified_json, publish_spool

COLLECTIONS = ('fixtures', 'quality_evaluations', 'multimarket_evaluations',
               'signal_observations', 'analysis_updates')


def encoded(value):
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'), allow_nan=False)


def entries(path):
    """Yield metadata or one forecast at a time, including keys containing dots."""
    import ijson
    from ijson.common import ObjectBuilder

    def value(events, first):
        builder = ObjectBuilder()
        _, event, item = first
        builder.event(event, item)
        depth = int(event in ('start_map', 'start_array'))
        while depth:
            _, event, item = next(events)
            builder.event(event, item)
            depth += int(event in ('start_map', 'start_array'))
            depth -= int(event in ('end_map', 'end_array'))
        return builder.value

    with open_verified_json(path) as stream:
        events = iter(ijson.parse(stream, use_float=True))
        if next(events, None) != ('', 'start_map', None):
            raise ValueError('Audit root must be an object')
        for prefix, event, item in events:
            if prefix == '' and event == 'end_map':
                # Exhaust the parser so trailing invalid JSON cannot be ignored.
                if next(events, None) is not None:
                    raise ValueError('Trailing audit data')
                return
            if prefix != '' or event != 'map_key':
                raise ValueError('Invalid audit structure')
            name = item
            first = next(events)
            if name not in COLLECTIONS:
                yield None, name, value(events, first)
                continue
            if first != (name, 'start_map', None):
                raise ValueError(f'Audit collection must be an object: {name}')
            # Preserve even empty collections in exported files.
            yield name, None, None
            for prefix, event, item in events:
                if prefix == name and event == 'end_map':
                    break
                if prefix != name or event != 'map_key':
                    raise ValueError('Invalid audit record key')
                record = value(events, next(events))
                if not isinstance(record, dict):
                    raise ValueError('Audit record must be an object')
                yield name, item, record


class Collection(Mapping):
    def __init__(self, ledger, name):
        self.ledger, self.name = ledger, name

    def __len__(self):
        return self.ledger.db.execute('SELECT count(*) FROM records WHERE collection=?',
                                      (self.name,)).fetchone()[0]

    def __iter__(self):
        return (key for key, in self.ledger.db.execute(
            'SELECT key FROM records WHERE collection=? ORDER BY rowid', (self.name,)))

    def __getitem__(self, key):
        row = self.ledger.db.execute('SELECT value FROM records WHERE collection=? AND key=?',
                                     (self.name, str(key))).fetchone()
        if row is None:
            raise KeyError(key)
        return json.loads(row[0])

    def items(self):
        for key, raw in self.ledger.db.execute(
                'SELECT key,value FROM records WHERE collection=? ORDER BY rowid', (self.name,)):
            yield key, json.loads(raw)

    def values(self):
        return (record for _, record in self.items())


class AuditLedger(Mapping):
    def __init__(self, path=None, progress=lambda message: None):
        self.temp = tempfile.TemporaryDirectory(prefix='arabsniper-ledger-')
        self.db = sqlite3.connect(str(Path(self.temp.name)/'work.sqlite'))
        self.db.execute('PRAGMA journal_mode=OFF')
        self.db.execute('PRAGMA cache_size=-8192')
        self.db.execute('PRAGMA temp_store=FILE')
        self.db.execute('CREATE TABLE records (collection TEXT, key TEXT, fixture_id TEXT, '
                        'day TEXT, value TEXT, UNIQUE(collection,key))')
        self.db.execute('CREATE INDEX fixture_lookup ON records(fixture_id)')
        self.db.execute('CREATE INDEX date_lookup ON records(day)')
        self.metadata, self.collections = {}, []
        self.progress = progress
        try:
            if path is not None and Path(path).exists():
                self.load(path)
        except BaseException:
            self.close()
            raise

    def close(self):
        self.db.close()
        self.temp.cleanup()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __iter__(self):
        return iter([*self.metadata, *self.collections])

    def __len__(self):
        return len(self.metadata)+len(self.collections)

    def __getitem__(self, name):
        if name in self.collections:
            return Collection(self, name)
        return self.metadata[name]

    def add_collection(self, name):
        if name not in COLLECTIONS:
            raise ValueError('Unknown forecast collection')
        if name not in self.collections:
            self.collections.append(name)

    def put(self, collection, key, record, *, overwrite=True):
        self.add_collection(collection)
        fixture = record.get('fixture') or {}
        try:
            at = datetime.fromisoformat(str(fixture.get('kickoff')).replace('Z', '+00:00'))
            day = at.astimezone(ZoneInfo('Europe/Rome')).date().isoformat() if at.tzinfo else None
        except (ValueError, TypeError):
            day = None
        query = ('INSERT INTO records VALUES(?,?,?,?,?) ON CONFLICT(collection,key) '
                 'DO UPDATE SET fixture_id=excluded.fixture_id,day=excluded.day,value=excluded.value'
                 if overwrite else 'INSERT OR IGNORE INTO records VALUES(?,?,?,?,?)')
        return self.db.execute(query, (collection, str(key), str(fixture.get('fixture_id')),
                                       day, encoded(record))).rowcount

    def load(self, path):
        count = 0
        self.progress(f'indicizzazione su disco: {Path(path).name}')
        for collection, key, record in entries(path):
            if collection is None:
                self.metadata[key] = record
            elif key is None:
                self.add_collection(collection)
            else:
                self.put(collection, key, record)
                count += 1
                if count % 1000 == 0:
                    self.progress(f'indicizzate {count} osservazioni da {Path(path).name}')
        self.db.commit()
        self.progress(f'indicizzazione completata: {count} osservazioni da {Path(path).name}')

    def records(self, day=None, fixture_id=None):
        sql = 'SELECT collection,key,value FROM records'
        parameters = ()
        if day is not None:
            sql += ' WHERE day=?'; parameters = (str(day),)
        elif fixture_id is not None:
            sql += ' WHERE fixture_id=?'; parameters = (str(fixture_id),)
        for collection, key, raw in self.db.execute(sql+' ORDER BY rowid', parameters):
            yield collection, key, json.loads(raw)

    def fixture_store(self, fixture_id):
        store = dict(self.metadata)
        for collection, key, record in self.records(fixture_id=fixture_id):
            store.setdefault(collection, {})[key] = record
        return store

    def update_store(self, store):
        for name, value in store.items():
            if name in COLLECTIONS:
                self.add_collection(name)
                for key, record in value.items():
                    self.put(name, key, record)
            else:
                self.metadata[name] = value

    def write(self, path):
        """Export one record at a time; pointer is replaced only after all parts."""
        self.db.commit()
        with tempfile.TemporaryFile(mode='w+b') as spool:
            spool.write(b'{')
            first = True
            for name, value in self.metadata.items():
                if not first: spool.write(b',')
                first = False
                spool.write((encoded(name)+':'+encoded(value)).encode())
            for collection in self.collections:
                if not first: spool.write(b',')
                first = False
                spool.write((encoded(collection)+':{').encode())
                first_record = True
                for key, raw in self.db.execute('SELECT key,value FROM records WHERE collection=? ORDER BY rowid',
                                                 (collection,)):
                    if not first_record: spool.write(b',')
                    first_record = False
                    spool.write((encoded(key)+':'+raw).encode())
                spool.write(b'}')
            spool.write(b'}')
            publish_spool(path, spool)


def settle_ledger(ledger, output):
    from prematch_audit import settle_predictions
    # Each fixture is handled once, even when the feed contains duplicates.
    matches = {str(m.get('fixture', {}).get('fixture_id')): m
               for day in output.get('days', []) for m in day.get('matches', [])}
    for fid, match in matches.items():
        if not ((match.get('fixture') or {}).get('result') or
                (match.get('prediction_audit') or {}).get('result')):
            continue
        store = ledger.fixture_store(fid)
        settle_predictions(store, {'days': [{'matches': [match]}]})
        ledger.update_store(store)


def capture_ledger(ledger, output, capture, as_of):
    ledger.add_collection('fixtures')
    ledger.metadata['updated_at'] = as_of.isoformat()
    for day in output.get('days', []):
        for match in day.get('matches', []):
            fid = str((match.get('fixture') or {}).get('fixture_id'))
            store = ledger.fixture_store(fid)
            capture({'days': [{'matches': [match]}]}, store=store, persist=False, observed_at=as_of)
            ledger.update_store(store)
