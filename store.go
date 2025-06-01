package momentssqlite

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/danyo1399/moments"
	_ "github.com/mattn/go-sqlite3"
)

type Store struct {
	db     *sql.DB
	config Config
}

func NewStore(db *sql.DB, config Config) (*Store, error) {
	if db == nil {
		return nil, fmt.Errorf("db is nil")
	}
	return &Store{db, config}, nil
}

func (s *Store) Close() {
}

func (s *Store) SaveEvents(args moments.SaveEventArgs) error {
	isNew := len(args.Events) == int(args.ExpectedVersion)
	startVersion := len(args.Events) - int(args.ExpectedVersion)
	tran, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tran.Rollback()

	if isNew {
		_, err = tran.Exec(`insert into streams(
			stream_id, version, stream_type)
			values(?, ?, ?)`, args.StreamId.String(), args.ExpectedVersion, args.StreamId.StreamType)
		if err != nil {
			return err
		}
	} else {
		res, err := tran.Exec(`update streams set timestamp = current_timestamp, version = ? 
			where stream_id = ? and version = ?
			`, args.ExpectedVersion, args.StreamId.String(), startVersion)
		if err != nil {
			return err
		}
		rowsAffected, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if rowsAffected == 0 {
			return fmt.Errorf("expected version mismatch")
		}

	}
	for idx, evt := range args.Events {
		data, err := json.Marshal(evt.Data)
		if err != nil {
			return err
		}
		metadata, err := json.Marshal(args.Metadata)
		if err != nil {
			return err
		}
		_, err = tran.Exec(`insert into events(
		stream_id, event_id, event_type, version, data , correlation_id, causation_id, 
		metadata) values(?, ?, ?, ?, ?, ?, ?, ?)
		`,
			args.StreamId.String(),
			evt.EventId,
			moments.GetEventType(evt.Data),
			startVersion+idx+1,
			string(data),
			args.CorrelationId,
			args.CausationId,
			string(metadata),
		)
		if err != nil {
			return err
		}
	}
	err = tran.Commit()
	if err != nil {
		return err
	}
	return nil
}

func (s *Store) LoadEvents(args moments.LoadEventArgs) ([]moments.PersistedEvent, error) {
	var b strings.Builder
	params := make([]any, 0)
	b.WriteString(`
		select 
		stream_id,
		event_id,
		event_type,
		version,
		data,
		sequence,
		correlation_id,
		causation_id,
		metadata,
		timestamp
		from events
		where 1 = 1 
		`)

	if args.StreamId != (moments.StreamId{}) {
		b.WriteString(` and stream_id = ?`)
		params = append(params, args.StreamId.String())
	}
	if args.FromSequence != 0 {

		b.WriteString(` and sequence >= ?`)
		params = append(params, args.FromSequence)
	}
	if args.ToSequence != 0 {
		b.WriteString(` and sequence <= ?`)
		params = append(params, args.ToSequence)
	}
	if args.FromVersion != 0 {
		b.WriteString(` and version >= ?`)
		params = append(params, args.FromVersion)
	}
	if args.ToVersion != 0 {
		b.WriteString(` and version <= ?`)
		params = append(params, args.ToVersion)
	}
	b.WriteString(" order by sequence")
	if args.Descending {
		b.WriteString(" desc")
	}
	if args.Count != 0 {
		b.WriteString(` limit ?`)
		params = append(params, args.Count)
	}
	rows, err := s.db.Query(b.String(), params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	events := make([]moments.PersistedEvent, 0)
	for rows.Next() {
		var streamId string
		var eventId string
		var eventType string
		var version uint64
		var data string
		var sequence uint64
		var correlationId string
		var causationId string
		var metadata string
		var timestamp time.Time
		err := rows.Scan(
			&streamId, &eventId, &eventType, &version,
			&data, &sequence, &correlationId, &causationId, &metadata, &timestamp)
		if err != nil {
			return nil, err
		}
		event, err := s.config.EventDeserialiser.Deserialise(moments.EventType(eventType), []byte(data))
		if err != nil {
			return nil, err
		}
		evt := moments.PersistedEvent{
			StreamId: moments.StreamId{
				Id:         streamId,
				StreamType: moments.AggregateType(streamId),
			},
			Event: moments.Event{
				Data:    event,
				EventId: moments.EventId(eventId),
			},
			EventType:      moments.EventType(eventType),
			Version:        moments.Version(version),
			Sequence:       moments.Sequence(sequence),
			CorrelationId:  moments.CorrelationId(correlationId),
			CausationId:    moments.CausationId(causationId),
			GlobalSequence: moments.Sequence(sequence),
			Timestamp:      timestamp,
		}
		events = append(events, evt)
	}
	return events, nil
}

func (s *Store) SaveSnapshot(snapshot *moments.Snapshot) error {
	tran, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tran.Rollback()
	tran.Exec(`
insert into snapshots(stream_id, type, schema_version, version, data, timestamp) values(?, ?, ?, ?, ?, current_timestamp)
		on conflict(stream_id, schema_version) do update 
		set type = excluded.type
		, schema_version = excluded.schema_version
		, version = excluded.version
		, data = excluded.data
		, timestamp = excluded.timestamp
		where excluded.version >= snapshots.version
		`,
		snapshot.Id.StreamId.String(),
		snapshot.Id.StreamId.StreamType,
		snapshot.Id.SchemaVersion,
		snapshot.Version,
		snapshot.State)
	err = tran.Commit()
	if err != nil {
		return err
	}
	return nil
}

func (s *Store) LoadSnapshot(id moments.SnapshotId) (*moments.Snapshot, error) {
	row := s.db.QueryRow(`
select schema_version, version, data from snapshots where stream_id = ? and schema_version = ?`,
		id.StreamId.String(), id.SchemaVersion)
	var schemaVersion moments.SchemaVersion
	var version moments.Version
	var data []byte
	err := row.Scan(&schemaVersion, &version, &data)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	snapshot := moments.Snapshot{
		Id:      moments.NewSnapshotId(id.StreamId, schemaVersion),
		Version: version,
		State:   data,
	}
	return &snapshot, nil
}

// DeleteSnapshot(snapshot *moments.Snapshot) error
func (s *Store) DeleteSnapshot(id moments.SnapshotId) error {
	_, err := s.db.Exec(`
delete from snapshots where stream_id = ? and schema_version = ?
		`, id.StreamId.String(), id.SchemaVersion)
	if err != nil {
		return err
	}
	return nil
}
