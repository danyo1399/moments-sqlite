package momentssqlite

import (
	"testing"

	"github.com/danyo1399/gotils"
	"github.com/danyo1399/moments"
	"github.com/danyo1399/moments/test"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
)

type dispose func()

func createProvider(t *testing.T) (*StoreProvider, moments.Store, dispose) {
	eventDeserialiser := moments.NewEventDeserialiser()
	moments.AddJsonEventDeserialiser[test.Added](eventDeserialiser)
	moments.AddJsonEventDeserialiser[test.Subtracted](eventDeserialiser)
	moments.AddJsonEventDeserialiser[test.Updated](eventDeserialiser)

	config := Config{
		DataPath:          "./data",
		EventDeserialiser: eventDeserialiser,
	}

	tenantName := gotils.NewTimeOrderedId()

	provider := NewStoreProvider(config)
	err := provider.NewTenant(tenantName)
	if err != nil {
		panic(err)
	}

	t.Log("Created tenant")
	store, err := provider.GetStore(tenantName)
	if err != nil {
		panic(err)
	}

	dispose := func() {
		err := provider.DeleteTenant(tenantName)
		if err != nil {
			panic(err)
		}
		provider.Close()
	}
	return provider, store, dispose
}

func Test_GivenNoEventsForStream_WhenLoadEvents_ThenReturnsEmptySlice(t *testing.T) {
	_, store, dispose := createProvider(t)
	defer dispose()
	assert.NotNil(t, store)
	assert.NotNil(t, dispose)
	// defer dispose()
	evts, err := store.LoadEvents(
		moments.LoadEventArgs{StreamId: moments.StreamId{Id: "invalid", StreamType: "invalid"}})
	assert.NoError(t, err)
	assert.Empty(t, evts)
}

func createSnapshot() moments.Snapshot {
	snapshot := moments.Snapshot{
		Id: moments.SnapshotId{
			StreamId: moments.StreamId{
				Id:         "id",
				StreamType: "streamType",
			},
			SchemaVersion: moments.SchemaVersion(1),
		},
		Version: 1,
		State:   []byte("test"),
	}
	return snapshot
}

func Test_GivenSavedMultipeSnapshotSchemaVersions_WhenLoadSnapshots_ThenCanLoadMultipleSavedSchemaVersions(t *testing.T) {
	_, store, dispose := createProvider(t)
	defer dispose()
	id1 := moments.SnapshotId{
		StreamId: moments.StreamId{
			Id:         "id",
			StreamType: "streamType",
		},
		SchemaVersion: moments.SchemaVersion(1),
	}
	id2 := moments.SnapshotId{
		StreamId: moments.StreamId{
			Id:         "id",
			StreamType: "streamType",
		},
		SchemaVersion: moments.SchemaVersion(2),
	}
	snapshot := createSnapshot()
	snapshot.Id = id1

	err := store.SaveSnapshot(&snapshot)
	assert.NoError(t, err)

	snapshot.Id = id2
	err = store.SaveSnapshot(&snapshot)
	assert.NoError(t, err)

	loadedSnapshot1, err := store.LoadSnapshot(id1)
	assert.NoError(t, err)

	loadedSnapshot2, err := store.LoadSnapshot(id2)
	assert.NoError(t, err)
	assert.NotNil(t, loadedSnapshot1)
	assert.Equal(t, loadedSnapshot1.Id.SchemaVersion, moments.SchemaVersion(1))
	assert.NotNil(t, loadedSnapshot2)
	assert.Equal(t, loadedSnapshot2.Id.SchemaVersion, moments.SchemaVersion(2))
}

func Test_GivenSavedEvent_WhenLoadEvents_ReturnsSavedEvent(t *testing.T) {
	_, store, dispose := createProvider(t)
	assert.NotNil(t, store)
	defer dispose()
	streamId := moments.StreamId{Id: "123", StreamType: "Type"}
	err := store.SaveEvents(moments.SaveEventArgs{
		StreamId:        streamId,
		CorrelationId:   "correlation",
		CausationId:     "causation",
		Metadata:        map[string]any{},
		ExpectedVersion: moments.Version(1),
		Events: []moments.Event{
			{
				EventId: "evtid",
				Data:    test.Added{Value: 5},
			},
		},
	},
	)
	assert.NoError(t, err)
	evts, err := store.LoadEvents(
		moments.LoadEventArgs{StreamId: streamId})
	assert.NoError(t, err)
	assert.Len(t, evts, 1)
	evt := evts[0]
	assert.Equal(t, evt.CausationId, moments.CausationId("causation"))
	assert.Equal(t, evt.CorrelationId, moments.CorrelationId("correlation"))
	assert.Equal(t, evt.GlobalSequence, moments.Sequence(1))
	assert.Equal(t, evt.Version, moments.Version(1))
	assert.Equal(t, evt.EventId, moments.EventId("evtid"))
	assert.Equal(t, evt.EventType, moments.EventType("test.Added"))
	assert.Equal(t, test.Added{Value: 5}, evt.Data)
}

func Test_GivenNoSnapshots_WhenCreateLoadDeleteShapshot_ThenSnapshotCreatedLoadedDeleted(t *testing.T) {
	_, store, dispose := createProvider(t)
	assert.NotNil(t, store)
	defer dispose()

	snapshot := moments.Snapshot{
		Id: moments.NewSnapshotId(
			moments.StreamId{"id", "streamType"}, moments.SchemaVersion(2)),
		Version: 1,
		State:   []byte("test"),
	}
	store.SaveSnapshot(&snapshot)
	loadedSnapshot, err := store.LoadSnapshot(snapshot.Id)
	assert.NoError(t, err)
	err = store.DeleteSnapshot(snapshot.Id)
	assert.NoError(t, err)
	deletedSnapshot, err := store.LoadSnapshot(snapshot.Id)
	assert.NoError(t, err)

	assert.Nil(t, deletedSnapshot)
	assert.Equal(t, snapshot, *loadedSnapshot)
}
