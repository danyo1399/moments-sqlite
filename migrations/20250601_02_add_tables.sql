create table streams(
    stream_id text primary key,
    version bigint not null,
    stream_type text not null,
    created_at timestamp not null default current_timestamp,
    timestamp timestamp not null default current_timestamp

);
create unique index idx_streams on streams(stream_id, version);
create table events(
   sequence integer primary key autoincrement,
    stream_id text not null,
    event_id text not null,
    event_type text not null,
    version bigint not null,
    data text not null,
    timestamp timestamp not null default current_timestamp,
    correlation_id text,
    causation_id text,
    metadata text 
);
create unique index idx_events_id on events(event_id);
create unique index idx_events_version on events(stream_id, version);

create index idx_correlation on events(correlation_id);
create table snapshots(
    stream_id text  not null,
    type text not null,
    schema_version integer not null,
    version bigint not null,
    data text not null,
    timestamp timestamp not null default current_timestamp,
    primary key(stream_id, schema_version)

)

