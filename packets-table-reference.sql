create table packets (
    tm timestamp with time zone,
    callsign text,
    symbol text,
    speed_mph numeric,
    bearing numeric,
    altitude numeric,
    comment text,
    location2d geometry(POINT, 4326),
    location3d geometry(POINTZ, 4326),
    raw text,
    ptype text,
    hash text,
    source text,
    channel numeric,
    frequency numeric,
    primary key (tm, source, channel, callsign, hash)
);

create index packets_idx2 on packets (callsign);
create index packets_idx3 on packets (ptype);
create index packets_idx5 on packets (hash);

