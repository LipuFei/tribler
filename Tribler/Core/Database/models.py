from axiom import item, attributes


class Peer(item.Item):
    typeName = u'Peer'
    schemaVersion = 1

    peer_mid = attributes.bytes(allowNone=False, indexed=True)


class Torrent(item.Item):
    typeName = u'Torrent'
    schemaVersion = 1

    infohash = attributes.bytes(allowNone=False, indexed=True)
    name = attributes.text(allowNone=True)
    length = attributes.integer(allowNone=True)
    creation_date = attributes.integer(allowNone=True)
    num_files = attributes.integer(allowNone=True)
    insert_time = attributes.integer(allowNone=True)
    secret = attributes.integer(allowNone=True)
    relevance = attributes.integer(allowNone=True)
    num_seeders = attributes.integer(allowNone=True)
    num_leechers = attributes.integer(allowNone=True)
    comment = attributes.text(allowNone=True)
    dispersy_id = attributes.integer(allowNone=True)
    is_collected = attributes.integer(allowNone=False, default=0)
    last_tracker_check = attributes.integer(allowNone=False, default=0)
    tracker_check_retries = attributes.integer(allowNone=False, default=0)
    next_tracker_check = attributes.integer(allowNone=False, default=0)


class MyDownload(item.Item):
    typeName = u'MyDownload'
    schemaVersion = 1

    torrent = attributes.reference(allowNone=False, reftype=Torrent,
                                   whenDeleted=attributes.reference.CASCADE)
    destination_path = attributes.text(allowNone=True)
    creation_time = attributes.integer(allowNone=False)


class Tracker(item.Item):
    typeName = u'Tracker'
    schemaVersion = 1

    tracker_url = attributes.text(allowNone=False, indexed=True)
    last_check = attributes.integer(allowNone=False, default=0)
    failures = attributes.integer(allowNone=False, default=0)
    is_alive = attributes.integer(allowNone=False, default=0)


class TorrentTrackerMap(item.Item):
    typeName = u'TorrentTrackerMap'
    schemaVersion = 1

    torrent = attributes.reference(allowNone=False, reftype=Torrent,
                                   whenDeleted=attributes.reference.CASCADE)
    tracker = attributes.reference(allowNone=False, reftype=Tracker,
                                   whenDeleted=attributes.reference.CASCADE)
