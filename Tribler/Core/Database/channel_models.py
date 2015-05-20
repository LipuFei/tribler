from axiom import item, attributes

from .models import Torrent


class Peer(item.Item):
    typeName = u'Peer'
    schemaVersion = 1

    peer_mid = attributes.bytes(allowNone=False, indexed=True)


class Channel(item.Item):
    typeName = u'Channel'
    schemaVersion = 1

    dispersy_cid = attributes.bytes(allowNone=True)
    peer = attributes.reference(allowNone=True, reftype=Peer,
                                whenDeleted=attributes.reference.NULLIFY)
    name = attributes.text(allowNone=False)
    description = attributes.text(allowNone=True)
    time_modified = attributes.timestamp()
    time_inserted = attributes.timestamp()
    deleted_at = attributes.timestamp(indexed=True)
    nr_torrents = attributes.integer(default=0)
    nr_spam = attributes.integer(default=0)
    nr_favorite = attributes.integer(default=0)
