import logging
from abc import ABCMeta
from binascii import hexlify
from axiom.errors import ItemNotFound

from .models import Peer, Torrent

from Tribler.Core.simpledefs import INFOHASH_LENGTH
from Tribler.Core.CacheDB.SqliteCacheDBHandler import LimitedOrderedDict
from Tribler.dispersy.util import blocking_call_on_reactor_thread


DEFAULT_ID_CACHE_SIZE = 1024 * 5


class AbstractDatabaseHandler(object):
    """
    This abstract class for DatabaseHandlers.
    """
    __meta__ = ABCMeta

    def __init__(self, session):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._session = session
        self._store = session.lm.axiom_store


class PeerDatabaseHandler(AbstractDatabaseHandler):
    """
    Mainly handles requests with the Peer table.
    """

    def __init__(self, session):
        super(PeerDatabaseHandler, self).__init__(session)

        self._peer_id_mid_cache = LimitedOrderedDict(DEFAULT_ID_CACHE_SIZE)

    @blocking_call_on_reactor_thread
    def get_peer(self, peer_mid):
        """
        Gets a Peer object with the given peer member ID.
        :param peer_mid: The given peer member ID.
        :return: The Peer object if exists, otherwise None.
        """
        assert isinstance(peer_mid, str), u"peer_mid is not str: %s" % type(peer_mid)

        try:
            peer = self._store.store.findUnique(Peer, Peer.peer_mid == peer_mid)
        except ItemNotFound:
            peer = None
            self._logger.warn(u"Peer not found with peer_mid: %s", hexlify(peer_mid))
        return peer

    @blocking_call_on_reactor_thread
    def add_peer(self, peer_mid):
        """
        Adds a new peer with the given peer member ID into the database.
        :param peer_mid: The given peer member ID.
        :return: The newly added peer object if successful, otherwise None.
        """
        assert isinstance(peer_mid, str), u"peer_mid is not str: %s" % type(peer_mid)

        try:
            peer = self._store.store.findUnique(Peer, Peer.peer_mid == peer_mid)
            self._logger.warn(u"Peer already exists, skip. peer_id = %s", peer.storeID)
        except ItemNotFound:
            peer = Peer(store=self._store.store, peer_mid=peer_mid)
        return peer


class TorrentDatabaseHandler(AbstractDatabaseHandler):
    """
    Mainly handles requests with the Torrent and Tracker tables.
    """

    @blocking_call_on_reactor_thread
    def get_torrent(self, infohash):
        """
        Gets a Torrent object with the given infohash.
        :param infohash: The given infohash.
        :return: The Torrent object if exists, otherwise None.
        """
        assert isinstance(infohash, str), u"infohash is not str: %s" % type(infohash)
        assert len(infohash) == INFOHASH_LENGTH, u"infohash is not 20-character long: %s" % len(infohash)

        try:
            torrent = self._store.store.findUnique(Peer, Torrent.infohash == infohash)
        except ItemNotFound:
            torrent = None
            self._logger.warn(u"Torrent not found with infohash: %s", hexlify(infohash))
        return torrent

    def add_uncollected_torrent(self, torrent_dict):
        pass

    def add_collected_torrent(self, tdef):
        """

        :param tdef:
        :return:
        """
        pass

    def update_torrent(self, infohash):
        pass

    def get_collected_torrent(self, infohash):
        pass
