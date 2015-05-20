import logging
from abc import ABCMeta
from binascii import hexlify
from time import timezone

from axiom import attributes
from axiom.errors import ItemNotFound

from twisted.python.threadable import isInIOThread

from .models import Peer, Torrent, MyDownload, Tracker, TorrentTrackerMap

from Tribler.Core.simpledefs import INFOHASH_LENGTH
from Tribler.Core.TorrentDef import TorrentDef
from Tribler.Core.CacheDB.SqliteCacheDBHandler import LimitedOrderedDict
from Tribler.Core.Utilities.tracker_utils import get_uniformed_tracker_url

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
        assert isInIOThread()
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
        assert isInIOThread()
        assert isinstance(peer_mid, str), u"peer_mid is not str: %s" % type(peer_mid)

        try:
            peer = self._store.store.findUnique(Peer, Peer.peer_mid == peer_mid)
            self._logger.warn(u"Peer already exists, skip. peer_id = %s", peer.storeID)
        except ItemNotFound:
            peer = Peer(store=self._store.store, peer_mid=peer_mid)
        return peer


class TorrentDatabaseHandler(AbstractDatabaseHandler):
    """
    Mainly handles requests with the Torrent, MyDownload, and Tracker tables.
    """

    def add_download_torrent(self, infohash, destination_dir):
        """
        Adds the given torrent as a download torrent into the MyDownload table.
        :param infohash: The given infohash to be added.
        :param destination_dir: The download directory for the given torrent.
        """
        assert isInIOThread()
        assert isinstance(infohash, str), u"infohash is not str: %s" % type(infohash)
        assert len(infohash) == INFOHASH_LENGTH, u"infohash is not 20-character long: %s" % len(infohash)
        assert isinstance(destination_dir, unicode), u"destination_dir is not unicode: %s" % type(destination_dir)
        assert destination_dir != u'', u"destination_dir is empty"

        # get torrent
        torrent = self.get_torrent(infohash)
        if torrent is None:
            self._logger.error(u"Torrent doesn't exist for MyDownload. infohash = %s", hexlify(infohash))
            return

        # check if we already have the download
        try:
            my_download = self._store.store.findUnique(MyDownload, MyDownload.torrent == Torrent)
            if my_download.destination_dir is not None:
                self._logger.error(u"Duplicate download. infohash: %s, destination_dir: (old) %s, (given) %s",
                                   hexlify(infohash), my_download.destination_dir, destination_dir)
                return

            # the old destination_dir is None, set to the given value
            my_download.destination_dir = destination_dir
        except ItemNotFound:
            # add the new download
            MyDownload(store=self._store.store, torrent=torrent, destination_dir=destination_dir)

        # TODO(lipu): notify that the download has been added

    def remove_download_torrent(self, infohash):
        """
        Removes the given torrent from MyDownload table (by setting destination_dir to None).
        :param infohash: The given infohash.
        """
        assert isInIOThread()
        assert isinstance(infohash, str), u"infohash is not str: %s" % type(infohash)
        assert len(infohash) == INFOHASH_LENGTH, u"infohash is not 20-character long: %s" % len(infohash)

        # get torrent
        torrent = self.get_torrent(infohash)
        if torrent is None:
            self._logger.error(u"Torrent doesn't exist for MyDownload. infohash: %s", hexlify(infohash))
            return

        # check if we already have the download
        try:
            my_download = self._store.store.findUnique(MyDownload, MyDownload.torrent == Torrent)
            my_download.destination_dir = None
        except ItemNotFound:
            self._logger.error(u"Download doesn't exist. infohash: %s", hexlify(infohash))

    def get_torrent(self, infohash):
        """
        Gets a Torrent object with the given infohash.
        :param infohash: The given infohash.
        :return: The Torrent object if exists, otherwise None.
        """
        assert isInIOThread()
        assert isinstance(infohash, str), u"infohash is not str: %s" % type(infohash)
        assert len(infohash) == INFOHASH_LENGTH, u"infohash is not 20-character long: %s" % len(infohash)

        try:
            torrent = self._store.store.findUnique(Torrent, Torrent.infohash == infohash)
        except ItemNotFound:
            torrent = None
            self._logger.warn(u"Torrent not found with infohash: %s", hexlify(infohash))
        return torrent

    def add_uncollected_torrent(self, torrent_dict):
        pass
        # TODO

    def add_collected_torrent(self, tdef):
        """
        Adds a collected torrent into the Torrent table.
        :param tdef: The collected TorrentDef object.
        """
        assert isInIOThread()
        assert isinstance(tdef, TorrentDef), u"tdef is not TorrentDef: %s" % type(tdef)

        # check if torrent exists
        torrent = self.get_torrent(tdef.infohash)
        if torrent is not None:
            if torrent.is_collected:
                self._logger.error(u"Duplicate collected torrent. infohash: %s", hexlify(tdef.infohash))
                return

            # update the current one
            torrent.infohash = tdef.infohash
            torrent.name = tdef.get_name_as_unicode()
            torrent.length = tdef.get_length()
            torrent.creation_date = tdef.get_creation_date()
            torrent.num_files = len(tdef.get_files())
            torrent.secret = tdef.is_private()
            torrent.comment = tdef.get_comment_as_unicode()
            torrent.is_collected = 1

        else:
            # insert a new torrent
            data_dict = {u'infohash': tdef.infohash,
                         u'name': tdef.get_name_as_unicode(),
                         u'length': tdef.get_length(),
                         u'creation_date': tdef.get_creation_date(),
                         u'num_files': len(tdef.get_files()),
                         u'insert_time': timezone.now(),
                         u'secret': tdef.is_private(),
                         u'relevance': 0,
                         u'category': u'unknown',
                         u'status': u'unknown',
                         u'num_seeders': 0,
                         u'num_leechers': 0,
                         u'comment': tdef.get_comment_as_unicode(),
                         u'dispersy_id': None,
                         u'is_collected': 1,
                         u'last_tracker_check': 0,
                         u'tracker_check_retries': 0,
                         u'next_tracker_check': 0}
            torrent = Torrent(store=self._store.store, **data_dict)

        # add trackers
        tracker_list = list(tdef.get_trackers_as_single_tuple())
        self.add_trackers(tdef.infohash, tracker_list, torrent=torrent)

        # TODO: notify signal

    def get_collected_torrents_count(self):
        """
        Gets the total number of collected torrents.
        :return: The total number of collected torrents.
        """
        assert isInIOThread()

        collected_torrent_result = self._store.store.query(Torrent, Torrent.is_collected == 1)
        collected_torrents_count = sum(1 for _ in collected_torrent_result)
        return collected_torrents_count

    def get_recently_collected_torrents(self, limit):
        results = self._store.store.query(Torrent, attributes.AND(Torrent.is_collected == 1, Torrent.secret == 0),
                                          sort=Torrent.insert_time.descending, limit=limit)
        recently_collected_torrent_list = []
        for torrent in results:
            torrent_data = {u'infohash': torrent.infohash,
                            u'num_seeders': torrent.num_seeders,
                            u'num_leechers': torrent.num_leechers,
                            u'last_tracker_check': torrent.last_tracker_check,
                            u'insert_time': torrent.insert_time}
            recently_collected_torrent_list.append(torrent_data)
        return results

    # ----- ----- ----- ----- ----- -----
    # Tracker-related code
    # ----- ----- ----- ----- ----- -----

    def get_trackers_for_torrent(self, infohash):
        """
        Gets the trackers of a given torrent.
        :param infohash: The given torrent infohash.
        :return: A list of trackers if exists, None otherwise.
        """
        assert isInIOThread()
        assert isinstance(infohash, str), u"infohash is not str: %s" % type(infohash)
        assert len(infohash) == INFOHASH_LENGTH, u"infohash is not 20-character long: %s" % len(infohash)

        torrent = self.get_torrent(infohash)
        if torrent is None:
            return

        tracker_list = []
        torrent_tracker_map_result = self._store.store.query(TorrentTrackerMap, TorrentTrackerMap.torrent == torrent)
        for torrent_tracker_map in torrent_tracker_map_result:
            tracker_data = {u'tracker_url': torrent_tracker_map.tracker.tracker_url,
                            u'last_check': torrent_tracker_map.tracker.last_check,
                            u'failures': torrent_tracker_map.tracker.failures,
                            u'is_alive': torrent_tracker_map.tracker.is_alive}
            tracker_list.append(tracker_data)

        return tracker_list

    def add_trackers(self, infohash, tracker_list, torrent=None):
        """
        Adds trackers for a given torrent into Tracker and TorrentTrackerMap tables.
        :param infohash: The given infohash.
        :param tracker_list: The list of trackers to add.
        :param torrent: The Torrent axiom object (used by calls within the database, for convenience).
        """
        assert isInIOThread()
        assert isinstance(tracker_list, list), u"tracker_list is not a list: %s" % type(tracker_list)

        # get the torrent. a database function can give the Torrent object directly so we don't need to get it again
        if torrent is None:
            torrent = self.get_torrent(infohash)
            if torrent is None:
                self._logger.error(u"Torrent doesn't exist. infohash: %s", hexlify(infohash))
                return

        for raw_tracker_url in tracker_list:
            tracker_url = get_uniformed_tracker_url(raw_tracker_url)
            if tracker_url is None:
                self._logger.warn(u"Skipping invalid tracker: %s", repr(raw_tracker_url))
                continue

            # add the tracker if it's not in the Tracker table.
            try:
                tracker = self._store.store.findUnique(Tracker, Tracker.tracker_url == tracker_url)
            except ItemNotFound:
                tracker = Tracker(self._store.store, tracker_url=tracker_url)

            # add into map if it doesn't exist
            try:
                self._store.store.findUnique(TorrentTrackerMap,
                                             attributes.AND(TorrentTrackerMap.torrent == torrent,
                                                            TorrentTrackerMap.tracker == tracker))
            except ItemNotFound:
                TorrentTrackerMap(self._store.store, torrent=torrent, tracker=tracker)

    def update_tracker(self, tracker_data):
        """
        Updates a tracker data.
        :param tracker_data: A dictionary of tracker data.
        """
        assert isInIOThread()
        assert isinstance(tracker_data, dict), u"tracker_data is not a dict: %s" % type(tracker_data)

        try:
            tracker = self._store.store.findUnique(Tracker, Tracker.tracker_url == tracker_data[u'tracker_url'])
            # update
            tracker.last_check = tracker_data[u'last_check']
            tracker.failures = tracker_data[u'failures']
            tracker.is_alive = tracker_data[u'is_alive']
        except ItemNotFound:
            self._logger.error(u"Tracker not found. url: %s", tracker_data[u'tracker_url'])

    def get_all_trackers(self):
        """
        Gets all tracker data from the Tracker table.
        :return: A list of all tracker data (no-DHT and DHT will be included).
        """
        assert isInIOThread()

        tracker_data_list = []
        tracker_result = self._store.store.query(Tracker)
        for tracker in tracker_result:
            tracker_data = {u'tracker_url': tracker.tracker_url,
                            u'last_check': tracker.last_check,
                            u'failures': tracker.failures,
                            u'is_alive': tracker.is_alive}
            tracker_data_list.append(tracker_data)
        return tracker_data_list
