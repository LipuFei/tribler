import os
import logging

from axiom.store import Store

from Tribler.dispersy.util import blocking_call_on_reactor_thread

from .models import Torrent, MyDownload, Tracker, TorrentTrackerMap
from .channel_models import Peer, Channel

AXIOM_DIR = u"axiom_store"


class AxiomStore(object):
    """ A simple wrapper for axiom Store.
    """

    def __init__(self, session):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._session = session
        self._store = None

    @property
    def store(self):
        return self._store

    @blocking_call_on_reactor_thread
    def initialize(self):
        # initialize Store
        work_dir = os.path.join(self._session.get_state_dir(), AXIOM_DIR)
        self._store = Store(work_dir)

        # create tables if they do not exist
        self._store.getTableName(Torrent)
        self._store.getTableName(MyDownload)
        self._store.getTableName(Tracker)
        self._store.getTableName(TorrentTrackerMap)

        # channel-related tables
        self._store.getTableName(Peer)
        self._store.getTableName(Channel)

    @blocking_call_on_reactor_thread
    def shutdown(self):
        self._store.close()
        self._store = None
        self._session = None
