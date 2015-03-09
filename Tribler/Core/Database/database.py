import os
import logging

from axiom.store import Store

from Tribler.dispersy.util import blocking_call_on_reactor_thread

from .models import Peer

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
        self._store.getTableName(Peer)

    @blocking_call_on_reactor_thread
    def shutdown(self):
        self._store.close()
        self._store = None
        self._session = None
