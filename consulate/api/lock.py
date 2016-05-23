"""
Lock Object for easy locking

"""
import contextlib
import logging
import uuid
import time

from consulate.api import base
from consulate import utils, exceptions
from threading import Thread, local

LOGGER = logging.getLogger(__name__)


class Lock(base.Endpoint):
    """Wrapper for easy :class:`~consulate.api.kv.KV` locks. Keys are
    automatically prefixed with ``consulate/locks/``. To change the prefix or
    remove it invoke the :meth:~consulate.api.lock.Lock.prefix` method.

    Example:

    .. code:: python

        import consulate

        consul = consulate.Consul()
        with consul.lock.acquire('my-key'):
            print('Locked: {}'.format(consul.lock.key))
            # Do stuff

    :raises: :exc:`~consulate.exception.LockError`

    """
    DEFAULT_PREFIX = 'consulate/locks'

    def __init__(self, uri, adapter, session, datacenter=None, token=None):
        """Create a new instance of the Lock

        :param str uri: Base URI
        :param consul.adapters.Request adapter: Request adapter
        :param consul.api.session.Session session: Session endpoint instance
        :param str datacenter: datacenter
        :param str token: Access Token

        """
        super(Lock, self).__init__(uri, adapter, datacenter, token)
        self._base_uri = '{0}/kv'.format(uri)
        self._session = session
        self._session_id = None
        self._ttl_timer = None
        self._item = str(uuid.uuid4())
        self._prefix = self.DEFAULT_PREFIX

    @contextlib.contextmanager
    def acquire(self, key=None, value=None, behavior=None, ttl=None, renew_before_ttl=5):
        """A context manager that allows you to acquire the lock, optionally
        passing in a key and/or value.

        :param str key: The key to lock
        :param str value: The value to set in the lock
        :raises: :exc:`~consulate.exception.LockError`

        """
        self._acquire(key, value, behavior, ttl, renew_before_ttl)
        yield
        self._release()

    @property
    def key(self):
        """Return the lock key

        :rtype: str

        """
        return self._item

    def prefix(self, value):
        """Override the path prefix for the lock key

        :param str value: The value to set the path prefix to

        """
        self._prefix = value or ''

    def _acquire(self, key=None, value=None, behavior='release', ttl=None, renew_before_ttl=5):
        if ttl is None:
            self._session_id = self._session.create(behavior=behavior)
        else:
            if ttl < 10:
                raise ValueError("ttl is less than the minimum (10s)")
                return False
            self._session_id = self._session.create(behavior=behavior, ttl="{}s".format(str(ttl)))

        self._item = '/'.join([self._prefix, (key or str(uuid.uuid4()))])
        LOGGER.debug('Acquiring a lock of %s for session %s',
                     self._item, self._session_id)
        response = self._put_response_body([self._item],
                                           {'acquire': self._session_id},
                                           value)
        if not response:
            self._session.destroy(self._session_id)
            raise exceptions.LockFailure()
            return False

        if ttl is not None:
            #handle renewal
            timer_ttl = ttl - renew_before_ttl

            if timer_ttl > 0:
                def renew():
                    while(self._ttl_timer.active):
                        LOGGER.debug("renewing lock on %s for %s with ttl %s" % (self._item, self._session_id, timer_ttl))
                        if self._session.renew(self._session_id):
                            time.sleep(timer_ttl)
                    LOGGER.debug("ceasing renewal of %s for %s" % (self._item, self._session_id))
                self._ttl_timer = StoppableThread(target=renew)
                self._ttl_timer.daemon = True
                self._ttl_timer.start()

    def _release(self):
        """Release the lock"""
        self._ttl_timer.active = False
        self._put_response_body([self._item], {'release': self._session_id})
        self._adapter.delete(self._build_uri([self._item]))
        self._session.destroy(self._session_id)
        self._item, self._session_id = None, None

class StoppableThread(Thread):
    """ Simple Thread which can be set inactive. """
    def __init__(self, target=None):
        Thread.__init__(self, target=target)
        self.active = local()
        self.active = True

