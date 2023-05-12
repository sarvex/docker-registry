# -*- coding: utf-8 -*-
# Copyright (c) 2014 Docker.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
docker_registry.core.boto
~~~~~~~~~~~~~~~~~~~~~~~~~~

Might be useful for
 * Amazon Simple Storage Service (S3)
 * Google Cloud Storage
 * Amazon Glacier
 * Amazon Elastic Block Store (EBS)

"""

import gevent.monkey
gevent.monkey.patch_all()

import logging
import os

from . import driver
from . import lru
from .exceptions import FileNotFoundError

logger = logging.getLogger(__name__)


class Base(driver.Base):

    supports_bytes_range = True

    def __init__(self, path=None, config=None):
        self._config = config
        self._root_path = path or '/test'
        self._boto_conn = self.makeConnection()
        self._boto_bucket = self._boto_conn.get_bucket(
            self._config.boto_bucket)
        logger.info("Boto based storage initialized")

    def _build_connection_params(self):
        kwargs = {'is_secure': (self._config.s3_secure is True)}
        config_args = [
            'host', 'port', 'debug',
            'proxy', 'proxy_port',
            'proxy_user', 'proxy_pass',
            'calling_format'
        ]
        for arg in config_args:
            confkey = f'boto_{arg}'
            if getattr(self._config, confkey, None) is not None:
                kwargs[arg] = getattr(self._config, confkey)
        return kwargs

    def _debug_key(self, key):
        """Used for debugging only."""
        orig_meth = key.bucket.connection.make_request

        def new_meth(*args, **kwargs):
            print('#' * 16)
            print(args)
            print(kwargs)
            print('#' * 16)
            return orig_meth(*args, **kwargs)
        key.bucket.connection.make_request = new_meth

    def _init_path(self, path=None):
        path = os.path.join(self._root_path, path) if path else self._root_path
        return path[1:] if path and path[0] == '/' else path

    def stream_read(self, path, bytes_range=None):
        path = self._init_path(path)
        headers = None
        if bytes_range:
            headers = {'Range': 'bytes={0}-{1}'.format(*bytes_range)}
        key = self._boto_bucket.lookup(path, headers=headers)
        if not key:
            raise FileNotFoundError(f'{path} is not there')
        while True:
            if buf := key.read(self.buffer_size):
                yield buf
            else:
                break

    def list_directory(self, path=None):
        path = self._init_path(path)
        if not path.endswith('/'):
            path += '/'
        ln = len(self._root_path) if self._root_path != '/' else 0
        exists = False
        for key in self._boto_bucket.list(prefix=path, delimiter='/'):
            if f'{key.name}/' == path:
                continue
            exists = True
            name = key.name
            if name.endswith('/'):
                yield name[ln:-1]
            else:
                yield name[ln:]
        if not exists:
            raise FileNotFoundError(f'{path} is not there')

    def get_size(self, path):
        path = self._init_path(path)
        if key := self._boto_bucket.lookup(path):
            return key.size
        else:
            raise FileNotFoundError(f'{path} is not there')

    @lru.get
    def get_content(self, path):
        path = self._init_path(path)
        key = self.makeKey(path)
        if not key.exists():
            raise FileNotFoundError(f'{path} is not there')
        return key.get_contents_as_string()

    def exists(self, path):
        path = self._init_path(path)
        key = self.makeKey(path)
        return key.exists()

    @lru.remove
    def remove(self, path):
        path = self._init_path(path)
        key = self.makeKey(path)
        if key.exists():
            # It's a file
            key.delete()
            return
        # We assume it's a directory
        if not path.endswith('/'):
            path += '/'
        exists = False
        for key in self._boto_bucket.list(prefix=path, delimiter='/'):
            if f'{key.name}/' == path:
                continue
            exists = True
            key.delete()
        if not exists:
            raise FileNotFoundError(f'{path} is not there')
