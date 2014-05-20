# -*- coding: utf-8 -*-
"""
docker_registry.drivers.s3
~~~~~~~~~~~~~~~~~~~~~~~~~~

This is a s3 based driver.

"""

import gevent.monkey
gevent.monkey.patch_all()

import docker_registry.core.boto as coreboto
# from docker_registry.core import exceptions
from docker_registry.core import compat
from docker_registry.core import lru

# import gevent.monkey
# gevent.monkey.patch_all()

import logging

import boto.s3
import boto.s3.connection
import boto.s3.key

logger = logging.getLogger(__name__)


class Storage(coreboto.Base):

    def __init__(self, path, config):
        super(Storage, self).__init__(path, config)

    def _build_connection_params(self):
        kwargs = super(Storage, self)._build_connection_params()
        if self._config.s3_secure is not None:
            kwargs['is_secure'] = (self._config.s3_secure is True)
        return kwargs

    def makeConnection(self):
        kwargs = self._build_connection_params()
        if self._config.s3_region is not None:
            return boto.s3.connect_to_region(
                region_name=self._config.s3_region,
                aws_access_key_id=self._config.s3_access_key,
                aws_secret_access_key=self._config.s3_secret_key,
                **kwargs)
        logger.warn("No S3 region specified, using boto default region, " +
                    "this may affect performance and stability.")
        return boto.s3.connection.S3Connection(
            self._config.s3_access_key,
            self._config.s3_secret_key,
            **kwargs)

    def makeKey(self, path):
        return boto.s3.key.Key(self._boto_bucket, path)

    @lru.set
    def put_content(self, path, content):
        path = self._init_path(path)
        key = self.makeKey(path)
        key.set_contents_from_string(
            content, encrypt_key=(self._config.s3_encrypt is True))
        return path

    def stream_write(self, path, fp):
        # Minimum size of upload part size on S3 is 5MB
        buffer_size = 5 * 1024 * 1024
        if self.buffer_size > buffer_size:
            buffer_size = self.buffer_size
        path = self._init_path(path)
        mp = self._boto_bucket.initiate_multipart_upload(
            path, encrypt_key=(self._config.s3_encrypt is True))
        num_part = 1
        try:
            while True:
                buf = fp.read(buffer_size)
                if not buf:
                    break
                io = compat.StringIO(buf)
                mp.upload_part_from_file(io, num_part)
                num_part += 1
                io.close()
        except IOError:
            pass
        mp.complete_upload()

    def content_redirect_url(self, path):
        path = self._init_path(path)
        key = self.makeKey(path)
        if not key.exists():
            raise IOError('No such key: \'{0}\''.format(path))
        return key.generate_url(
            expires_in=1200,
            method='GET',
            query_auth=True)