#!/usr/bin/env python
# Copyright 2010 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Extra output writers for MapReduce."""


__all__ = [
    "PostgresOutputWriter"
    ]

import pg8000

from mapreduce.output_writers import OutputWriter, _get_params
from mapreduce import context
from mapreduce import errors


class PostgresOutputWriter(OutputWriter):
  """A customized output writer for Postgres database.

  To use this output writer, the output of your mapper should be well-formatted SQL queries, which
  will be executed in batch against the Postgres server that specified by your output_writer parameters.

  An example of the parameters below:
      ...
      output_writer_spec="mapreduce.third_party.custom_output_writers.PostgresOutputWriter",
      params = {
        "input_reader": {
          ...
        },
        "output_writer": {
          "host": "127.0.0.1",
          "port": 5432,
          "database": "example_db",
          "user": "postgres_user1",
          "password": "kjkjegkajgklejkjak"
        }
      },

  """

  def __init__(self, host=None, port=None, database=None, user=None, password=None): # pylint: disable=W0231
    self.host = host
    self.port = port
    self.database = database
    self.user = user
    self.password = password

  @classmethod
  def create(cls, mr_spec, shard_number, shard_attempt, _writer_state=None):
    mapper_spec = mr_spec.mapper
    params = _get_params(mapper_spec)
    return cls(host=params.get('host'),
               port=params.get('port'),
               database=params.get('database'),
               user=params.get('user'),
               password=params.get('password'))

  def write(self, data):
    ctx = context.get()
    pg_pool = ctx.get_pool('postgres_pool')
    if not pg_pool:
      pg_pool = _PostgresPool(ctx=ctx,
                              host=self.host,
                              port=self.port,
                              database=self.database,
                              user=self.user,
                              password=self.password)
      ctx.register_pool('postgres_pool', pg_pool)
    pg_pool.append(data)

  def to_json(self):
    return {
      "host": self.host,
      "port": self.port,
      "database": self.database,
      "user": self.user,
      "password": self.password
    }

  @classmethod
  def from_json(cls, state):
    return cls(host=state.get('host'),
               port=state.get('port'),
               database=state.get('database'),
               user=state.get('user'),
               password=state.get('password'))

  @classmethod
  def validate(cls, mapper_spec):
    required_params = ["host", "port", "database", "user", "password"]
    if mapper_spec.output_writer_class() != cls:
      raise errors.BadWriterParamsError("Output writer class mismatch")

    params = _get_params(mapper_spec)
    if not all([arg in params for arg in required_params]):
      raise errors.BadWriterParamsError("Output writer requires parameters [{}]".format(', '.join(required_params)))

    if not isinstance(params.get("port"), int):
      raise errors.BadWriterParamsError("Parameter 'port' must be integer.")

  @classmethod
  def init_job(cls, mapreduce_state):
    pass

  def finalize(self, ctx, shard_state):
    pass

  @classmethod
  def finalize_job(cls, mapreduce_state):
    pass

  @classmethod
  def get_filenames(cls, mapreduce_state):
    return []


class _PostgresPool(context.Pool):
  """A mutation pool that accumulate writes of PostgresOutputWriter."""

  PG_POOL_SIZE = 200

  def __init__(self, ctx=None, host=None, port=None, database=None, user=None, password=None):
    self._queries = []
    self._size = 0
    self._ctx = ctx
    self._conn = pg8000.connect(host=host, port=port, database=database,
                                user=user, password=password, ssl=True)

  def append(self, query):
    self._queries.append(query)
    self._size += 1
    if self._size > self.PG_POOL_SIZE:
      self.flush()

  def flush(self):
    if self._queries:
      cur = self._conn.cursor()
      for query in self._queries:
        cur.execute(query)
      cur.close()
      self._conn.commit()
    self._queries = []
    self._size = 0

  def __enter__(self):
    return self

  def __exit__(self, atype, value, traceback):
    self.flush()
    self._conn.close()
