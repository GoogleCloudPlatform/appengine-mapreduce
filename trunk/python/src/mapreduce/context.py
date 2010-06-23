#!/usr/bin/env python
#
# Copyright 2007 Google Inc.
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
#

"""Mapreduce execution context.

Mapreduce context provides handler code with information about
current mapreduce execution and organizes utility data flow
from handlers such as counters, log messages, mutation pools.
"""


__all__ = ["MAX_ENTITY_COUNT", "MAX_POOL_SIZE", "Context", "MutationPool",
           "Counters", "EntityList", "get", "COUNTER_MAPPER_CALLS"]




from google.appengine.ext import db

MAX_POOL_SIZE = 900 * 1000

MAX_ENTITY_COUNT = 500

COUNTER_MAPPER_CALLS = "mapper_calls"


def _get_entity_size(entity):
  """Get entity size in bytes.

  Should computes size of entity representation in wire format when sent
  to datastore.

  Args:
    entity: an entity as db.Model subclass.

  Returns:
    the size of entity protobuf representation in bytes as int.
  """
  return len(db.model_to_protobuf(entity).Encode())


class EntityList(object):
  """Holds list of entities, and their total size.

  Properties:
    entities: list of entities.
    length: length of entity list.
    size: aggregate entities size in bytes.
  """

  def __init__(self):
    """Constructor."""
    self.entities = []
    self.length = 0
    self.size = 0

  def append(self, entity, entity_size):
    """Add new entity to the list.

    Args:
      entity: an entity to add to the list.
      entity_size: entity size in bytes as int.
    """
    self.entities.append(entity)
    self.length += 1
    self.size += entity_size

  def clear(self):
    """Clear entity list."""
    self.entities = []
    self.length = 0
    self.size = 0


class MutationPool(object):
  """Mutation pool accumulates datastore changes to perform them in batch.

  Properties:
    puts: EntityList of entities to put to datastore.
    deletes: EntityList of entities to delete from datastore.
    max_pool_size: maximum single list pool size. List changes will be flushed
      when this size is reached.
  """

  def __init__(self, max_pool_size=MAX_POOL_SIZE):
    """Constructor.

    Args:
      max_pool_size: maximum pools size in bytes before flushing it to db.
    """
    self.max_pool_size = max_pool_size
    self.puts = EntityList()
    self.deletes = EntityList()

  def put(self, entity):
    """Registers entity to put to datastore.

    Args:
      entity: an entity to put.
    """
    entity_size = _get_entity_size(entity)
    if (self.puts.length >= MAX_ENTITY_COUNT or
        (self.puts.size + entity_size) > self.max_pool_size):
      self.__flush_puts()
    self.puts.append(entity, entity_size)

  def delete(self, entity):
    """Registers entity to delete from datastore.

    Args:
      entity: an entity to delete.
    """
    entity_size = _get_entity_size(entity)
    if (self.deletes.length >= MAX_ENTITY_COUNT or
        (self.deletes.size + entity_size) > self.max_pool_size):
      self.__flush_deletes()
    self.deletes.append(entity, entity_size)

  def flush(self):
    """Flush(apply) all changed to datastore."""
    self.__flush_puts()
    self.__flush_deletes()

  def __flush_puts(self):
    """Flush all puts to datastore."""
    db.put(self.puts.entities)
    self.puts.clear()

  def __flush_deletes(self):
    """Flush all deletes to datastore."""
    db.delete(self.deletes.entities)
    self.deletes.clear()


class Counters(object):
  """Regulates access to counters."""

  def __init__(self, shard_state):
    """Constructor.

    Args:
      shard_state: current mapreduce shard state as model.ShardState.
    """
    self._shard_state = shard_state

  def increment(self, counter_name, delta=1):
    """Increment counter value.

    Args:
      counter_name: name of the counter as string.
      delta: increment delta as int.
    """
    self._shard_state.counters_map.increment(counter_name, delta)

  def flush(self):
    """Flush unsaved counter values."""
    pass


class Context(object):
  """MapReduce execution context.

  Properties:
    mapreduce_spec: current mapreduce specification as model.MapreduceSpec.
    shard_state: current shard state as model.ShardState.
    mutation_pool: current mutation pool as MutationPool.
    counters: counters object as Counters.
  """

  _context_instance = None

  def __init__(self, mapreduce_spec, shard_state):
    """Constructor.

    Args:
      mapreduce_spec: mapreduce specification as model.MapreduceSpec.
      shard_state: shard state as model.ShardState.
    """
    self.mapreduce_spec = mapreduce_spec
    self.shard_state = shard_state

    self.mutation_pool = MutationPool()
    self.counters = Counters(shard_state)

    self._pools = {}
    self.register_pool("mutation_pool", self.mutation_pool)
    self.register_pool("counters", self.counters)

  def flush(self):
    """Flush all information recorded in context."""
    for pool in self._pools.values():
      pool.flush()
    if self.shard_state:
      self.shard_state.put()



  def register_pool(self, key, pool):
    """Register an arbitrary pool to be flushed together with this context.

    Args:
      key: pool key as string.
      pool: a pool instance. Pool should implement flush(self) method.
    """
    self._pools[key] = pool

  def get_pool(self, key):
    """Obtains an instance of registered pool.

    Args:
      key: pool key as string.

    Returns:
      an instance of the pool registered earlier, or None.
    """
    return self._pools.get(key, None)

  @classmethod
  def _set(cls, context):
    """Set current context instance.

    Args:
      context: new context as Context or None.
    """
    cls._context_instance = context


def get():
  """Get current context instance.

  Returns:
    current context as Context.
  """
  return Context._context_instance
