#!/usr/bin/env python
"""Interface for user defined mapper."""

# pylint: disable=invalid-name


# TODO(user): Move common APIs to parent class.
class Mapper(object):
  """Interface user's mapper should implement.

  Each shard initiates one instance. The instance is pickled
  and unpickled if a shard can't finish within the boundary of a single
  task (a.k.a a slice of the shard).

  Upon shard retry, a new instance will be used.

  Upon slice retry, the instance is unpickled from its state
  at the end of last slice.

  Be wary of the size of your mapper instances. They have to be persisted
  across slices.
  """

  def __init__(self):
    """Init.

    Init must not take additional arguments.
    """
    pass

  def begin_shard(self, ctx):
    """Called at the beginning of a shard.

    This method may be called more than once due to slice retry.
    Make it idempotent.

    Args:
      ctx: map_job.ShardContext object.
    """
    pass

  def end_shard(self, ctx):
    """Called at the end of a shard.

    This method may be called more than once due to slice retry.
    Make it idempotent.

    If shard execution error out before reaching the end, this method
    won't be called.

    Args:
      ctx: map_job.ShardContext object.
    """
    pass

  def begin_slice(self, ctx):
    """Called at the beginning of a slice.

    This method may be called more than once due to slice retry.
    Make it idempotent.

    Args:
      ctx: map_job.SliceContext object.
    """
    pass

  def end_slice(self, ctx):
    """Called at the end of a slice.

    This method may be called more than once due to slice retry.
    Make it idempotent.

    If slice execution error out before reaching the end, this method
    won't be called.

    Args:
      ctx: map_job.SliceContext object.
    """
    pass

  def __call__(self, ctx, val):
    """Called for every value yielded by input reader.

    Normal case:
    This method is invoked exactly once on each input value. If an
    output writer is provided, this method can repeated call ctx.emit to
    write to output writer.

    On retries:
    Upon slice retry, some input value may have been processed by
    the previous attempt. This should not be a problem if your logic
    is idempotent (e.g write to datastore by key) but could be a
    problem otherwise (e.g write to cloud storage) and may result
    in duplicates.

    Advanced usage:
    Implementation can mimic combiner by tallying in-memory and
    and emit when memory is filling up or when end_slice() is called.
    CAUTION! Carefully tune to not to exceed memory limit or request deadline.

    Args:
      ctx: map_job.SliceContext object.
      val: a single value yielded by your input reader. The type
        depends on the input reader. For example, some may yield a single
        datastore entity, others may yield a (int, str) tuple.
    """
    pass
