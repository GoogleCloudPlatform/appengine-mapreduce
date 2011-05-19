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



from google.net.proto import ProtocolBuffer
import array
import dummy_thread as thread

__pychecker__ = """maxreturns=0 maxbranches=0 no-callinit
                   unusednames=printElemNumber,debug_strs no-special"""

class ShuffleEnums(ProtocolBuffer.ProtocolMessage):


  CSV_INPUT    =    0
  SSTABLE_KEY_VALUE_PROTO_INPUT =    1
  RECORD_INPUT =    2

  _InputFormat_NAMES = {
    0: "CSV_INPUT",
    1: "SSTABLE_KEY_VALUE_PROTO_INPUT",
    2: "RECORD_INPUT",
  }

  def InputFormat_Name(cls, x): return cls._InputFormat_NAMES.get(x, "")
  InputFormat_Name = classmethod(InputFormat_Name)



  CSV_OUTPUT   =    0
  SSTABLE_MULTI_VALUE_PROTO_OUTPUT =    1
  RECORD_OUTPUT =    2

  _OutputFormat_NAMES = {
    0: "CSV_OUTPUT",
    1: "SSTABLE_MULTI_VALUE_PROTO_OUTPUT",
    2: "RECORD_OUTPUT",
  }

  def OutputFormat_Name(cls, x): return cls._OutputFormat_NAMES.get(x, "")
  OutputFormat_Name = classmethod(OutputFormat_Name)



  UNKNOWN      =    1
  RUNNING      =    2
  SUCCESS      =    3
  FAILURE      =    4
  INVALID_INPUT =    5
  OUTPUT_ALREADY_EXISTS =    6
  INCORRECT_SHUFFLE_SIZE_BYTES =    7

  _Status_NAMES = {
    1: "UNKNOWN",
    2: "RUNNING",
    3: "SUCCESS",
    4: "FAILURE",
    5: "INVALID_INPUT",
    6: "OUTPUT_ALREADY_EXISTS",
    7: "INCORRECT_SHUFFLE_SIZE_BYTES",
  }

  def Status_Name(cls, x): return cls._Status_NAMES.get(x, "")
  Status_Name = classmethod(Status_Name)


  def __init__(self, contents=None):
    pass
    if contents is not None: self.MergeFromString(contents)


  def MergeFrom(self, x):
    assert x is not self

  def Equals(self, x):
    if x is self: return 1
    return 1

  def IsInitialized(self, debug_strs=None):
    initialized = 1
    return initialized

  def ByteSize(self):
    n = 0
    return n

  def ByteSizePartial(self):
    n = 0
    return n

  def Clear(self):
    pass

  def OutputUnchecked(self, out):
    pass

  def OutputPartial(self, out):
    pass

  def TryMerge(self, d):
    while d.avail() > 0:
      tt = d.getVarInt32()


      if (tt == 0): raise ProtocolBuffer.ProtocolBufferDecodeError
      d.skipData(tt)


  def __str__(self, prefix="", printElemNumber=0):
    res=""
    return res


  def _BuildTagLookupTable(sparse, maxtag, default=None):
    return tuple([sparse.get(i, default) for i in xrange(0, 1+maxtag)])


  _TEXT = _BuildTagLookupTable({
    0: "ErrorCode",
  }, 0)

  _TYPES = _BuildTagLookupTable({
    0: ProtocolBuffer.Encoder.NUMERIC,
  }, 0, ProtocolBuffer.Encoder.MAX_TYPE)


  _STYLE = """"""
  _STYLE_CONTENT_TYPE = """"""
class ShuffleInputSpecification(ProtocolBuffer.ProtocolMessage):
  has_format_ = 0
  format_ = 0
  has_path_ = 0
  path_ = ""

  def __init__(self, contents=None):
    if contents is not None: self.MergeFromString(contents)

  def format(self): return self.format_

  def set_format(self, x):
    self.has_format_ = 1
    self.format_ = x

  def clear_format(self):
    if self.has_format_:
      self.has_format_ = 0
      self.format_ = 0

  def has_format(self): return self.has_format_

  def path(self): return self.path_

  def set_path(self, x):
    self.has_path_ = 1
    self.path_ = x

  def clear_path(self):
    if self.has_path_:
      self.has_path_ = 0
      self.path_ = ""

  def has_path(self): return self.has_path_


  def MergeFrom(self, x):
    assert x is not self
    if (x.has_format()): self.set_format(x.format())
    if (x.has_path()): self.set_path(x.path())

  def Equals(self, x):
    if x is self: return 1
    if self.has_format_ != x.has_format_: return 0
    if self.has_format_ and self.format_ != x.format_: return 0
    if self.has_path_ != x.has_path_: return 0
    if self.has_path_ and self.path_ != x.path_: return 0
    return 1

  def IsInitialized(self, debug_strs=None):
    initialized = 1
    if (not self.has_path_):
      initialized = 0
      if debug_strs is not None:
        debug_strs.append('Required field: path not set.')
    return initialized

  def ByteSize(self):
    n = 0
    if (self.has_format_): n += 1 + self.lengthVarInt64(self.format_)
    n += self.lengthString(len(self.path_))
    return n + 1

  def ByteSizePartial(self):
    n = 0
    if (self.has_format_): n += 1 + self.lengthVarInt64(self.format_)
    if (self.has_path_):
      n += 1
      n += self.lengthString(len(self.path_))
    return n

  def Clear(self):
    self.clear_format()
    self.clear_path()

  def OutputUnchecked(self, out):
    if (self.has_format_):
      out.putVarInt32(8)
      out.putVarInt32(self.format_)
    out.putVarInt32(18)
    out.putPrefixedString(self.path_)

  def OutputPartial(self, out):
    if (self.has_format_):
      out.putVarInt32(8)
      out.putVarInt32(self.format_)
    if (self.has_path_):
      out.putVarInt32(18)
      out.putPrefixedString(self.path_)

  def TryMerge(self, d):
    while d.avail() > 0:
      tt = d.getVarInt32()
      if tt == 8:
        self.set_format(d.getVarInt32())
        continue
      if tt == 18:
        self.set_path(d.getPrefixedString())
        continue


      if (tt == 0): raise ProtocolBuffer.ProtocolBufferDecodeError
      d.skipData(tt)


  def __str__(self, prefix="", printElemNumber=0):
    res=""
    if self.has_format_: res+=prefix+("format: %s\n" % self.DebugFormatInt32(self.format_))
    if self.has_path_: res+=prefix+("path: %s\n" % self.DebugFormatString(self.path_))
    return res


  def _BuildTagLookupTable(sparse, maxtag, default=None):
    return tuple([sparse.get(i, default) for i in xrange(0, 1+maxtag)])

  kformat = 1
  kpath = 2

  _TEXT = _BuildTagLookupTable({
    0: "ErrorCode",
    1: "format",
    2: "path",
  }, 2)

  _TYPES = _BuildTagLookupTable({
    0: ProtocolBuffer.Encoder.NUMERIC,
    1: ProtocolBuffer.Encoder.NUMERIC,
    2: ProtocolBuffer.Encoder.STRING,
  }, 2, ProtocolBuffer.Encoder.MAX_TYPE)


  _STYLE = """"""
  _STYLE_CONTENT_TYPE = """"""
class ShuffleOutputSpecification(ProtocolBuffer.ProtocolMessage):
  has_format_ = 0
  format_ = 0
  has_path_base_ = 0
  path_base_ = ""

  def __init__(self, contents=None):
    if contents is not None: self.MergeFromString(contents)

  def format(self): return self.format_

  def set_format(self, x):
    self.has_format_ = 1
    self.format_ = x

  def clear_format(self):
    if self.has_format_:
      self.has_format_ = 0
      self.format_ = 0

  def has_format(self): return self.has_format_

  def path_base(self): return self.path_base_

  def set_path_base(self, x):
    self.has_path_base_ = 1
    self.path_base_ = x

  def clear_path_base(self):
    if self.has_path_base_:
      self.has_path_base_ = 0
      self.path_base_ = ""

  def has_path_base(self): return self.has_path_base_


  def MergeFrom(self, x):
    assert x is not self
    if (x.has_format()): self.set_format(x.format())
    if (x.has_path_base()): self.set_path_base(x.path_base())

  def Equals(self, x):
    if x is self: return 1
    if self.has_format_ != x.has_format_: return 0
    if self.has_format_ and self.format_ != x.format_: return 0
    if self.has_path_base_ != x.has_path_base_: return 0
    if self.has_path_base_ and self.path_base_ != x.path_base_: return 0
    return 1

  def IsInitialized(self, debug_strs=None):
    initialized = 1
    if (not self.has_path_base_):
      initialized = 0
      if debug_strs is not None:
        debug_strs.append('Required field: path_base not set.')
    return initialized

  def ByteSize(self):
    n = 0
    if (self.has_format_): n += 1 + self.lengthVarInt64(self.format_)
    n += self.lengthString(len(self.path_base_))
    return n + 1

  def ByteSizePartial(self):
    n = 0
    if (self.has_format_): n += 1 + self.lengthVarInt64(self.format_)
    if (self.has_path_base_):
      n += 1
      n += self.lengthString(len(self.path_base_))
    return n

  def Clear(self):
    self.clear_format()
    self.clear_path_base()

  def OutputUnchecked(self, out):
    if (self.has_format_):
      out.putVarInt32(8)
      out.putVarInt32(self.format_)
    out.putVarInt32(18)
    out.putPrefixedString(self.path_base_)

  def OutputPartial(self, out):
    if (self.has_format_):
      out.putVarInt32(8)
      out.putVarInt32(self.format_)
    if (self.has_path_base_):
      out.putVarInt32(18)
      out.putPrefixedString(self.path_base_)

  def TryMerge(self, d):
    while d.avail() > 0:
      tt = d.getVarInt32()
      if tt == 8:
        self.set_format(d.getVarInt32())
        continue
      if tt == 18:
        self.set_path_base(d.getPrefixedString())
        continue


      if (tt == 0): raise ProtocolBuffer.ProtocolBufferDecodeError
      d.skipData(tt)


  def __str__(self, prefix="", printElemNumber=0):
    res=""
    if self.has_format_: res+=prefix+("format: %s\n" % self.DebugFormatInt32(self.format_))
    if self.has_path_base_: res+=prefix+("path_base: %s\n" % self.DebugFormatString(self.path_base_))
    return res


  def _BuildTagLookupTable(sparse, maxtag, default=None):
    return tuple([sparse.get(i, default) for i in xrange(0, 1+maxtag)])

  kformat = 1
  kpath_base = 2

  _TEXT = _BuildTagLookupTable({
    0: "ErrorCode",
    1: "format",
    2: "path_base",
  }, 2)

  _TYPES = _BuildTagLookupTable({
    0: ProtocolBuffer.Encoder.NUMERIC,
    1: ProtocolBuffer.Encoder.NUMERIC,
    2: ProtocolBuffer.Encoder.STRING,
  }, 2, ProtocolBuffer.Encoder.MAX_TYPE)


  _STYLE = """"""
  _STYLE_CONTENT_TYPE = """"""

__all__ = ['ShuffleEnums','ShuffleInputSpecification','ShuffleOutputSpecification']
