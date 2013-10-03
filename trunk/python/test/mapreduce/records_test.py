#!/usr/bin/env python
"""Tests for records.py."""

from __future__ import with_statement

import array
import unittest

from mapreduce.lib import crc32c
from mapreduce import records


class StringWriter(object):
  """records.FileWriter compliant writer to string."""

  def __init__(self):
    self.data = ''

  def write(self, bytes):
    self.data += bytes

  def toarray(self):
    a = array.array('B')
    a.fromstring(self.data)
    return a

  def tolist(self):
    return self.toarray().tolist()


class StringReader(object):
  """records.FileReader compliant reader from string."""

  def __init__(self, data):
    self.data = data
    self.position = 0

  def read(self, length):
    result = self.data[self.position:self.position + length]
    self.position = min(self.position + length, len(self.data))
    return result

  def tell(self):
    return self.position

  def seek(self, position):
    self.position = position


class RecordsTest(unittest.TestCase):
  """Test records operations."""

  def setUp(self):
    # Work with 10 bytes blocks.
    records._BLOCK_SIZE = 20

  def testMaskUnmaskCrc(self):
    """Test masking and unmasking crc."""
    crc = crc32c.crc('foo')
    self.assertNotEquals(crc, records._mask_crc(crc))
    self.assertNotEquals(crc, records._mask_crc(records._mask_crc(crc)))
    self.assertEqual(crc, records._unmask_crc(records._mask_crc(crc)))
    self.assertEqual(
        crc,
        records._unmask_crc(records._unmask_crc(
            records._mask_crc(records._mask_crc(crc)))))

    # This value gave me troubles.
    crc = 2685849682
    self.assertEquals(crc, records._unmask_crc(records._mask_crc(crc)))

  def testWriteEmptyRecordWithPadding(self):
    """Test writing empty record."""
    writer = StringWriter()

    with records.RecordsWriter(writer) as w:
      w.write('')
      self.assertSequenceEqual(
          [
              # Record 1
              5, 43, 40, 67,    # crc
              0, 0,             # length
              1,                # type
          ],
          writer.tolist())
      w._pad_block()

    self.assertSequenceEqual(
        [
            # Record 1
            5, 43, 40, 67,    # crc
            0, 0,             # length
            1,                # type
            # Padding
            0, 0, 0, 0, 0,
            0, 0, 0, 0, 0,
            0, 0, 0,
        ],
        writer.tolist())

  def testWriteWholeBlocks(self):
    """Test writing whole block records."""
    writer = StringWriter()

    with records.RecordsWriter(writer) as w:
      w.write('1' * 13)

      self.assertSequenceEqual(
          [
              # Record 1
              134, 248, 6, 190,  # crc
              13, 0,             # length
              1,                 # type
              49, 49, 49, 49, 49,
              49, 49, 49, 49, 49,
              49, 49, 49,
          ],
          writer.tolist())
      w._pad_block()

    self.assertSequenceEqual(
        [
            # Record 1
            134, 248, 6, 190,  # crc
            13, 0,             # length
            1,                 # type
            49, 49, 49, 49, 49,
            49, 49, 49, 49, 49,
            49, 49, 49,
        ],
        writer.tolist())

  def testWriteNoRoomForHeader(self):
    """Test writing a record that doesn't have room for next record in block."""
    writer = StringWriter()

    with records.RecordsWriter(writer) as w:
      w.write('1' * 10)

      self.assertSequenceEqual(
          [
              # Record 1
              79, 121, 169, 29,  # crc
              10, 0,             # length
              1,                 # type
              49, 49, 49, 49, 49,
              49, 49, 49, 49, 49,
          ],
          writer.tolist())

      w.write('1' * 10)

      self.assertSequenceEqual(
          [
              # Record 1
              79, 121, 169, 29,  # crc
              10, 0,             # length
              1,                 # type
              49, 49, 49, 49, 49,
              49, 49, 49, 49, 49,
              # Padding
              0, 0, 0,
              # Record 2
              79, 121, 169, 29,  # crc
              10, 0,             # length
              1,                 # type
              49, 49, 49, 49, 49,
              49, 49, 49, 49, 49,
          ],
          writer.tolist())

    self.assertSequenceEqual(
        [
            # Record 1
            79, 121, 169, 29,  # crc
            10, 0,             # length
            1,                 # type
            49, 49, 49, 49, 49,
            49, 49, 49, 49, 49,
            # Padding
            0, 0, 0,
            # Record 2
            79, 121, 169, 29,  # crc
            10, 0,             # length
            1,                 # type
            49, 49, 49, 49, 49,
            49, 49, 49, 49, 49,
            # No Padding
        ],
        writer.tolist())

  def testWriteLargeRecord(self):
    """Test writing a record larger than block."""
    writer = StringWriter()

    with records.RecordsWriter(writer) as w:
      w.write('1' * 60)

    self.assertSequenceEqual(
        [
            # Chunk 1
            149, 224, 148, 134,# crc
            13, 0,             # length
            2,                 # type
            49, 49, 49, 49, 49,
            49, 49, 49, 49, 49,
            49, 49, 49,
            # Chunk 2
            139, 72, 32, 241,  # crc
            13, 0,             # length
            3,                 # type
            49, 49, 49, 49, 49,
            49, 49, 49, 49, 49,
            49, 49, 49,
            # Chunk 3
            139, 72, 32, 241,  # crc
            13, 0,             # length
            3,                 # type
            49, 49, 49, 49, 49,
            49, 49, 49, 49, 49,
            49, 49, 49,
            # Chunk 4
            139, 72, 32, 241,  # crc
            13, 0,             # length
            3,                 # type
            49, 49, 49, 49, 49,
            49, 49, 49, 49, 49,
            49, 49, 49,
            # Chunk 5
            198, 68, 73, 41,   # crc
            8, 0,              # length
            4,                 # type
            49, 49, 49, 49, 49,
            49, 49, 49,
            # No Padding
        ],
        writer.tolist())

  def testWriteHeaderAtTheEndOfBlock(self):
    """Test writing a header when there's exactly 7 bytes left in block."""
    writer = StringWriter()

    with records.RecordsWriter(writer) as w:
      w.write('1' * 6)
      w.write('1' * 10)
    self.assertSequenceEqual(
        [
            # Record 1
            43, 18, 162, 121,    # crc
            6, 0,                # length
            1,                   # type
            49, 49, 49, 49, 49,
            49,
            # Record 2, chunk 1
            100, 81, 208, 233, # crc
            0, 0,              # length
            2,                 # type
            # Record 2, chunk 2
            130, 247, 235, 147, # crc
            10, 0,              # length
            4,                  # type
            49, 49, 49, 49, 49,
            49, 49, 49, 49, 49,
            # No Padding
        ],
        writer.tolist())

  def testPadBlockIdempotency(self):
    """Test _pad_block is idempotent."""
    writer = StringWriter()

    with records.RecordsWriter(writer) as w:
      w.write('')
      w._pad_block()
      w._pad_block()
      w._pad_block()
      w._pad_block()

    reader = records.RecordsReader(StringReader(writer.data))
    self.assertEqual('', reader.read())
    self.assertEqual(records._BLOCK_SIZE, len(writer.data))

  def testReadEmptyRecord(self):
    """Test reading empty records."""
    writer = StringWriter()

    with records.RecordsWriter(writer) as w:
      w.write('')
      w._pad_block()

    with records.RecordsWriter(writer) as w:
      w.write('')

    reader = records.RecordsReader(StringReader(writer.data))

    self.assertEqual('', reader.read())
    # Should correctly skip padding.
    self.assertEqual('', reader.read())
    self.assertRaises(EOFError, reader.read)

  def testReadWholeBlocks(self):
    """Test reading record occupying a whole block."""
    writer = StringWriter()

    with records.RecordsWriter(writer) as w:
      w.write('1' * 13)
      w._pad_block()

    with records.RecordsWriter(writer) as w:
      w.write('1' * 13)

    reader = records.RecordsReader(StringReader(writer.data))

    self.assertEqual('1' * 13, reader.read())
    self.assertEqual('1' * 13, reader.read())
    self.assertRaises(EOFError, reader.read)

  def testReadNoRoomForHeader(self):
    """Test reading records that leave <7 bytes in a block."""
    writer = StringWriter()

    with records.RecordsWriter(writer) as w:
      w.write('1' * 10)
      w.write('1' * 10)

    reader = records.RecordsReader(StringReader(writer.data))

    self.assertEqual('1' * 10, reader.read())
    self.assertEqual('1' * 10, reader.read())
    self.assertRaises(EOFError, reader.read)

  def testReadLargeRecords(self):
    """Test reading large headers."""
    writer = StringWriter()

    with records.RecordsWriter(writer) as w:
      w.write('1' * 10)
      w.write('1' * 20)
      w.write('1' * 30)
      w.write('1' * 40)
      w.write('1' * 50)
      w.write('1' * 60)
      w.write('1' * 70)

    reader = records.RecordsReader(StringReader(writer.data))

    self.assertEqual('1' * 10, reader.read())
    self.assertEqual('1' * 20, reader.read())
    self.assertEqual('1' * 30, reader.read())
    self.assertEqual('1' * 40, reader.read())
    self.assertEqual('1' * 50, reader.read())
    self.assertEqual('1' * 60, reader.read())
    self.assertEqual('1' * 70, reader.read())
    self.assertRaises(EOFError, reader.read)

  def testReadHeaderAtTheEndOfTheBlock(self):
    """Test reading records, that leave exactly 7 bytes at the end of block."""
    writer = StringWriter()

    with records.RecordsWriter(writer) as w:
      w.write('1' * 6)
      w.write('1' * 10)

    reader = records.RecordsReader(StringReader(writer.data))

    self.assertEqual('1' * 6, reader.read())
    self.assertEqual('1' * 10, reader.read())
    self.assertRaises(EOFError, reader.read)

  def testReadCorruptedCrcSmallRecord(self):
    """Test reading small records with corrupted crc."""
    writer = StringWriter()

    with records.RecordsWriter(writer) as w:
      # Block 1
      w.write('1' * 2)
      w.write('1' * 2)
      # Block 2
      w.write('1' * 2)
      w.write('1' * 2)

    data = writer.data
    data = '_' + data[1:]
    reader = records.RecordsReader(StringReader(data))

    # First  block should be completely skipped.
    self.assertEqual('1' * 2, reader.read())
    self.assertEqual('1' * 2, reader.read())
    self.assertRaises(EOFError, reader.read)

  def testReadCorruptedCrcLargeRecord(self):
    """Test reading large records with corrupted crc."""
    writer = StringWriter()

    with records.RecordsWriter(writer) as w:
      # Blocks 1-6
      w.write('1' * 100)
      # Block 7
      w.write('1' * 2)

    data = writer.data
    data = '_' + data[1:]
    reader = records.RecordsReader(StringReader(data))

    # First record should be completely skipped.
    self.assertEqual('1' * 2, reader.read())
    self.assertRaises(EOFError, reader.read)

  def testReadCorruptedLength(self):
    """Test reading record with corrupted length."""
    writer = StringWriter()

    with records.RecordsWriter(writer) as w:
      # Block 1
      w.write('1' * 2)
      w.write('1' * 2)
      # Block 2
      w.write('1' * 2)
      w.write('1' * 2)

    data = writer.data
    # replace length by 65535
    data = data[:4] + '\xff\xff' + data[6:]
    reader = records.RecordsReader(StringReader(data))

    # First  block should be completely skipped.
    self.assertEqual('1' * 2, reader.read())
    self.assertEqual('1' * 2, reader.read())
    self.assertRaises(EOFError, reader.read)

  def testReadCorruptedRecordOrder_FirstThenFirst(self):
    """Tests corruption when a first record is followed by a first record."""
    writer = StringWriter()

    with records.RecordsWriter(writer) as w:
      # Fake first record that should be ignored
      w._RecordsWriter__write_record(
          records._RECORD_TYPE_FIRST, 'A' * (records._BLOCK_SIZE / 2))

      # Multi-block record. This will cause 'A' to be ignored because
      # of a repeated first record.
      w.write('B' * 2 * records._BLOCK_SIZE)

    data = writer.data
    reader = records.RecordsReader(StringReader(data))
    self.assertEqual('B' * 2 * records._BLOCK_SIZE, reader.read())
    self.assertRaises(EOFError, reader.read)

  def testReadCorruptedRecordOrder_FirstThenFull(self):
    """Tests corruption when a first record is followed by a full record."""
    writer = StringWriter()

    with records.RecordsWriter(writer) as w:
      # Fake first record that should be ignored
      w._RecordsWriter__write_record(
          records._RECORD_TYPE_FIRST, 'A' * (records._BLOCK_SIZE / 2))

      # Single-block, "full" record.
      w.write('B' * (records._BLOCK_SIZE / 4))

    data = writer.data
    reader = records.RecordsReader(StringReader(data))
    self.assertEqual('B' * (records._BLOCK_SIZE / 4), reader.read())
    self.assertRaises(EOFError, reader.read)

  def testReadCorruptedRecordOrder_MiddleThenFull(self):
    """Tests corruption when a middle record is followed by a full record."""
    writer = StringWriter()

    with records.RecordsWriter(writer) as w:
      # Fake middle record that should be ignored
      w._RecordsWriter__write_record(
          records._RECORD_TYPE_MIDDLE, 'A' * (records._BLOCK_SIZE / 2))

      # Single-block, "full" record.
      w.write('B' * (records._BLOCK_SIZE / 4))

    data = writer.data
    reader = records.RecordsReader(StringReader(data))
    self.assertEqual('B' * (records._BLOCK_SIZE / 4), reader.read())
    self.assertRaises(EOFError, reader.read)

  def testReadCorruptedRecordOrder_LastThenFull(self):
    """Tests corruption when a last record is followed by a full record."""
    writer = StringWriter()

    with records.RecordsWriter(writer) as w:
      # Fake last record that should be ignored
      w._RecordsWriter__write_record(
          records._RECORD_TYPE_LAST, 'A' * (records._BLOCK_SIZE / 2))

      # Single-block, "full" record.
      w.write('B' * (records._BLOCK_SIZE / 4))

    data = writer.data
    reader = records.RecordsReader(StringReader(data))
    self.assertEqual('B' * (records._BLOCK_SIZE / 4), reader.read())
    self.assertRaises(EOFError, reader.read)

  def testIter(self):
    """Test reader iterator interface."""
    writer = StringWriter()

    with records.RecordsWriter(writer) as w:
      w.write('1' * 1)
      w.write('2' * 2)
      w.write('3' * 3)
      w.write('4' * 4)

    reader = records.RecordsReader(StringReader(writer.data))
    self.assertEqual(['1', '22', '333', '4444'], list(reader))

  def testReadTruncatedBuffer(self):
    """Test reading records from truncated file."""
    writer = StringWriter()

    with records.RecordsWriter(writer) as w:
      # Block 1
      w.write('1' * 2)
      w.write('1' * 2)
      # Block 2
      w.write('1' * 2)
      w.write('1' * 2)

    data = writer.data

    while data:
      data = data[:-1]
      reader = records.RecordsReader(StringReader(data))
      count = len(list(reader))
      if len(data) >= 38:
        self.assertEqual(4, count)
      elif len(data) >= 29:
        self.assertEqual(3, count)
      elif len(data) >= 18:
        self.assertEqual(2, count)
      elif len(data) >= 9:
        self.assertEqual(1, count)
      else:
        self.assertEqual(0, count)

  def testSmoke(self):
    """Smoke test of all cases.

    Other smaller tests are more revealing in particular situations.
    """
    input_size = 0
    # Try many input sizes!
    while input_size < records._BLOCK_SIZE * 3:
      writer = StringWriter()
      inputs = '1' * input_size
      with records.RecordsWriter(writer) as w:
        # Make sure even the smallest input covers more than one block.
        for _ in range(records._BLOCK_SIZE):
          w.write(inputs)

      reader = records.RecordsReader(StringReader(writer.data))
      for _ in range(records._BLOCK_SIZE):
        self.assertEqual(inputs, reader.read())
      self.assertRaises(EOFError, reader.read)
      input_size += 1


if __name__ == '__main__':
  unittest.main()
