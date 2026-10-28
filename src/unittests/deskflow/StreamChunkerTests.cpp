/*
 * Deskflow -- mouse and keyboard sharing utility
 * SPDX-FileCopyrightText: (C) 2026 Deskflow Developers
 * SPDX-License-Identifier: GPL-2.0-only WITH LicenseRef-OpenSSL-Exception
 */

#include "StreamChunkerTests.h"

#include "MockEventQueue.h"
#include "deskflow/ClipboardChunk.h"
#include "deskflow/StreamChunker.h"

#include <cstring>
#include <string>
#include <vector>

namespace {

struct Captured
{
  ClipboardID id = 0;
  uint32_t sequence = 0;
  uint8_t mark = 0;
  std::string payload;
};

//! Event queue that records the chunks posted to it
class CapturingEventQueue : public MockEventQueue
{
public:
  void addEvent(Event &&event) override
  {
    if (event.getType() != EventTypes::ClipboardSending) {
      return;
    }

    const auto *chunk = static_cast<const ClipboardChunk *>(event.getDataObject());
    Captured captured;
    captured.id = static_cast<ClipboardID>(chunk->m_chunk[0]);
    std::memcpy(&captured.sequence, &chunk->m_chunk[1], 4);
    captured.mark = static_cast<uint8_t>(chunk->m_chunk[5]);
    captured.payload.assign(&chunk->m_chunk[6], chunk->m_dataSize);
    m_captured.push_back(std::move(captured));

    Event::deleteData(event);
  }

  std::vector<Captured> m_captured;
};

struct DrainResult
{
  std::vector<Captured> chunks;
  bool onePerPump = true;
  bool terminated = false;
};

//! Drive the chunker to completion, recording whether it ever posted more than one chunk per drain
DrainResult drain(StreamChunker &chunker, CapturingEventQueue &queue)
{
  // guards against a pump that never terminates
  constexpr size_t maxIterations = 100000;
  DrainResult result;

  for (size_t i = 0; i < maxIterations; ++i) {
    const size_t before = queue.m_captured.size();
    // the proxy writes the posted chunk, then the socket drains
    chunker.onChunkWritten();
    chunker.onStreamFlushed();
    const size_t posted = queue.m_captured.size() - before;

    if (posted == 0) {
      result.terminated = true;
      break;
    }

    // the whole point of the chunker: never more than one chunk in flight
    if (posted != 1) {
      result.onePerPump = false;
      break;
    }
  }

  result.chunks = queue.m_captured;
  return result;
}

std::string reassemble(const std::vector<Captured> &captured)
{
  std::string data;
  for (const auto &chunk : captured) {
    if (chunk.mark == ChunkType::DataChunk) {
      data += chunk.payload;
    }
  }
  return data;
}

std::string makeData(size_t size)
{
  std::string data(size, '\0');
  for (size_t i = 0; i < size; ++i) {
    data[i] = static_cast<char>('a' + (i % 26));
  }
  return data;
}

// large enough to span several chunks whatever the chunk size is tuned to
constexpr size_t s_largeSize = 1024 * 1024;

} // namespace

/*
Regression test for clipboard transfers starving input.

The old implementation queued every chunk of the clipboard in a single
synchronous loop, so a large clipboard put the whole transfer in front of any
input event and keep-alive reply that followed it.  Starting a transfer must
post exactly one chunk.
*/
void StreamChunkerTests::sendClipboardPostsOnlyFirstChunk()
{
  CapturingEventQueue queue;
  StreamChunker chunker(&queue, nullptr);

  chunker.sendClipboard(makeData(s_largeSize), 0, 0);

  QCOMPARE(queue.m_captured.size(), static_cast<size_t>(1));
  QCOMPARE(queue.m_captured.front().mark, static_cast<uint8_t>(ChunkType::DataStart));
}

/*
Regression test for the socket output buffer filling up on slow links.

Posting the next chunk as soon as the previous one was written still lets the
event loop outrun the network: every chunk lands in the socket's unbounded
output buffer within a few loop iterations, and input written after them waits
for all of it to drain.  The next chunk must wait for StreamOutputFlushed.
*/
void StreamChunkerTests::nextChunkWaitsForStreamFlush()
{
  CapturingEventQueue queue;
  StreamChunker chunker(&queue, nullptr);

  chunker.sendClipboard(makeData(s_largeSize), 0, 0);
  QCOMPARE(queue.m_captured.size(), static_cast<size_t>(1));

  // written, but the link has not drained yet: nothing more may be queued
  chunker.onChunkWritten();
  QCOMPARE(queue.m_captured.size(), static_cast<size_t>(1));

  chunker.onStreamFlushed();
  QCOMPARE(queue.m_captured.size(), static_cast<size_t>(2));
}

void StreamChunkerTests::staleFlushWhileChunkInFlightIsIgnored()
{
  CapturingEventQueue queue;
  StreamChunker chunker(&queue, nullptr);

  chunker.sendClipboard(makeData(s_largeSize), 0, 0);

  // a flush queued before the start chunk was written must not release another chunk
  chunker.onStreamFlushed();
  QCOMPARE(queue.m_captured.size(), static_cast<size_t>(1));

  chunker.onChunkWritten();
  chunker.onStreamFlushed();
  QCOMPARE(queue.m_captured.size(), static_cast<size_t>(2));
}

void StreamChunkerTests::chunksAreEmittedOneAtATime()
{
  CapturingEventQueue queue;
  StreamChunker chunker(&queue, nullptr);

  chunker.sendClipboard(makeData(s_largeSize), 0, 0);
  const auto drained = drain(chunker, queue);
  QVERIFY(drained.terminated);
  QVERIFY(drained.onePerPump);
  const auto &captured = drained.chunks;

  // start + data chunks + end
  QVERIFY(captured.size() > 3);
  QCOMPARE(captured.front().mark, static_cast<uint8_t>(ChunkType::DataStart));
  QCOMPARE(captured.back().mark, static_cast<uint8_t>(ChunkType::DataEnd));

  // the payload really was split, rather than sent as one chunk
  size_t dataChunks = 0;
  for (size_t i = 1; i + 1 < captured.size(); ++i) {
    QCOMPARE(captured[i].mark, static_cast<uint8_t>(ChunkType::DataChunk));
    QVERIFY(!captured[i].payload.empty());
    ++dataChunks;
  }
  QVERIFY(dataChunks > 1);

  // a chunk bounds how much clipboard data can sit in front of an input event,
  // so it must stay a small fraction of the transfer
  QVERIFY(captured[1].payload.size() < s_largeSize / 4);
}

void StreamChunkerTests::transferRoundTripsData()
{
  CapturingEventQueue queue;
  StreamChunker chunker(&queue, nullptr);

  const std::string data = makeData(s_largeSize + 7); // deliberately not a chunk multiple
  chunker.sendClipboard(data, 1, 42);
  const auto drained = drain(chunker, queue);
  QVERIFY(drained.terminated);
  QVERIFY(drained.onePerPump);
  const auto &captured = drained.chunks;

  QCOMPARE(reassemble(captured), data);
  QCOMPARE(captured.front().payload, std::to_string(data.size()));

  for (const auto &chunk : captured) {
    QCOMPARE(chunk.id, static_cast<ClipboardID>(1));
    QCOMPARE(chunk.sequence, static_cast<uint32_t>(42));
  }
}

void StreamChunkerTests::emptyClipboardSendsStartAndEnd()
{
  CapturingEventQueue queue;
  StreamChunker chunker(&queue, nullptr);

  chunker.sendClipboard(std::string(), 0, 0);
  const auto drained = drain(chunker, queue);
  QVERIFY(drained.terminated);
  const auto &captured = drained.chunks;

  QCOMPARE(captured.size(), static_cast<size_t>(2));
  QCOMPARE(captured.front().mark, static_cast<uint8_t>(ChunkType::DataStart));
  QCOMPARE(captured.front().payload, std::string("0"));
  QCOMPARE(captured.back().mark, static_cast<uint8_t>(ChunkType::DataEnd));
}

void StreamChunkerTests::queuedTransfersDoNotInterleave()
{
  CapturingEventQueue queue;
  StreamChunker chunker(&queue, nullptr);

  const std::string first = makeData(s_largeSize);
  const std::string second = makeData(2048);
  chunker.sendClipboard(first, 0, 1);
  chunker.sendClipboard(second, 1, 2);

  // the second transfer must not start before the first one has finished
  QCOMPARE(queue.m_captured.size(), static_cast<size_t>(1));

  const auto drained = drain(chunker, queue);
  QVERIFY(drained.terminated);
  QVERIFY(drained.onePerPump);
  const auto &captured = drained.chunks;

  std::string firstData;
  std::string secondData;
  bool firstFinished = false;
  for (const auto &chunk : captured) {
    if (!firstFinished) {
      QCOMPARE(chunk.id, static_cast<ClipboardID>(0));
      if (chunk.mark == ChunkType::DataChunk) {
        firstData += chunk.payload;
      } else if (chunk.mark == ChunkType::DataEnd) {
        firstFinished = true;
      }
    } else {
      QCOMPARE(chunk.id, static_cast<ClipboardID>(1));
      if (chunk.mark == ChunkType::DataChunk) {
        secondData += chunk.payload;
      }
    }
  }

  QVERIFY(firstFinished);
  QCOMPARE(firstData, first);
  QCOMPARE(secondData, second);
}

void StreamChunkerTests::newerCopySupersedesQueuedTransfer()
{
  CapturingEventQueue queue;
  StreamChunker chunker(&queue, nullptr);

  // clipboard 0 is on the wire; clipboard 1 is copied three times meanwhile
  chunker.sendClipboard(makeData(s_largeSize), 0, 1);
  chunker.sendClipboard(std::string("first"), 1, 2);
  chunker.sendClipboard(std::string("second"), 1, 3);
  chunker.sendClipboard(std::string("third"), 1, 4);

  const auto drained = drain(chunker, queue);
  QVERIFY(drained.terminated);

  size_t clipboardOneStarts = 0;
  std::string clipboardOneData;
  uint32_t clipboardOneSequence = 0;
  for (const auto &chunk : drained.chunks) {
    if (chunk.id != 1) {
      continue;
    }
    clipboardOneSequence = chunk.sequence;
    if (chunk.mark == ChunkType::DataStart) {
      ++clipboardOneStarts;
    } else if (chunk.mark == ChunkType::DataChunk) {
      clipboardOneData += chunk.payload;
    }
  }

  QCOMPARE(clipboardOneStarts, static_cast<size_t>(1));
  QCOMPARE(clipboardOneData, std::string("third"));
  QCOMPARE(clipboardOneSequence, static_cast<uint32_t>(4));
}

void StreamChunkerTests::cancelStopsTransfer()
{
  CapturingEventQueue queue;
  StreamChunker chunker(&queue, nullptr);

  chunker.sendClipboard(makeData(s_largeSize), 0, 0);
  chunker.onChunkWritten();
  chunker.onStreamFlushed();
  const size_t beforeCancel = queue.m_captured.size();

  chunker.cancel();
  chunker.onChunkWritten();
  chunker.onStreamFlushed();

  QCOMPARE(queue.m_captured.size(), beforeCancel);
}

QTEST_MAIN(StreamChunkerTests)
