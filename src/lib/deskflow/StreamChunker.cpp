/*
 * Deskflow -- mouse and keyboard sharing utility
 * SPDX-FileCopyrightText: (C) 2026 Deskflow Developers
 * SPDX-FileCopyrightText: (C) 2013 - 2016 Synergy App Ltd
 * SPDX-License-Identifier: GPL-2.0-only WITH LicenseRef-OpenSSL-Exception
 */

#include "deskflow/StreamChunker.h"

#include "base/Event.h"
#include "base/IEventQueue.h"
#include "base/Log.h"
#include "deskflow/ClipboardChunk.h"
#include "deskflow/ipc/CoreIpc.h"

#include <memory>

#include <algorithm>
#include <string>

namespace {

/*
Chunk size is a latency/overhead trade-off, not a wire format constant: the
receiver accumulates chunks until the end marker and does not care how the
data was split, so this can be changed freely without breaking compatibility.

Because the next chunk waits for the socket's output buffer to drain, a chunk
is the most clipboard data that can sit in that buffer ahead of an input event.
32 KiB is roughly 13 ms on a 20 Mbit link and negligible on anything faster,
while keeping the number of event loop iterations low (a 50 MB clipboard is
~1600 chunks).  The kernel send buffer below it is not bounded by this.
*/
constexpr size_t g_chunkSize = 32 * 1024;

} // namespace

StreamChunker::StreamChunker(IEventQueue *events, void *eventTarget, const QString &peerName)
    : m_events(events),
      m_eventTarget(eventTarget),
      m_peerName(peerName)
{
  // do nothing
}

StreamChunker::~StreamChunker()
{
  cancel();
}

void StreamChunker::sendClipboard(std::string data, ClipboardID id, uint32_t sequence)
{
  // a newer copy supersedes the older one for the same clipboard; a transfer already on the wire
  // restarts, since a receiver starts over on a new start chunk
  for (size_t i = 0; i < m_transfers.size(); ++i) {
    if (m_transfers[i].id == id) {
      if (i == 0 && m_sending && m_transfers[i].startSent) {
        LOG_DEBUG("restarting clipboard %d transfer with newer data", id);
      }
      m_transfers[i].data = std::move(data);
      m_transfers[i].sequence = sequence;
      m_transfers[i].offset = 0;
      m_transfers[i].startSent = false;
      return;
    }
  }

  Transfer transfer;
  transfer.data = std::move(data);
  transfer.id = id;
  transfer.sequence = sequence;
  m_transfers.push_back(std::move(transfer));

  // if a transfer is already running it will pick this one up when it finishes
  if (!m_sending) {
    m_sending = true;
    pumpOne();
  }
}

void StreamChunker::onChunkWritten()
{
  m_inFlight = false;
  m_inFlightChunk = nullptr;
}

void StreamChunker::onStreamFlushed()
{
  // a flush that was already queued before the last chunk was written is stale:
  // that chunk will produce its own flush once it drains
  if (m_sending && !m_inFlight) {
    pumpOne();
  }
}

void StreamChunker::cancel()
{
  m_transfers.clear();
  m_sending = false;
  m_inFlight = false;
  m_inFlightChunk = nullptr;
}

void StreamChunker::pumpOne()
{
  if (auto *chunk = nextChunk(); chunk != nullptr) {
    postChunk(chunk);
  }
}

ClipboardChunk *StreamChunker::nextChunk()
{
  if (m_transfers.empty()) {
    m_sending = false;
    return nullptr;
  }

  Transfer &transfer = m_transfers.front();

  // first message declares the total size
  if (!transfer.startSent) {
    transfer.startSent = true;
    transfer.sinceStart.start();
    ipcSendToClient(
        QStringLiteral("clipboardSending"), QStringLiteral("%1,%2").arg(transfer.data.size()).arg(m_peerName)
    );
    return ClipboardChunk::start(transfer.id, transfer.sequence, std::to_string(transfer.data.size()));
  }

  // then the data, one chunk per iteration
  if (transfer.offset < transfer.data.size()) {
    const size_t size = std::min(g_chunkSize, transfer.data.size() - transfer.offset);
    auto *chunk = ClipboardChunk::data(transfer.id, transfer.sequence, transfer.data.substr(transfer.offset, size));
    transfer.offset += size;
    return chunk;
  }

  // and finally the end marker
  auto *chunk = ClipboardChunk::end(transfer.id, transfer.sequence);
  const auto description = ClipboardChunk::describeTransfer(transfer.data.size(), transfer.sinceStart.elapsed());
  LOG_DEBUG("sent clipboard %d: %s", transfer.id, description.constData());
  ipcSendToClient(QStringLiteral("clipboardSent"), m_peerName);
  m_transfers.pop_front();
  return chunk;
}

void StreamChunker::sendRemaining(deskflow::IStream *stream)
{
  if (!m_sending) {
    return;
  }
  LOG_DEBUG("sending the rest of the clipboard transfer ahead of input");

  // the posted chunk goes first; its event is still queued, so the chunk is still alive
  if (m_inFlight) {
    ClipboardChunk::send(stream, m_inFlightChunk);
    ++m_skippedChunks;
    m_inFlight = false;
    m_inFlightChunk = nullptr;
  }

  while (m_sending) {
    if (const std::unique_ptr<ClipboardChunk> chunk(nextChunk()); chunk != nullptr) {
      ClipboardChunk::send(stream, chunk.get());
    }
  }
}

bool StreamChunker::takeSkippedChunk()
{
  if (m_skippedChunks == 0) {
    return false;
  }
  --m_skippedChunks;
  return true;
}

void StreamChunker::postChunk(ClipboardChunk *chunk)
{
  m_inFlight = true;
  m_inFlightChunk = chunk;
  m_events->addEvent(Event(EventTypes::ClipboardSending, m_eventTarget, chunk));
}
