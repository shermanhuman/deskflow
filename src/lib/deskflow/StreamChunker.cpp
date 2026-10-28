/*
 * Deskflow -- mouse and keyboard sharing utility
 * SPDX-FileCopyrightText: (C) 2013 - 2016 Synergy App Ltd
 * SPDX-License-Identifier: GPL-2.0-only WITH LicenseRef-OpenSSL-Exception
 */

#include "deskflow/StreamChunker.h"

#include "base/Event.h"
#include "base/IEventQueue.h"
#include "base/Log.h"
#include "deskflow/ClipboardChunk.h"

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

StreamChunker::StreamChunker(IEventQueue *events, void *eventTarget) : m_events(events), m_eventTarget(eventTarget)
{
  // do nothing
}

StreamChunker::~StreamChunker()
{
  cancel();
}

void StreamChunker::sendClipboard(std::string data, ClipboardID id, uint32_t sequence)
{
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
}

void StreamChunker::pumpOne()
{
  if (m_transfers.empty()) {
    m_sending = false;
    return;
  }

  Transfer &transfer = m_transfers.front();

  // first message declares the total size
  if (!transfer.startSent) {
    transfer.startSent = true;
    postChunk(ClipboardChunk::start(transfer.id, transfer.sequence, std::to_string(transfer.data.size())));
    return;
  }

  // then the data, one chunk per iteration
  if (transfer.offset < transfer.data.size()) {
    const size_t size = std::min(g_chunkSize, transfer.data.size() - transfer.offset);
    postChunk(ClipboardChunk::data(transfer.id, transfer.sequence, transfer.data.substr(transfer.offset, size)));
    transfer.offset += size;
    return;
  }

  // and finally the end marker
  postChunk(ClipboardChunk::end(transfer.id, transfer.sequence));
  LOG_DEBUG("sent clipboard %d size=%zu", transfer.id, transfer.data.size());
  m_transfers.pop_front();
}

void StreamChunker::postChunk(ClipboardChunk *chunk)
{
  m_inFlight = true;
  m_events->addEvent(Event(EventTypes::ClipboardSending, m_eventTarget, chunk));
}
