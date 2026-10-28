/*
 * Deskflow -- mouse and keyboard sharing utility
 * SPDX-FileCopyrightText: (C) 2013 - 2016 Synergy App Ltd
 * SPDX-License-Identifier: GPL-2.0-only WITH LicenseRef-OpenSSL-Exception
 */

#pragma once

#include "deskflow/ClipboardTypes.h"

#include <cstdint>
#include <deque>
#include <string>

class ClipboardChunk;
class IEventQueue;

//! Paced clipboard transmitter
/*!
Splits clipboard data into chunks and emits them one at a time.

Clipboard chunks, input events and keep-alives all travel over the same event
queue and the same socket.  Queueing a whole clipboard at once therefore puts
every later mouse move, key press and keep-alive reply behind it: the pointer
freezes for the duration of the transfer, and on a large enough clipboard the
keep-alive reply misses its deadline and the connection is dropped.

This class keeps at most one chunk in flight.  The next chunk is only queued
once the previous one has been written *and* the stream has reported that its
output buffer drained (StreamOutputFlushed).  Waiting for the write alone is not
enough: the event loop turns far faster than a slow link drains, so chunks would
still pile up in the socket's output buffer ahead of any input written after
them.  Waiting for the drain bounds that buffer to roughly one chunk.
*/
class StreamChunker
{
public:
  StreamChunker(IEventQueue *events, void *eventTarget);
  ~StreamChunker();

  StreamChunker(StreamChunker const &) = delete;
  StreamChunker(StreamChunker &&) = delete;
  StreamChunker &operator=(StreamChunker const &) = delete;
  StreamChunker &operator=(StreamChunker &&) = delete;

  //! Queue clipboard data for transmission
  /*!
  Takes ownership of \p data.  Transfers are sent in the order queued.
  */
  void sendClipboard(std::string data, ClipboardID id, uint32_t sequence);

  //! The chunk from the last ClipboardSending event has been written to the stream
  void onChunkWritten();

  //! The stream's output buffer has drained; post the next pending chunk, if any
  void onStreamFlushed();

  //! Discard all queued transfers
  void cancel();

private:
  struct Transfer
  {
    std::string data;
    ClipboardID id = 0;
    uint32_t sequence = 0;
    size_t offset = 0;
    bool startSent = false;
  };

  void pumpOne();
  void postChunk(ClipboardChunk *chunk);

  IEventQueue *m_events = nullptr;
  void *m_eventTarget = nullptr;
  std::deque<Transfer> m_transfers;
  bool m_sending = false;
  bool m_inFlight = false;
};
