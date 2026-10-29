/*
 * Deskflow -- mouse and keyboard sharing utility
 * SPDX-FileCopyrightText: (C) 2026 Deskflow Developers
 * SPDX-License-Identifier: GPL-2.0-only WITH LicenseRef-OpenSSL-Exception
 */

#include "base/Log.h"

#include <QTest>

class ClipboardSenderTests : public QObject
{
  Q_OBJECT
private Q_SLOTS:
  void initTestCase();
  // Test are run in order top to bottom
  void sendClipboardPostsOnlyFirstChunk();
  void nextChunkWaitsForStreamFlush();
  void staleFlushWhileChunkInFlightIsIgnored();
  void chunksAreEmittedOneAtATime();
  void transferRoundTripsData();
  void emptyClipboardSendsStartAndEnd();
  void queuedTransfersDoNotInterleave();
  void newerCopySupersedesQueuedTransfer();
  void newerCopyRestartsTransferOnTheWire();
  void cancelStopsTransfer();
  void sendRemainingWritesInFlightChunkFirst();
  void sendRemainingWithNothingQueuedWritesNothing();

private:
  Log m_log;
};
