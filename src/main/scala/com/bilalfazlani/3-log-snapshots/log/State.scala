package com.bilalfazlani.logSnapshots
package log

import zio.UIO

trait State[A]:
  def all: UIO[A]
