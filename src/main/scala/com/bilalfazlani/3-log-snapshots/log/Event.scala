package com.bilalfazlani.logSnapshots
package log

enum Event:
  case PointerMoved(point: Point)
  case LowWaterMarkChanged(lwm: Long)
  case DateDiscarded(filesDeleted: List[String])
