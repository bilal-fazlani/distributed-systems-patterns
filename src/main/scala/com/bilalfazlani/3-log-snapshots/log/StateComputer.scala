package com.bilalfazlani.logSnapshots
package log

trait StateComputer[Item, State]:
  val zero: State
  def compute(state: State, item: Item): State
