package com.bilalfazlani.logSegmentation
package log

trait StateComputer[Item, State]:
  val zero: State
  def compute(state: State, item: Item): State
