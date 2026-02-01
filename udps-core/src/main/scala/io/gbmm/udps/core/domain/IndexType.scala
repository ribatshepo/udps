package io.gbmm.udps.core.domain

sealed trait IndexType extends Product with Serializable

object IndexType {
  case object ZoneMap extends IndexType
  case object BloomFilter extends IndexType
  case object Bitmap extends IndexType
  case object SkipList extends IndexType
  case object Inverted extends IndexType
  case object BTree extends IndexType
}
