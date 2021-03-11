/*
 * Copyright (2020) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.standalone.internal.storage

import org.apache.hadoop.fs.Path

/**
 * General interface for all critical file system operations required to read and write the
 * [[DeltaLog]]. The correctness of the [[DeltaLog]] is predicated on the atomicity and
 * durability guarantees of the implementation of this interface. Specifically,
 *
 * 1. Atomic visibility of files: Any file written through this store must
 *    be made visible atomically. In other words, this should not generate partial files.
 *
 * 2. Mutual exclusion: Only one writer must be able to create (or rename) a file at the final
 *    destination.
 *
 * 3. Consistent listing: Once a file has been written in a directory, all future listings for
 *    that directory must return that file.
 */
trait LogStore extends ReadOnlyLogStore {
  /**
   * Write the given `actions` to the given `path` without overwriting any existing file.
   * Implementation must throw [[java.nio.file.FileAlreadyExistsException]] exception if the file
   * already exists. Furthermore, implementation must ensure that the entire file is made
   * visible atomically, that is, it should not generate partial files.
   */
  final def write(path: String, actions: Iterator[String]): Unit = write(new Path(path), actions)

  /**
   * Write the given `actions` to the given `path` with or without overwrite as indicated.
   * Implementation must throw [[java.nio.file.FileAlreadyExistsException]] exception if the file
   * already exists and overwrite = false. Furthermore, implementation must ensure that the
   * entire file is made visible atomically, that is, it should not generate partial files.
   */
  def write(path: Path, actions: Iterator[String], overwrite: Boolean = false): Unit

  /** Invalidate any caching that the implementation may be using */
  def invalidateCache(): Unit

  /** Resolve the fully qualified path for the given `path`. */
  def resolvePathOnPhysicalStorage(path: Path): Path = {
    throw new UnsupportedOperationException()
  }

  /**
   * Whether a partial write is visible when writing to `path`.
   *
   * As this depends on the underlying file system implementations, we require the input of `path`
   * here in order to identify the underlying file system, even though in most cases a log store
   * only deals with one file system.
   *
   * The default value is only provided here for legacy reasons, which will be removed.
   * Any LogStore implementation should override this instead of relying on the default.
   */
  def isPartialWriteVisible(path: Path): Boolean = true
}