/*
 * Copyright (c) 2017-2022 TIBCO Software Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package com.gemstone.gemfire.internal.cache.store;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Set;
import java.util.function.BiConsumer;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.LowMemoryException;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.shared.BufferAllocator;
import com.gemstone.gemfire.internal.shared.unsafe.DirectBufferAllocator;
import com.gemstone.gemfire.internal.shared.unsafe.FreeMemory;
import com.gemstone.gemfire.internal.shared.unsafe.UnsafeHolder;
import com.gemstone.gemfire.internal.snappy.CallbackFactoryProvider;
import com.gemstone.gemfire.internal.snappy.StoreCallbacks;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.memory.MemoryAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Direct ByteBuffer implementation of {@link BufferAllocator} that integrates
 * with the SnappyData's UnifiedMemoryManager.
 */
public final class ManagedDirectBufferAllocator extends DirectBufferAllocator {

  private static final ManagedDirectBufferAllocator instance =
      new ManagedDirectBufferAllocator();

  private static final FreeMemory.Factory freeStoreBufferFactory =
      FreeStoreBuffer::new;

  public static ManagedDirectBufferAllocator instance() {
    DirectBufferAllocator.setInstance(instance);
    return instance;
  }

  private Logger logger = initLogger();

  private ManagedDirectBufferAllocator() {
  }

  private Logger initLogger() {
    return LoggerFactory.getLogger(getClass().getName());
  }

  private boolean reserveMemory(String objectName, long requiredSize,
      boolean shouldEvict) {
    return CallbackFactoryProvider.getStoreCallbacks().acquireStorageMemory(
        objectName, requiredSize, null, shouldEvict, true);
  }

  private boolean tryEvictData(String objectName, long requiredSpace) {
    UnsafeHolder.releasePendingReferences();
    if (reserveMemory(objectName, requiredSpace, true)) {
      return true;
    } else {
      // wait for runtime manager if required
      CallbackFactoryProvider.getStoreCallbacks().waitForRuntimeManager(10000);
      return reserveMemory(objectName, requiredSpace, true);
    }
  }

  @Override
  public RuntimeException lowMemoryException(String op, int required) {
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    DistributedMember myId = cache != null ? cache.getMyId() : null;
    Set<DistributedMember> m = myId != null ? Collections.singleton(myId)
        : Collections.emptySet();
    StoreCallbacks callbacks = CallbackFactoryProvider.getStoreCallbacks();
    String maxSpace;
    long used;
    if (op.startsWith("EXECUTION:")) {
      maxSpace = "maxExecution=" + callbacks.getExecutionPoolSize(true);
      used = callbacks.getExecutionPoolUsedMemory(true);
    } else {
      maxSpace = "maxStorage=" + callbacks.getStoragePoolSize(true);
      used = callbacks.getStoragePoolUsedMemory(true);
    }
    LowMemoryException lowMemory = new LowMemoryException(LocalizedStrings
        .ResourceManager_LOW_MEMORY_FOR_0_FUNCEXEC_MEMBERS_1
        .toLocalizedString("ManagedDirectBufferAllocator." + op + " (" +
            maxSpace + " used=" + used + " required=" + required + ')', m), m);
    logger.warn(lowMemory.toString());
    return lowMemory;
  }

  private FreeMemory.Factory freeBufferFactory(final String owner) {
    // allocating small objects like this on the fly is always more efficient
    // than map lookup etc
    return (address, size) -> new FreeBuffer(address, size, owner);
  }

  @Override
  public ByteBuffer allocate(int size, String owner) {
    return allocate(owner, size, freeBufferFactory(owner));
  }

  @Override
  public ByteBuffer allocateForStorage(int size) {
    return allocate(DIRECT_STORE_OBJECT_OWNER, size, freeStoreBufferFactory);
  }

  private ByteBuffer allocate(String objectName, int size,
      FreeMemory.Factory factory) {
    // calculate total size required as per allocation size (aligned to 8 bytes)
    // and the direct object overhead
    final int totalSize = UnsafeHolder.getAllocationSize(size) +
        DirectBufferAllocator.DIRECT_OBJECT_OVERHEAD;
    if (reserveMemory(objectName, totalSize, false) ||
        tryEvictData(objectName, totalSize)) {
      return allocateCustom(size, factory);
    } else {
      throw lowMemoryException("allocate", size);
    }
  }

  @Override
  public ByteBuffer expand(ByteBuffer buffer, int required, String owner) {
    assert required > 0 : "expand: unexpected required = " + required;

    final int currentUsed = buffer.limit();
    final int currentCapacity = buffer.capacity();
    if (currentUsed + required > currentCapacity) {
      final int newLength = BufferAllocator.expandedSize(currentUsed, required);
      int delta = UnsafeHolder.getAllocationSize(newLength) - currentCapacity;
      // expect original owner to be ManagedDirectBufferAllocator
      if (reserveMemory(owner, delta, false) ||
          tryEvictData(owner, delta)) {
        try {
          ByteBuffer newBuffer = UnsafeHolder.reallocateDirectBuffer(buffer,
              newLength, FreeBufferBase.class, freeBufferFactory(owner));
          if (MemoryAllocator.MEMORY_DEBUG_FILL_ENABLED) {
            // fill the remaining bytes
            ByteBuffer buf = newBuffer.duplicate();
            buf.position(currentUsed);
            fill(buf, MemoryAllocator.MEMORY_DEBUG_FILL_CLEAN_VALUE);
          }
          return newBuffer;
        } catch (IllegalStateException ise) {
          // un-reserve the delta bytes
          CallbackFactoryProvider.getStoreCallbacks().releaseStorageMemory(
              owner, delta, true);
          throw ise;
        }
      } else {
        throw lowMemoryException("expand", delta);
      }
    } else {
      buffer.limit(currentUsed + required);
      return buffer;
    }
  }

  @Override
  public void changeOwnerToStorage(ByteBuffer buffer, int capacity,
      BiConsumer<String, Object> changeOwner) {
    try {
      UnsafeHolder.changeDirectBufferCleaner(buffer, capacity,
          FreeBuffer.class, FreeStoreBuffer.class,
          freeStoreBufferFactory, changeOwner);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to change the owner of " +
          buffer + " to storage.", e);
    }
  }

  @Override
  public boolean isManagedDirect() {
    return true;
  }

  @Override
  public ByteBuffer allocateCustom(int size,
      FreeMemory.Factory factory) {
    ByteBuffer buffer = UnsafeHolder.allocateDirectBuffer(size, factory);
    if (MemoryAllocator.MEMORY_DEBUG_FILL_ENABLED) {
      fill(buffer, MemoryAllocator.MEMORY_DEBUG_FILL_CLEAN_VALUE);
    }
    return buffer;
  }

  @Override
  public void close() {
    UnsafeHolder.releasePendingReferences();
    // check that all memory has been released else try to release
    long allocated = CallbackFactoryProvider.getStoreCallbacks()
        .getOffHeapMemory(DIRECT_STORE_OBJECT_OWNER);
    if (allocated > 0) {
      UnsafeHolder.releasePendingReferences();
      allocated = CallbackFactoryProvider.getStoreCallbacks()
          .getOffHeapMemory(DIRECT_STORE_OBJECT_OWNER);
      if (allocated > 0) {
        // TODO: this needs to be observed since its quite possible that
        // unreleased references will remain especially in cache close
        // unless an explicit GC is invoked
        logger.info("Unreleased memory " + allocated + " bytes in close.");
      }
    }
    DirectBufferAllocator.resetInstance();
  }

  @SuppressWarnings("serial")
  static abstract class FreeBufferBase extends FreeMemory {

    protected final int size;

    FreeBufferBase(long address, int size) {
      super(address);
      this.size = size;
    }

    @Override
    protected abstract String objectName();

    @Override
    public final void run() {
      final long address = tryFree();
      if (address != 0) {
        Platform.freeMemory(address);
        try {
          // decrement the size from pool
          CallbackFactoryProvider.getStoreCallbacks().releaseStorageMemory(
              objectName(), this.size + DIRECT_OBJECT_OVERHEAD, true);
        } catch (Throwable t) {
          // ignore exceptions
          SystemFailure.checkFailure();
          try {
            ManagedDirectBufferAllocator.instance().logger.error(
                "FreeBuffer unexpected exception", t);
          } catch (Throwable ignored) {
            // ignore if even logging failed
          }
        }
      }
    }
  }

  @SuppressWarnings("serial")
  static final class FreeBuffer extends FreeBufferBase {

    private final String owner;

    FreeBuffer(long address, int size, String owner) {
      super(address, size);
      this.owner = owner;
    }

    protected String objectName() {
      return this.owner;
    }
  }

  @SuppressWarnings("serial")
  static final class FreeStoreBuffer extends FreeBufferBase {

    FreeStoreBuffer(long address, int size) {
      super(address, size);
    }

    protected String objectName() {
      return DIRECT_STORE_OBJECT_OWNER;
    }
  }
}
