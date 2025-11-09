/*
 * Copyright (C) 2023 Pr. Olivier Gruber
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package utils;

/**
 * This circular buffer of bytes can be used to pass bytes between two threads:
 * one thread pushing bytes in the buffer and the other pulling bytes from the
 * buffer. The buffer policy is FIFO: first byte in is the first byte out.
 */
public class CircularBuffer {
  volatile int m_tail, m_head;
  volatile byte m_bytes[];
  volatile boolean m_closed;

  public CircularBuffer(int capacity) {
    m_bytes = new byte[capacity];
    m_tail = m_head = 0;
    m_closed = false;
  }

  /**
   * @return true if this buffer is full, false otherwise
   */
  public boolean full() {
    int next = (m_head + 1) % m_bytes.length;
    return (next == m_tail);
  }

  /**
   * @return true if this buffer is empty, false otherwise
   */
  public boolean empty() {
    return (m_tail == m_head);
  }

  /**
   * @param b: the byte to push in the buffer
   * @return the next available byte
   * @throws an IllegalStateException if full.
   */
  public synchronized void push(byte b) {
    int next = (m_head + 1) % m_bytes.length;
    if (next == m_tail)
      throw new IllegalStateException();
    m_bytes[m_head] = b;
    m_head = next;
    notifyAll();
  }

  /**
   * @return the next available byte
   * @throws an IllegalStateException if empty.
   */
  public synchronized byte pull() {
    if (m_tail == m_head)
      throw new IllegalStateException();
    int next = (m_tail + 1) % m_bytes.length;
    byte bits = m_bytes[m_tail];
    m_tail = next;
    notifyAll();
    return bits;
  }

  /**
   * Push multiple bytes into the buffer.
   * Blocks until at least one byte can be written.
   *
   * @param bytes the source array
   * @param offset starting position in the source array
   * @param length maximum number of bytes to push
   * @return the actual number of bytes pushed
   * @throws IllegalArgumentException if offset or length are invalid
   */
  public synchronized int push(byte[] bytes, int offset, int length) {
    if (bytes == null)
      throw new IllegalArgumentException("bytes array cannot be null");
    if (offset < 0 || length < 0 || offset + length > bytes.length)
      throw new IllegalArgumentException("Invalid offset or length");

    // Cannot push to a closed buffer
    if (m_closed)
      return 0;

    // Wait until there is at least one space available
    while (full() && !m_closed) {
      try {
        wait();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return 0;
      }
    }

    // Check again if closed after waking up
    if (m_closed)
      return 0;

    // Push as many bytes as possible
    int pushed = 0;
    while (pushed < length && !full()) {
      int next = (m_head + 1) % m_bytes.length;
      m_bytes[m_head] = bytes[offset + pushed];
      m_head = next;
      pushed++;
    }

    // Notify threads waiting to pull
    notifyAll();
    return pushed;
  }

  /**
   * Pull multiple bytes from the buffer.
   * Blocks until at least one byte is available.
   *
   * @param bytes the destination array
   * @param offset starting position in the destination array
   * @param length maximum number of bytes to pull
   * @return the actual number of bytes pulled
   * @throws IllegalArgumentException if offset or length are invalid
   */
  public synchronized int pull(byte[] bytes, int offset, int length) {
    if (bytes == null)
      throw new IllegalArgumentException("bytes array cannot be null");
    if (offset < 0 || length < 0 || offset + length > bytes.length)
      throw new IllegalArgumentException("Invalid offset or length");

    // Wait until there is at least one byte available or buffer is closed
    while (empty() && !m_closed) {
      try {
        wait();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return 0;
      }
    }

    // If closed and empty, return 0 (end of stream)
    if (m_closed && empty())
      return 0;

    // Pull as many bytes as possible
    int pulled = 0;
    while (pulled < length && !empty()) {
      int next = (m_tail + 1) % m_bytes.length;
      bytes[offset + pulled] = m_bytes[m_tail];
      m_tail = next;
      pulled++;
    }

    // Notify threads waiting to push
    notifyAll();
    return pulled;
  }

  /**
   * Close this buffer. All blocked threads will be awakened.
   * After closing, push operations will fail and pull operations
   * will return remaining data then return 0.
   */
  public synchronized void close() {
    m_closed = true;
    notifyAll();
  }

  /**
   * @return true if this buffer is closed, false otherwise
   */
  public boolean closed() {
    return m_closed;
  }

}
