package ar.emily.ftp;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.security.MessageDigest;

interface ChannelUtils {

  byte ACK_ACCEPT = 1;
  byte ACK_REJECT = 0;

  byte VISIT_COMPLETE = -1;
  byte VISIT_START = 0;
  byte VISIT_DIR = 1;
  byte VISIT_FILE = 2;
  byte VISIT_FILE_FAILED = 3;
  byte VISIT_PARENT = 4;

  static void write(final FileChannel fc, final ByteBuffer src, long position) throws IOException {
    while (src.hasRemaining()) {
      position += fc.write(src, position);
    }
  }

  static void send(final WritableByteChannel ch, final ByteBuffer... srcs) throws IOException {
    if (ch instanceof final GatheringByteChannel gathering) {
      long remaining = 0L;
      for (final ByteBuffer buffer : srcs) {
        remaining += buffer.remaining();
      }
      while (remaining != 0L) {
        remaining -= gathering.write(srcs);
      }
    } else {
      for (final ByteBuffer src : srcs) {
        while (src.hasRemaining()) {
          ch.write(src);
        }
      }
    }
  }

  static void recv(final ReadableByteChannel ch, final ByteBuffer dst) throws IOException {
    while (dst.hasRemaining()) {
      ch.read(dst);
    }
  }

  static void transferTo(final FileChannel fc, final WritableByteChannel dst) throws IOException {
    long position = fc.position();
    long remaining = fc.size() - position;
    while (remaining != 0L) {
      final long transferred = fc.transferTo(position, remaining, dst);
      remaining -= transferred;
      position += transferred;
    }
  }

  static void transferFrom(final FileChannel fc, final ReadableByteChannel src, final long count) throws IOException {
    long position = fc.position();
    long remaining = count;
    while (remaining != 0L) {
      final long transferred = fc.transferFrom(src, position, remaining);
      remaining -= transferred;
      position += transferred;
    }
  }

  static byte[] digestingTransferFrom(final FileChannel fc, final ReadableByteChannel src, final long count, final MessageDigest md) throws IOException {
    long position = fc.position();
    long remaining = count;
    while (remaining != 0L) {
      final long transferred = fc.transferFrom(src, position, remaining);
      md.update(fc.map(FileChannel.MapMode.READ_ONLY, position, transferred));
      remaining -= transferred;
      position += transferred;
    }

    return md.digest();
  }
}
