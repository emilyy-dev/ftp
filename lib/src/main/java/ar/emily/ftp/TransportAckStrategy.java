package ar.emily.ftp;

import java.io.IOException;
import java.lang.System.Logger;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;

public sealed interface TransportAckStrategy {

  static TransportAckStrategy digestAck(final Path digestFile, final ByteChannel ch) {
    return digestAck(digestFile, ch, ch);
  }

  static TransportAckStrategy digestAck(
      final Path digestFile,
      final WritableByteChannel dst,
      final ReadableByteChannel acknowledgementSource
  ) {
    General.ensure(Files.isReadable(digestFile) && Files.isWritable(digestFile), "Digest file must be a readable and writable file");
    return new DigestAckTransportAckStrategy(new FileDigestStorage(digestFile), dst, acknowledgementSource);
  }

  static TransportAckStrategy nack(final ByteChannel ch) {
    return nack(ch, ch);
  }

  static TransportAckStrategy nack(final WritableByteChannel dst, final ReadableByteChannel src) {
    return new NackTransportAckStrategy(dst, src);
  }
}

sealed interface AbstractTransportAckStrategy extends TransportAckStrategy {

  WritableByteChannel dst();
  ReadableByteChannel src();
  void sendFile(Path file, ByteBuffer smallBuffer) throws IOException;
  void receiveFile(Path file, ByteBuffer smallBuffer, long size) throws IOException;
}

record DigestAckTransportAckStrategy(
    FileDigestStorage digestStorage,
    WritableByteChannel dst,
    ReadableByteChannel src
) implements AbstractTransportAckStrategy {

  @Override
  public void sendFile(final Path file, final ByteBuffer smallBuffer) throws IOException {
    General.LOGGER.log(Logger.Level.DEBUG, "Retrieving file digest");
    final byte[] digest = this.digestStorage.getOrCalculateFileDigest(file);
    ChannelUtils.send(this.dst, ByteBuffer.wrap(digest));
    General.LOGGER.log(Logger.Level.DEBUG, "Awaiting remote response");
    ChannelUtils.recv(this.src, smallBuffer.limit(1));
    if (smallBuffer.flip().get() != ChannelUtils.ACK_REJECT) {
      General.LOGGER.log(Logger.Level.DEBUG, "Sending");
      try (final FileChannel fc = FileChannel.open(file, StandardOpenOption.READ)) {
        ChannelUtils.transferTo(fc, this.dst);
      }
    } else {
      General.LOGGER.log(Logger.Level.DEBUG, "Skipping");
    }
  }

  @Override
  public void receiveFile(final Path file, final ByteBuffer smallBuffer, final long size) throws IOException {
    final byte[] expectedDigest = new byte[16];
    if (Files.notExists(file) || Files.size(file) != size) {
      ChannelUtils.recv(this.src, smallBuffer.limit(16));
      smallBuffer.flip().get(expectedDigest).clear();
      ChannelUtils.send(this.dst, smallBuffer.limit(1).put(ChannelUtils.ACK_ACCEPT).flip());
      General.LOGGER.log(Logger.Level.DEBUG, "Receiving");
      final byte[] calculatedDigest;
      try (final FileChannel fc = FileChannel.open(file, StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
        calculatedDigest = ChannelUtils.digestingTransferFrom(fc, this.src, size, this.digestStorage.md);
        fc.truncate(size);
      }

      this.digestStorage.remember(file, calculatedDigest);
      if (!MessageDigest.isEqual(calculatedDigest, expectedDigest)) {
        General.LOGGER.log(
            Logger.Level.WARNING, "Received file digest differs from expected digest{0}File: ''{1}''{0}Expected digest: {2}{0}Calculated digest: {3}",
            System.lineSeparator(), file, FileDigestStorage.formatHex(expectedDigest), FileDigestStorage.formatHex(calculatedDigest)
        );
      }
    } else {
      General.LOGGER.log(Logger.Level.DEBUG, "Retrieving local file digest");
      final byte[] localDigest = this.digestStorage.getOrCalculateFileDigest(file);
      General.LOGGER.log(Logger.Level.DEBUG, "Awaiting remote file digest");
      ChannelUtils.recv(this.src, smallBuffer.limit(16));
      smallBuffer.flip().get(expectedDigest).clear();
      if (MessageDigest.isEqual(expectedDigest, localDigest)) {
        General.LOGGER.log(Logger.Level.DEBUG, "Skipping");
        ChannelUtils.send(this.dst, smallBuffer.limit(1).put(ChannelUtils.ACK_REJECT).flip());
      } else {
        General.LOGGER.log(Logger.Level.DEBUG, "Receiving");
        ChannelUtils.send(this.dst, smallBuffer.limit(1).put(ChannelUtils.ACK_ACCEPT).flip());
        try (final FileChannel fc = FileChannel.open(file, StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
          ChannelUtils.transferFrom(fc, this.src, size);
          fc.truncate(size);
        }

        this.digestStorage.remember(file, expectedDigest);
      }
    }
  }
}

record NackTransportAckStrategy(
    WritableByteChannel dst,
    ReadableByteChannel src
) implements AbstractTransportAckStrategy {

  @Override
  public void sendFile(final Path file, final ByteBuffer smallBuffer) throws IOException {
    try (final FileChannel fc = FileChannel.open(file, StandardOpenOption.READ)) {
      ChannelUtils.transferTo(fc, this.dst);
    }
  }

  @Override
  public void receiveFile(final Path file, final ByteBuffer smallBuffer, final long size) throws IOException {
    try (final FileChannel fc = FileChannel.open(file, StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
      ChannelUtils.transferFrom(fc, this.src, size);
      fc.truncate(size);
    }
  }
}
