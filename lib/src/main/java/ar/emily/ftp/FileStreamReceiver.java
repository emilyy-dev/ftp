package ar.emily.ftp;

import java.io.IOException;
import java.lang.System.Logger;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.NumberFormat;
import java.util.Objects;

final class FileStreamReceiver {

  static void receive(final Path dst, final AbstractTransportAckStrategy transportAckStrategy) throws IOException {
    General.ensure(Files.isDirectory(dst), "Destination must be a directory");
    General.ensure(Files.isWritable(dst), "Destination must be a directory");
    if (transportAckStrategy instanceof final DigestAckTransportAckStrategy digestAck) {
      digestAck.digestStorage().load();
    }

    final var receiver = new FileStreamReceiver(dst, transportAckStrategy);
    receiver.run();

    if (transportAckStrategy instanceof final DigestAckTransportAckStrategy digestAck) {
      digestAck.digestStorage().save();
    }
  }

  private final AbstractTransportAckStrategy transportAckStrategy;
  private final ReadableByteChannel src;
  private final ByteBuffer dataBuffer;
  private final NumberFormat sizeFormatter;
  private Path currentDir;
  private long fileCount;
  private long filesDownloaded;

  private FileStreamReceiver(final Path dst, final AbstractTransportAckStrategy transportAckStrategy) {
    this.transportAckStrategy = transportAckStrategy;
    this.src = transportAckStrategy.src();
    this.currentDir = Objects.requireNonNull(dst, "dst");
    this.dataBuffer = ByteBuffer.allocateDirect(32);
    this.sizeFormatter = General.sizeFormatter();
    this.fileCount = 0L;
    this.filesDownloaded = 0L;
  }

  private byte readState() throws IOException {
    ChannelUtils.recv(this.src, this.dataBuffer.clear().limit(1));
    return this.dataBuffer.flip().get();
  }

  private long readSize() throws IOException {
    ChannelUtils.recv(this.src, this.dataBuffer.clear().limit(8));
    return this.dataBuffer.flip().getLong();
  }

  private String readPath() throws IOException {
    ChannelUtils.recv(this.src, this.dataBuffer.clear().limit(4));
    final ByteBuffer encodedPath = ByteBuffer.allocate(this.dataBuffer.flip().getInt());
    ChannelUtils.recv(this.src, encodedPath);
    return new String(encodedPath.array(), StandardCharsets.UTF_8);
  }

  public void run() throws IOException {
    while (true) {
      final byte state = readState();
      switch (state) {
        case ChannelUtils.VISIT_START -> {
          General.LOGGER.log(Logger.Level.DEBUG, "VISIT_START");
          ChannelUtils.recv(this.src, this.dataBuffer.clear().limit(8));
          this.fileCount = this.dataBuffer.flip().getLong();
          General.LOGGER.log(Logger.Level.DEBUG, "file count = {0,number}", this.fileCount);
        }
        case ChannelUtils.VISIT_DIR -> {
          General.LOGGER.log(Logger.Level.DEBUG, "VISIT_DIR");
          this.currentDir = Files.createDirectories(this.currentDir.resolve(readPath()));
          General.LOGGER.log(Logger.Level.DEBUG, "dir = ''{0}''", this.currentDir.getFileName());
        }
        case ChannelUtils.VISIT_PARENT -> {
          General.LOGGER.log(Logger.Level.DEBUG, "VISIT_PARENT");
          this.currentDir = this.currentDir.getParent();
        }
        case ChannelUtils.VISIT_FILE -> {
          General.LOGGER.log(Logger.Level.DEBUG, "VISIT_FILE");
          final Path file = this.currentDir.resolve(readPath());
          General.LOGGER.log(Logger.Level.DEBUG, "file = ''{0}''", file.getFileName());
          final long size = readSize();
          General.LOGGER.log(Logger.Level.DEBUG, "size = {0}", this.sizeFormatter.format(size));
          this.transportAckStrategy.receiveFile(file, this.dataBuffer.clear().slice(), size);
          this.filesDownloaded += 1;
          General.LOGGER.log(Logger.Level.DEBUG, "{0,number} / {1,number} ({2,number,###.##%})", this.filesDownloaded, this.fileCount, (double) this.filesDownloaded / this.fileCount);
          General.LOGGER.log(Logger.Level.DEBUG, "Exited VISIT_FILE");
        }
        case ChannelUtils.VISIT_FILE_FAILED -> {
          General.LOGGER.log(Logger.Level.ERROR, "VISIT_FILE_FAILED");
          General.LOGGER.log(Logger.Level.ERROR, "file = '{0}'", readPath());
          this.filesDownloaded += 1;
          General.LOGGER.log(Logger.Level.DEBUG, "{0,number} / {1,number} ({2,number,###.##%})", this.filesDownloaded, this.fileCount, (double) this.filesDownloaded / this.fileCount);
        }
        case ChannelUtils.VISIT_COMPLETE -> {
          General.LOGGER.log(Logger.Level.DEBUG, "VISIT_COMPLETE");
          return;
        }
        default -> throw new IllegalStateException("Unknown state " + state);
      }
    }
  }
}
