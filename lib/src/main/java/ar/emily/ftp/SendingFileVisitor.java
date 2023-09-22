package ar.emily.ftp;

import java.io.IOException;
import java.lang.System.Logger;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.NumberFormat;
import java.util.function.Predicate;
import java.util.stream.Stream;

final class SendingFileVisitor implements FileVisitor<Path> {

  static void send(final Iterable<? extends Path> roots, final AbstractTransportAckStrategy transportAckStrategy) throws IOException {
    final long fileCount;
    {
      long acc = 0L;
      for (final Path path : roots) {
        try (final Stream<Path> s = Files.walk(path)) {
          acc += s.filter(Predicate.not(Files::isDirectory)).count();
        }
      }

      fileCount = acc;
    }

    if (transportAckStrategy instanceof final DigestAckTransportAckStrategy digestAck) {
      digestAck.digestStorage().load();
    }

    final var visitor = new SendingFileVisitor(transportAckStrategy, fileCount);
    visitor.send(visitor.stateBuffer(ChannelUtils.VISIT_START), visitor.fileCountBuffer());
    for (final Path root : roots) {
      Files.walkFileTree(root, visitor);
    }
    visitor.send(visitor.stateBuffer(ChannelUtils.VISIT_COMPLETE));

    if (transportAckStrategy instanceof final DigestAckTransportAckStrategy digestAck) {
      digestAck.digestStorage().save();
    }
  }

  private final AbstractTransportAckStrategy transportAckStrategy;
  private final WritableByteChannel dst;
  private final ByteBuffer dataBuffer;
  private final NumberFormat sizeFormatter;
  private final long fileCount;
  private long filesTransferred;
  private int bufferPosition;

  private SendingFileVisitor(final AbstractTransportAckStrategy transportAckStrategy, final long fileCount) {
    this.transportAckStrategy = transportAckStrategy;
    this.dst = transportAckStrategy.dst();
    this.dataBuffer = ByteBuffer.allocateDirect(32);
    this.bufferPosition = 0;
    this.sizeFormatter = General.sizeFormatter();
    this.fileCount = fileCount;
    this.filesTransferred = 0L;
  }

  private ByteBuffer bufferSlice(final int size) {
    final int pos = this.bufferPosition;
    this.bufferPosition += size;
    return this.dataBuffer.slice(pos, size);
  }

  private ByteBuffer stateBuffer(final byte state) {
    assert state == ChannelUtils.VISIT_COMPLETE ||
        state == ChannelUtils.VISIT_START ||
        state == ChannelUtils.VISIT_DIR ||
        state == ChannelUtils.VISIT_FILE ||
        state == ChannelUtils.VISIT_FILE_FAILED ||
        state == ChannelUtils.VISIT_PARENT;
    return bufferSlice(1).put(state).flip();
  }

  private ByteBuffer fileCountBuffer() {
    return bufferSlice(8).putLong(this.fileCount).flip();
  }

  private void pathNameBuffers(final Path path, final ByteBuffer[] dst) {
    final byte[] encodedPathName = path.getFileName().toString().getBytes(StandardCharsets.UTF_8);
    //dst[0] = stateBuffer(VISIT_DIR/VISIT_FILE/VISIT_FILE_FAILED)
    dst[1] = bufferSlice(4).putInt(encodedPathName.length).flip();
    dst[2] = ByteBuffer.wrap(encodedPathName);
  }

  private ByteBuffer sizeBuffer(final BasicFileAttributes attrs) {
    assert attrs.isRegularFile();
    return bufferSlice(8).putLong(attrs.size()).flip();
  }

  private void send(final ByteBuffer... buffers) throws IOException {
    ChannelUtils.send(this.dst, buffers);
    this.bufferPosition = 0;
  }

  @Override
  public FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attrs) throws IOException {
    General.LOGGER.log(Logger.Level.DEBUG, "VISIT_DIR");
    General.LOGGER.log(Logger.Level.DEBUG, "dir = ''{0}''", dir.getFileName());
    final ByteBuffer[] buffers = new ByteBuffer[3];
    buffers[0] = stateBuffer(ChannelUtils.VISIT_DIR);
    pathNameBuffers(dir, buffers);
    send(buffers);
    return FileVisitResult.CONTINUE;
  }

  @Override
  public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
    General.LOGGER.log(Logger.Level.DEBUG, "VISIT_FILE {0,number} / {1,number} ({2,number,###.##%})", this.filesTransferred + 1, this.fileCount, (this.filesTransferred + 1.0) / this.fileCount);
    General.LOGGER.log(Logger.Level.DEBUG, "file = ''{0}''", file.getFileName());
    General.LOGGER.log(Logger.Level.DEBUG, "size = {0}", this.sizeFormatter.format(attrs.size()));
    final ByteBuffer[] buffers = new ByteBuffer[4];
    buffers[0] = stateBuffer(ChannelUtils.VISIT_FILE);
    pathNameBuffers(file, buffers);
    buffers[3] = sizeBuffer(attrs);
    send(buffers);
    this.transportAckStrategy.sendFile(file, this.dataBuffer.slice());
    this.filesTransferred += 1;
    return FileVisitResult.CONTINUE;
  }

  @Override
  public FileVisitResult visitFileFailed(final Path file, final IOException exc) throws IOException {
    General.LOGGER.log(Logger.Level.ERROR, "VISIT_FILE_FAILED {0,number} / {1,number} ({2,number,###.##%})", this.filesTransferred + 1, this.fileCount, (this.filesTransferred + 1.0) / this.fileCount);
    General.LOGGER.log(Logger.Level.ERROR, "file = ''{0}''", file.getFileName());
    General.LOGGER.log(Logger.Level.ERROR, (String) null, exc);
    final ByteBuffer[] buffers = new ByteBuffer[3];
    buffers[0] = stateBuffer(ChannelUtils.VISIT_FILE_FAILED);
    pathNameBuffers(file, buffers);
    send(buffers);
    this.filesTransferred += 1;
    return FileVisitResult.CONTINUE;
  }

  @Override
  public FileVisitResult postVisitDirectory(final Path dir, final IOException exc) throws IOException {
    if (exc != null) {
      General.LOGGER.log(Logger.Level.ERROR, "VISIT_PARENT", exc);
    } else {
      General.LOGGER.log(Logger.Level.DEBUG, "VISIT_PARENT");
    }

    send(stateBuffer(ChannelUtils.VISIT_PARENT));
    return FileVisitResult.CONTINUE;
  }
}
