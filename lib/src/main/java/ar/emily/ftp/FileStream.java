package ar.emily.ftp;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.lang.System.Logger;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

public interface FileStream {

  // this method solely exists because Path extends Iterable<Path>
  // and it is valid (although incorrect) to pass a Path to the method below this one
  static void sendFileTree(final Path root, final TransportAckStrategy transportAckStrategy) throws IOException {
    SendingFileVisitor.send(List.of(root), (AbstractTransportAckStrategy) transportAckStrategy);
  }

  static void sendFileTree(final Iterable<? extends Path> roots, final TransportAckStrategy transportAckStrategy) throws IOException {
    SendingFileVisitor.send(roots, (AbstractTransportAckStrategy) transportAckStrategy);
  }

  static void receiveFileTree(final Path dst, final TransportAckStrategy transportAckStrategy) throws IOException {
    FileStreamReceiver.receive(dst, (AbstractTransportAckStrategy) transportAckStrategy);
  }

  // see comment in #sendFileTree(Path, TransportAckStrategy)
  static void generateDigestFile(
      final Path root,
      final Path digestFile,
      final DigestParallelism parallelism
  ) throws IOException {
    generateDigestFile(List.of(root), digestFile, parallelism);
  }

  static void generateDigestFile(
      final Iterable<? extends Path> roots, final Path digestFile, final DigestParallelism parallelism
  ) throws IOException {
    General.ensure(!Files.isDirectory(digestFile), "Digest file must not be a directory");
    try {
      Files.createFile(digestFile);
    } catch (final FileAlreadyExistsException ex) {
      // ignore
    }
    General.ensure(Files.isWritable(digestFile), "Digest file must be a writable file");

    final long fileCount;
    {
      long acc = 0L;
      for (final Path path : roots) {
        try (final Stream<Path> s = Files.walk(path)) {
          acc +=
              s.filter(Predicate.not(Files::isDirectory))
                  .peek(file -> General.ensure(Files.isReadable(file), "File to digest must be readable: " + file))
                  .count();
        }
      }

      fileCount = acc;
    }

    Stream<Path> fileStream = Stream.empty();
    for (final Path root : roots) {
      fileStream = Stream.concat(fileStream, Files.walk(root));
    }

    try (final Stream<Path> s = fileStream.filter(Predicate.not(Files::isDirectory))) {
      parallelism.process(digestFile, fileCount, s::iterator);
    }
  }

  enum DigestParallelism {
    SERIAL {
      @Override
      void process(final Path digestFile, final long fileCount, final Iterable<? extends Path> fileSource) throws IOException {
        try (final FileChannel fc = FileChannel.open(digestFile, StandardOpenOption.WRITE)) {
          final MessageDigest md = FileDigestStorage.md();
          final var out = new ByteArrayOutputStream();
          final var writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8));
          long filesProcessed = 0L;
          for (final Path file : fileSource) {
            filesProcessed += 1L;
            General.LOGGER.log(
                Logger.Level.DEBUG, "{0,number} / {1,number} ({2,number,###.##%}) - ''{3}''",
                filesProcessed + 1, fileCount, (filesProcessed + 1.0) / fileCount, file
            );
            writer.write(file.toUri().toASCIIString());
            writer.write('\t');
            writer.write(Base64.getEncoder().encodeToString(FileDigestStorage.digest(file, md)));
            writer.write('\n');
            writer.flush();
            ChannelUtils.send(fc, ByteBuffer.wrap(out.toByteArray()));
            out.reset();
          }

          fc.truncate(fc.position());
        }
      }
    },
    PARALLEL {
      static final ThreadLocal<MessageDigest> MD = ThreadLocal.withInitial(FileDigestStorage::md);
      static final ThreadLocal<ByteArrayOutputStream> OUT = ThreadLocal.withInitial(ByteArrayOutputStream::new);
      static final ThreadLocal<Writer> WRITER = new ThreadLocal<>();

      @Override
      void process(final Path digestFile, final long fileCount, final Iterable<? extends Path> fileSource) throws IOException {
        final Iterator<? extends Path> it = fileSource.iterator();
        process0(digestFile, fileCount, new Supplier<>() {
          @Override
          public synchronized Path get() {
            return it.hasNext() ? it.next() : null;
          }
        });
      }

      static void process0(final Path digestFile, final long fileCount, final Supplier<? extends Path> fileSource) throws IOException {
        try (final FileChannel fc = FileChannel.open(digestFile, StandardOpenOption.WRITE)) {
          final var position = new AtomicLong(0L);
          final var filesProcessed = new AtomicLong(0L);
          Stream.generate(() -> null).parallel().map(__ -> fileSource.get()).takeWhile(Objects::nonNull).forEach(file -> {
            try {
              final long fileNo = filesProcessed.incrementAndGet();
              General.LOGGER.log(
                  Logger.Level.DEBUG, "{0,number} / {1,number} ({2,number,###.##%}) - ''{3}''",
                  fileNo, fileCount, (fileNo + 0.0) / fileCount, file
              );
              final MessageDigest md = MD.get();
              final ByteArrayOutputStream out = OUT.get();
              Writer writer = WRITER.get();
              if (writer == null) {
                WRITER.set(writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8)));
              }

              writer.write(file.toUri().toASCIIString());
              writer.write('\t');
              writer.write(Base64.getEncoder().encodeToString(FileDigestStorage.digest(file, md)));
              writer.write('\n');
              writer.flush();
              final ByteBuffer buff = ByteBuffer.wrap(out.toByteArray());
              ChannelUtils.write(fc, buff, position.getAndAdd(buff.remaining()));
              out.reset();
            } catch (final IOException ex) {
              throw new UncheckedIOException(ex);
            }
          });

          fc.truncate(position.get());
        }
      }
    };

    abstract void process(Path digestFile, long fileCount, Iterable<? extends Path> fileSource) throws IOException;
  }
}
