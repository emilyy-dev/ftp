package ar.emily.ftp;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.System.Logger;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

final class FileDigestStorage {

  private static final HexFormat HEX_FORMAT = HexFormat.of();
  private static final MessageDigest MD;

  static {
    try {
      MD = MessageDigest.getInstance("MD5");
    } catch (final NoSuchAlgorithmException ex) {
      // should not throw
      throw new ExceptionInInitializerError(ex);
    }
  }

  static MessageDigest md() {
    try {
      return (MessageDigest) MD.clone();
    } catch (final CloneNotSupportedException ex) {
      // should not throw
      throw new RuntimeException(ex);
    }
  }

  static String formatHex(final byte[] bs) {
    return HEX_FORMAT.formatHex(bs);
  }

  static byte[] digest(final Path file, final MessageDigest md) throws IOException {
    assert !Files.isDirectory(file) && Files.isReadable(file);
    General.LOGGER.log(Logger.Level.DEBUG, "Calculating file digest");
    try (
        final Arena arena = Arena.openConfined();
        final FileChannel fc = FileChannel.open(file, StandardOpenOption.READ)
    ) {
      final MemorySegment mappedSegment = fc.map(FileChannel.MapMode.READ_ONLY, 0L, fc.size(), arena.scope());
      long remaining = fc.size();
      long position = 0L;
      while (remaining != 0L) {
        final long blockSize = Math.min(remaining, Integer.MAX_VALUE - 8); // dumb implementation detail
        md.update(mappedSegment.asSlice(position, blockSize).asByteBuffer());
        remaining -= blockSize;
        position += blockSize;
      }
    }

    return md.digest();
  }

  final MessageDigest md;
  private final Path digestFile;
  private final Map<Path, byte[]> fileDigestMap;

  FileDigestStorage(final Path digestFile) {
    this.digestFile = digestFile;
    this.fileDigestMap = new HashMap<>();
    this.md = md();
  }

  byte[] getOrCalculateFileDigest(final Path file) throws IOException {
    byte[] digest = this.fileDigestMap.get(file);
    if (digest == null) {
      this.fileDigestMap.put(file, digest = digest(file, this.md));
    }

    return digest;
  }

  void remember(final Path file, final byte[] digest) {
    this.fileDigestMap.put(file, digest);
  }

  void load() throws IOException {
    this.fileDigestMap.clear();
    try (final Stream<String> s = Files.lines(this.digestFile)) {
      this.fileDigestMap.putAll(
          s.map(line -> {
            final int htabIdx = line.indexOf('\t');
            final Path filePath = Path.of(URI.create(line.substring(0, htabIdx)));
            final byte[] digest = Base64.getDecoder().decode(line.substring(htabIdx + 1));
            return Map.entry(filePath, digest);
          }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
      );
    }
  }

  void save() throws IOException {
    try (final FileChannel fc = FileChannel.open(this.digestFile, StandardOpenOption.WRITE)) {
      final var out = new ByteArrayOutputStream();
      final var writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8));
      for (final Map.Entry<Path, byte[]> fileDigestEntry : this.fileDigestMap.entrySet()) {
        writer.write(fileDigestEntry.getKey().toUri().toASCIIString());
        writer.write('\t');
        writer.write(Base64.getEncoder().encodeToString(fileDigestEntry.getValue()));
        writer.write('\n');
        writer.flush();
        ChannelUtils.send(fc, ByteBuffer.wrap(out.toByteArray()));
        out.reset();
      }

      fc.truncate(fc.position());
    }
  }
}
