package ar.emily.ftp;

import java.text.CompactNumberFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;

final class General {

  static final System.Logger LOGGER = System.getLogger("ar.emily.ftp");
  private static final NumberFormat SIZE_FORMATTER;

  static {
    SIZE_FORMATTER =
        new CompactNumberFormat("", DecimalFormatSymbols.getInstance(), new String[]{
            "0 B", "00 B", "000 B",
            "0 kB", "00 kB", "000 kB",
            "0 MB", "00 MB", "000 MB",
            "0 GB", "00 GB", "000 GB",
            "0 TB", "00 TB", "000 TB",
        });
    SIZE_FORMATTER.setMinimumFractionDigits(2);
    SIZE_FORMATTER.setMaximumFractionDigits(2);
  }

  static void ensure(final boolean expr, final String msg) {
    if (!expr) {
      throw new IllegalArgumentException(msg);
    }
  }

  static NumberFormat sizeFormatter() {
    return (NumberFormat) SIZE_FORMATTER.clone();
  }

  private General() {
  }
}
