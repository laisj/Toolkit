package com.xiaomi.contest.cvr.utils;

import java.math.BigInteger;
import java.nio.charset.Charset;

public class FeatureHash {
  public static Charset CharSetUTF8 = Charset.forName("UTF-8");

  public static long getUInt32(byte a, byte b, byte c, byte d) {
    return (d << 24 | (c & 0xFF) << 16 | (b & 0xFF) << 8 | (a & 0xFF));
  }

  // 64-bit hash for 32-bit platforms
  public static BigInteger MurmurHash64(byte data[], int len, long seed) {
    long m = 0x5bd1e995L;
    int r = 24;

    long h1 = (seed) ^ len;
    long h2 = (seed >> 32);

    int offset = 0;
    while (len >= 8) {
      long k1 = getUInt32(data[offset], data[offset + 1], data[offset + 2], data[offset + 3]);
      k1 *= m;
      k1 &= 0xFFFFFFFFL;
      k1 ^= k1 >> r;
      k1 *= m;
      k1 &= 0xFFFFFFFFL;
      h1 *= m;
      h1 &= 0xFFFFFFFFL;
      h1 ^= k1;

      long k2 = getUInt32(data[offset + 4], data[offset + 5], data[offset + 6], data[offset + 7]);
      k2 *= m;
      k2 &= 0xFFFFFFFFL;
      k2 ^= k2 >> r;
      k2 *= m;
      k2 &= 0xFFFFFFFFL;
      h2 *= m;
      h2 &= 0xFFFFFFFFL;
      h2 ^= k2;

      offset += 8;
      len -= 8;
    }

    if (len >= 4) {
      long k1 = getUInt32(data[offset], data[offset + 1], data[offset + 2], data[offset + 3]);
      k1 *= m;
      k1 &= 0xFFFFFFFFL;
      k1 ^= k1 >> r;
      k1 *= m;
      k1 &= 0xFFFFFFFFL;
      h1 *= m;
      h1 &= 0xFFFFFFFFL;
      h1 ^= k1;
      offset += 4;
      len -= 4;
    }

    switch (len) {
      case 3:
        h2 ^= (data[offset + 2] & 0xFF) << 16;
      case 2:
        h2 ^= (data[offset + 1] & 0xFF) << 8;
      case 1:
        h2 ^= (data[offset] & 0xFF);
        h2 *= m;
        h2 &= 0xFFFFFFFFL;
    }
    ;

    h1 ^= h2 >> 18;
    h1 *= m;
    h1 &= 0xFFFFFFFFL;
    h2 ^= h1 >> 22;
    h2 *= m;
    h2 &= 0xFFFFFFFFL;
    h1 ^= h2 >> 17;
    h1 *= m;
    h1 &= 0xFFFFFFFFL;
    h2 ^= h1 >> 19;
    h2 *= m;
    h2 &= 0xFFFFFFFFL;

    return BigInteger.valueOf(h1).shiftLeft(32).or(BigInteger.valueOf(h2));
  }

  public static String hash(String input) {
    byte[] tt = input.getBytes(CharSetUTF8);
    return MurmurHash64(tt, tt.length, 11L).toString();
  }

  public static Long hashToLong(String input) {
    byte[] tt = input.getBytes(CharSetUTF8);
    return MurmurHash64(tt, tt.length, 11L).longValue();
  }

  /**
   * the constant 2^64
   */
  private static final BigInteger TWO_64 = BigInteger.ONE.shiftLeft(64);

  public static String asUnsignedLongString(long l) {
    BigInteger b = BigInteger.valueOf(l);
    if (b.signum() < 0) {
      b = b.add(TWO_64);
    }
    return b.toString();
  }
}
