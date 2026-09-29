/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.iceberg.Files;
import org.apache.iceberg.encryption.AesGcmOutputFile;
import org.apache.iceberg.io.PositionOutputStream;

/** Writes the AGS1 fixtures in this directory with Java's AesGcmOutputStream. See README.md. */
public class GenerateAgs1Fixtures {
  static final int PLAIN_BLOCK_SIZE = 1024 * 1024;
  static final byte[] KEY = new byte[16];
  static final byte[] AAD_PREFIX = "pyiceberg-ags1".getBytes(StandardCharsets.UTF_8);

  static {
    for (int i = 0; i < KEY.length; i++) {
      KEY[i] = (byte) i;
    }
  }

  /** Byte i is i % 251. The prime period shifts phase across every 1 MiB block boundary. */
  static byte[] plaintext(int length) {
    byte[] out = new byte[length];
    for (int i = 0; i < length; i++) {
      out[i] = (byte) (i % 251);
    }
    return out;
  }

  static void write(File dir, String name, int length, byte[] aadPrefix) throws IOException {
    File target = new File(dir, name);
    if (target.exists() && !target.delete()) {
      throw new IOException("Could not delete " + target);
    }

    AesGcmOutputFile encrypted = new AesGcmOutputFile(Files.localOutput(target), KEY, aadPrefix);
    try (PositionOutputStream stream = encrypted.create()) {
      stream.write(plaintext(length));
    }

    System.out.printf("%-26s plaintext=%-8d encrypted=%d%n", name, length, target.length());
  }

  public static void main(String[] args) throws IOException {
    File dir = new File(args.length > 0 ? args[0] : ".");
    write(dir, "empty.ags1", 0, AAD_PREFIX);
    write(dir, "partial-block.ags1", 100, AAD_PREFIX);
    write(dir, "partial-block-no-aad.ags1", 100, null);
    write(dir, "aligned-multi-block.ags1", 2 * PLAIN_BLOCK_SIZE, AAD_PREFIX);
  }
}
