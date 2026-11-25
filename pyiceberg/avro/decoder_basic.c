/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
*/

#include <stdint.h>

/*
  Decode an an array of zig-zag encoded integers from a buffer.

  The buffer is advanced to the end of the integers.
  `end` is the first byte after the buffer.
  `count` is the number of integers to decode.
  `result` is where the decoded integers are stored.

  The result is guaranteed to be 64 bits wide.

*/
static inline int decode_zigzag_ints(
    const unsigned char **buffer, const unsigned char *end, const uint64_t count, uint64_t *result) {
  uint64_t current_index;
  const unsigned char *current_position = *buffer;
  uint64_t temp;
  unsigned char shift;
  unsigned char byte;

  for (current_index = 0; current_index < count; current_index++) {
    temp = 0;
    shift = 0;
    while (1) {
      if (current_position >= end || shift >= 64) {
        return 0;
      }

      byte = *current_position;
      current_position += 1;

      if (shift == 63 && (byte & 0x7E)) {
        return 0;
      }
      temp |= (uint64_t)(byte & 0x7F) << shift;

      if (!(byte & 0x80)) {
        break;
      }
      shift += 7;
    }
    result[current_index] = (temp >> 1) ^ (~(temp & 1) + 1);
  }
  *buffer = current_position;
  return 1;
}

/*
  Skip a zig-zag encoded integer in a buffer.

  The buffer is advanced to the end of the integer.
*/
static inline int skip_zigzag_int(const unsigned char **buffer, const unsigned char *end) {
  uint64_t ignored;
  return decode_zigzag_ints(buffer, end, 1, &ignored);
}
