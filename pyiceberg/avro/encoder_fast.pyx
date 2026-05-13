# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import cython
from cpython.bytes cimport PyBytes_FromStringAndSize, PyBytes_AS_STRING, PyBytes_GET_SIZE
from cpython.mem cimport PyMem_Malloc, PyMem_Realloc, PyMem_Free
from libc.string cimport memcpy
from libc.stdint cimport uint8_t, uint64_t, int64_t

from pyiceberg.avro import STRUCT_DOUBLE, STRUCT_FLOAT

cdef uint64_t _INITIAL_CAPACITY = 1024


@cython.final
cdef class CythonBinaryEncoder:
    """In-memory BinaryEncoder that writes to a growable C buffer.

    Drop-in replacement for BinaryEncoder for the block-encoding path:
    exposes the same write_* methods the Writer tree calls, plus
    getvalue() to materialise the encoded bytes once at the end.
    """

    cdef unsigned char *_data
    cdef uint64_t _size
    cdef uint64_t _capacity

    def __cinit__(self):
        self._data = <unsigned char *> PyMem_Malloc(_INITIAL_CAPACITY)
        if not self._data:
            raise MemoryError()
        self._size = 0
        self._capacity = _INITIAL_CAPACITY

    def __dealloc__(self):
        PyMem_Free(self._data)

    cdef inline int _ensure(self, uint64_t n) except -1:
        cdef uint64_t need = self._size + n
        if need <= self._capacity:
            return 0
        cdef uint64_t cap = self._capacity
        while cap < need:
            cap <<= 1
        cdef unsigned char *grown = <unsigned char *> PyMem_Realloc(self._data, cap)
        if not grown:
            raise MemoryError()
        self._data = grown
        self._capacity = cap
        return 0

    cpdef bytes getvalue(self):
        return PyBytes_FromStringAndSize(<char *> self._data, self._size)

    cpdef void write(self, bytes b):
        cdef Py_ssize_t n = PyBytes_GET_SIZE(b)
        self._ensure(n)
        memcpy(self._data + self._size, PyBytes_AS_STRING(b), n)
        self._size += n

    cpdef void write_boolean(self, bint v):
        self._ensure(1)
        self._data[self._size] = 1 if v else 0
        self._size += 1

    cpdef void write_int(self, int64_t v):
        # zigzag then base-128 varint; a 64-bit value needs at most 10 bytes
        self._ensure(10)
        cdef uint64_t uv = <uint64_t> v
        cdef uint64_t datum = (uv << 1) ^ (0 - (uv >> 63))
        cdef unsigned char *p = self._data + self._size
        while datum & <uint64_t> ~0x7F:
            p[0] = <uint8_t> ((datum & 0x7F) | 0x80)
            p += 1
            datum >>= 7
        p[0] = <uint8_t> datum
        p += 1
        self._size = p - self._data

    def write_float(self, v: float) -> None:
        self.write(STRUCT_FLOAT.pack(v))

    def write_double(self, v: float) -> None:
        self.write(STRUCT_DOUBLE.pack(v))

    cpdef void write_bytes(self, b):
        cdef bytes bb = bytes(b) if type(b) is not bytes else b
        cdef Py_ssize_t n = PyBytes_GET_SIZE(bb)
        self.write_int(n)
        self._ensure(n)
        memcpy(self._data + self._size, PyBytes_AS_STRING(bb), n)
        self._size += n

    def write_utf8(self, s) -> None:
        self.write_bytes(s.encode("utf-8"))

    def write_uuid(self, uuid) -> None:
        cdef bytes b = uuid.bytes
        if PyBytes_GET_SIZE(b) != 16:
            raise ValueError(f"Expected UUID to have 16 bytes, got: len({b!r})")
        self._ensure(16)
        memcpy(self._data + self._size, PyBytes_AS_STRING(b), 16)
        self._size += 16

    def write_unknown(self, _) -> None:
        pass
