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

import vortex as vx
import tempfile
import os

# Create a test Vortex file
with tempfile.NamedTemporaryFile(delete=False, suffix='.vortex') as f:
    temp_path = f.name

try:
    # Write some test data
    print("Creating test Vortex file...")
    vx.io.write(vx.array([1, 2, 3]), temp_path)
    
    # Open the file
    print("Opening Vortex file...")
    vf = vx.open(temp_path)
    
    print(f"VortexFile type: {type(vf)}")
    print(f"VortexFile methods: {[m for m in dir(vf) if not m.startswith('_')]}")
    
    # Try to see what methods actually work
    if hasattr(vf, 'to_arrow'):
        print("Has to_arrow method")
    if hasattr(vf, 'to_arrow_table'):
        print("Has to_arrow_table method") 
    if hasattr(vf, 'read'):
        print("Has read method")
    if hasattr(vf, 'to_table'):
        print("Has to_table method")
        
finally:
    # Clean up
    if os.path.exists(temp_path):
        os.unlink(temp_path)
