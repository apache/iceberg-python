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

FROM python:3.11-slim

LABEL org.opencontainers.image.source=https://github.com/apache/iceberg-python
LABEL org.opencontainers.image.description="Universal Iceberg REST Catalog Server"
LABEL org.opencontainers.image.licenses=Apache-2.0

WORKDIR /app

# Install REST server dependencies
RUN pip install --no-cache-dir \
    'fastapi>=0.104.0' \
    'uvicorn[standard]>=0.24.0' \
    'pydantic>=2.0.0' \
    pyyaml \
    pyiceberg

# Copy REST server code
COPY dev/rest-server/main.py .
COPY dev/rest-server/.pyiceberg.yaml /root/.pyiceberg.yaml

# Expose REST API port
EXPOSE 8000

# Set default environment variables
ENV ICEBERG_CATALOG_NAME=dynamodb-local
ENV SERVER_HOST=0.0.0.0
ENV SERVER_PORT=8000
ENV LOG_LEVEL=info

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD python -c "import requests; requests.get('http://localhost:8000/health').raise_for_status()"

# Run REST server
CMD ["python", "main.py"]
