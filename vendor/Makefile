# Makefile for generating vendor packages

.PHONY: all clean fb303 hive-metastore

all: fb303 hive-metastore

# FB303 Thrift client generation
fb303:
	rm -f /tmp/fb303.thrift
	rm -rf fb303
	curl -s https://raw.githubusercontent.com/apache/thrift/master/contrib/fb303/if/fb303.thrift > /tmp/fb303.thrift
	rm -rf /tmp/gen-py/
	thrift -gen py -o /tmp/ /tmp/fb303.thrift
	mv /tmp/gen-py/fb303 fb303

# Hive Metastore Thrift definition generation
hive-metastore:
	rm -rf /tmp/hive
	mkdir -p /tmp/hive/share/fb303/if/
	curl -s https://raw.githubusercontent.com/apache/thrift/master/contrib/fb303/if/fb303.thrift > /tmp/hive/share/fb303/if/fb303.thrift
	curl -s https://raw.githubusercontent.com/apache/hive/master/standalone-metastore/metastore-common/src/main/thrift/hive_metastore.thrift > /tmp/hive/hive_metastore.thrift
	thrift -gen py -o /tmp/hive /tmp/hive/hive_metastore.thrift
	rm -rf hive_metastore
	mv /tmp/hive/gen-py/hive_metastore hive_metastore

# Clean generated files
clean:
	rm -rf fb303
	rm -rf hive_metastore
	rm -rf /tmp/fb303.thrift
	rm -rf /tmp/gen-py/
	rm -rf /tmp/hive