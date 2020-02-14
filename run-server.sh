#!/usr/bin/env bash

mvn exec:java -Dexec.mainClass="com.github.wongoo.hazelcast.HazelcastServer" -Dexec.args="$1 $2"