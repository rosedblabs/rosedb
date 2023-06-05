#!/bin/bash
make
redis-cli -h 0.0.0.0 -p 5200
