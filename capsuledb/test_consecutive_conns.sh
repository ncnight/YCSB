#!/bin/bash

nc 128.32.37.40 4001 < test_write.txt
sleep 1
nc 128.32.37.40 4001 < test_read.txt