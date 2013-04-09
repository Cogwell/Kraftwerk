#!/bin/bash
# NOTE: this script must be run as root/sudo
rm *.bahn
./build_separate.sh
./build_structured.sh
#chown chembio:chembio * -R
