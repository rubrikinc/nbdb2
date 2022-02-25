#!/bin/bash
################################################################################
#
# USAGE: build.sh
#
# This script builds nbdb, i.e. installs latest nbdb modules (setup.py install).
# It can be run from any path under the nbdb git repo, from nbdb dev box.
#
################################################################################
is_container() { [[ -f /.dockerenv ]] ; }
is_git      () { git rev-parse --show-toplevel 1>/dev/null 2>/dev/null; }
is_nbdb_git () { git config --get remote.origin.url | grep -q nbdb.git ; }
echo2       () { echo "$*" 1>&2 ; }
echo2_red   () { echo2 "$(tput setaf 1)$*$(tput sgr 0)" ; }
echo2_green () { echo2 "$(tput setaf 2)$*$(tput sgr 0)" ; }
echo2_yellow() { echo2 "$(tput setaf 3)$*$(tput sgr 0)" ; }
echo2_blue  () { echo2 "$(tput setaf 4)$*$(tput sgr 0)" ; }
log_err     () { echo2_red "$prog: ERROR: $*" ; }
log_warn    () { echo2_yellow "$prog: WARNING: $*" ; }
log_info    () { echo2_blue "$prog: INFO: $*" ; }
################################################################################
# main
################################################################################

prog=$(basename "$0")

# assert in nbdb dev box
if ! is_container ; then
  log_err "not in nbdb dev box!"
  exit 1
fi

# assert under nbdb git repo
# if ! is_git || ! is_nbdb_git ; then
#   log_err "not under nbdb git repo!"
#   exit 1
# fi

# top_level=$(git rev-parse --show-toplevel)
# cd "$top_level/python" || exit 1

if [ "$1" == "-v" ] ; then
  sudo python3.7 setup.py install
else
  sudo python3.7 setup.py install 1>/dev/null 2>/dev/null
fi
if [ $? -eq 0 ] ; then
  echo2_green "Build PASSED!"
else
  log_err "Build FAILED!"
fi

cd - >/dev/null || exit 1
