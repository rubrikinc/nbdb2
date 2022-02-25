#!/bin/bash
########################################################################################################################
#
# nbdb_dev_box.sh
#
# USAGE: ./nbdb_dev_box.sh [OPTIONS] (for details, see output of -h|--help)
#
# this script is a wrapper for sd_dev_box. It customizes sd_dev_box docker
# image via Dockerfile.append file. It also hacks the local sd_dev_box python
# code to run docker-compose with parameters that are needed for nbdb:
#  1. docker socket to allow docker-in-docker
#  2. AWS/ECS credential files needed by nbdb deploy scripts
#
########################################################################################################################
is_linux    () { uname | grep -q 'Linux' ; }
is_osx      () { uname | grep -q 'Darwin' ; }
is_container() { [[ -f /.dockerenv ]] ; }
echo2       () { echo "$*" 1>&2; }
echo2_red   () { echo2 "$(tput setaf 1)$*$(tput sgr 0)" ; }
echo2_green () { echo2 "$(tput setaf 2)$*$(tput sgr 0)" ; }
echo2_yellow() { echo2 "$(tput setaf 3)$*$(tput sgr 0)" ; }
echo2_blue  () { echo2 "$(tput setaf 4)$*$(tput sgr 0)" ; }
log_err     () { echo2_red "$prog: ERROR: $*" ; }
log_warn    () { echo2_yellow "$prog: WARNING: $*" ; }
log_info    () { echo2_blue "$prog: INFO: $*" ; }
tab_name    () { echo -en "\033]0;$*\a" ; }
is_number   () { [ "$1" -eq "$1" ] 2> /dev/null ; }

########################################################################################################################
set_tab_name()
{
  if is_osx ; then
    tab_name "$1<$2>"
  else # most likely a remote Dev VM, so wrap with [] to indicate a remote host
    tab_name "$1[<$2>]"
  fi
}
########################################################################################################################
perform_sanity_checks()
{
  # readlink is used in order retrieve the absolute path of this script
  local prog_abspath
  if is_linux ; then
    prog_abspath="$(readlink -f $0)"
  elif is_osx ; then
    if ! which greadlink > /dev/null ; then
      if ! which brew > /dev/null ; then
        # install homebrew for Mac, then coreutils (for readlink)
        log_info "install homebrew for Mac ..."
        bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install.sh)"
      fi
      log_info "install brew coreutils (GNU readlink is required) ..."
      if ! brew install coreutils ; then
        log_err "failed to brew install coreutils, required for GNU readlink"
        exit 1
      fi
    fi
    prog_abspath="$(greadlink -f $0)"
  else
    log_err "unsupported platform - only Linux and MacOS are currently supported"
    exit 1
  fi

  if [ "$prog_abspath" != "$workdir/$prog" ] ; then
    log_err "this script must be run from its parent directory, i.e. ./$prog"
    exit 1
  fi
  if is_container ; then
    log_err "already in container!"
    exit 1
  fi
}
########################################################################################################################

########################################################################################################################
# main
########################################################################################################################

prog=$(basename "$0")
workdir=$(pwd)
nbdb_dirname=$(basename "$workdir")

# within the dev box, use sudo docker
DOCKER="docker"
if is_container ; then
  DOCKER="sudo docker"
fi

# if docker ps command fails - abort
if ! $DOCKER ps 2>/dev/null 1>/tmp/dockerps ; then
  log_err "docker ps command failed! aborting."
  exit 1
fi

# perform env sanity checks before starting or connecting-to an nbdb dev box
perform_sanity_checks

# set terminal tab name before entering nbdb dev box
set_tab_name "" "$nbdb_dirname"

cd ./dev_box && sudo docker-compose build nbdb-box && cd ..

cmd="cd && sudo updatedb && sudo chown -R ubuntu:ubuntu . && cd - && \
  cd $nbdb_dirname/python && sudo python3.7 setup.py install && cd .. && \
  bash"

cd ./dev_box && sudo docker-compose run --rm nbdb-box /bin/bash -i -c "$cmd"

ret=$?
log_info "nbdb_dev_box has exited ($ret)"

# set terminal tab name after exiting nbdb dev box
set_tab_name "BYE " "$nbdb_dirname"