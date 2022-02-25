#!/bin/bash

set -euf -o pipefail

is_linux       () { uname | grep -q 'Linux' ; }
is_osx         () { uname | grep -q 'Darwin' ; }
is_container   () { [[ -f /.dockerenv ]] ; }
echo2          () { echo "$*" 1>&2 ; }
echo2_ne       () { echo -ne "$*" 1>&2 ; }
echo2_red      () { echo2 "$(tput setaf 1)$*$(tput sgr 0)" ; }
echo2_green    () { echo2 "$(tput setaf 2)$*$(tput sgr 0)" ; }
echo2_yellow   () { echo2 "$(tput setaf 3)$*$(tput sgr 0)" ; }
echo2_yellow_ne() { echo2_ne "$(tput setaf 3)$*$(tput sgr 0)" ; }
echo2_blue     () { echo2 "$(tput setaf 4)$*$(tput sgr 0)" ; }
log_err        () { echo2_red "$prog: ERROR: $*" ; }
log_warn       () { echo2_yellow "$prog: WARNING: $*" ; }
log_info       () { echo2_blue "$prog: INFO: $*" ; }

verify_script_run_path()
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
}

verify_in_nbdb_dev_box()
{
  if ! is_container ; then
    log_err "not in nbdb dev box!"
    exit 1
  fi
  # assert nbdb dev box is up-to-date
  # cd .. || exit 1
  # if ! ./nbdb_dev_box.sh --is-dev-box-up-to-date ; then
  #   log_err "nbdb dev box does not use the latest sd_dev_bx! run 'git pull' under sdmain and restart nbdb dev box"
  #   cd - >/dev/null
  #   exit 1
  # fi
  # cd - >/dev/null || exit 1
}

verify_java_version_for_druid()
{
  if type -p java; then
    log_info "found java executable in PATH"
    _java=java
  elif [[ -n "$JAVA_HOME" ]] && [[ -x "$JAVA_HOME/bin/java" ]];  then
    log_info "found java executable in JAVA_HOME"
    _java="$JAVA_HOME/bin/java"
  else
    log_err "no java"
    exit 1
  fi

  version=$("$_java" -version 2>&1 | awk -F '"' '/version/ {print $2}')
  log_info "Found Java version $version."
  if [[ "$version" != "1.8"* ]]; then
    log_err "Druid requires java 1.8. Your version is $version"
    if is_linux; then
      log_info "Use 'update-alternatives --config java' or set JAVA_HOME"
    elif is_osx; then
      log_info "Available Java versions:"
      /usr/libexec/java_home -V
      log_info "Set JAVA_HOME and try again."
    else
      log_err "Unsupported platform"
    fi
    exit 1
  fi
}

# When running locally, all apps refer to kafka by its docker
# hostname, which is `kafka`.
# However, on MacOS, Druid runs natively, and hence it can't access
# kafka by the hostname, unless an entry is manually added in /etc/hosts
verify_kafka_reachability_on_mac()
{
  if is_osx; then
    # Temporarily allow errors to print a better error message
    set +e
    ip=$(ping -c 2 kafka | head -2 | tail -1 | awk '{print $4}' | sed 's/[(:)]//g')
    set -e
    if [[ "$ip" != "127.0.0.1" ]]; then
      log_err "kafka must resolve to 127.0.0.1 for Druid to be able to reach \
it. Add the following entry in /etc/hosts and try again: 127.0.0.1 kafka"
      exit 1
    fi
  fi
}

verify_python_conditions()
{
  if ! hash python3.7; then
    log_err "Please install/configure python3.7, its used by arc unit"
    exit 1
  fi

  # Lexicographic (greater than, less than) comparison used below
  python3_ver=$(python3 -V 2>&1 | sed 's/.* \([0-9]\).\([0-9]\).*/\1\2/')
  if [ "$python3_ver" \< "37" ]; then
    log_err "AnomalyDB requires python 3.7 or greater"
    exit 1
  fi

  pip_ver=$(python3 -m pip -V 2>&1 | sed 's/.* \([0-9]\)\([0-9]\).*/\1\2/')
  if [ "$pip_ver" \< "20" ]; then
    log_err "AnomalyDB requires pip >=20"
    exit 1
  fi
}

build_nbdb()
{
  log_info "Building nbdb ..."
  if ! ../build.sh ; then
    log_err "nbdb build failed!"
    exit 1
  fi
}

is_druid_up()
{
  sudo lsof -i:8888 | grep -q LISTEN
}

exit_handler()
{
  log_info "Stopping druid ..."
  pkill -f supervise
  sleep 5
}

start_druid()
{
  DRUID_VERSION="0.22.1"
  DRUID_PKG="apache-druid-$DRUID_VERSION"

  # Check if druid needs to be downloaded
  if [ -f "./dist/druid/$DRUID_PKG-bin.tar.gz" ] ; then
    log_info "Druid has already been downloaded"
  else
    pushd .
    log_info "Downloading Druid ($DRUID_PKG) from apache.org ..."
    rm -rf dist/druid
    mkdir dist/druid
    cd dist/druid
    wget "https://dlcdn.apache.org/druid/$DRUID_VERSION/apache-druid-$DRUID_VERSION-bin.tar.gz"
    tar -zxf "$DRUID_PKG-bin.tar.gz"
    # mkdir -p "./dist/druid/$DRUID_PKG/dist/druid/lib"
    log_info "Installing druid-influxdb-emitter extension & its dependencies"
    java -cp "./$DRUID_PKG/lib/*" \
      -Ddruid.extensions.directory="./$DRUID_PKG/extensions" \
      org.apache.druid.cli.Main tools pull-deps -c \
      org.apache.druid.extensions.contrib:druid-influxdb-emitter:$DRUID_VERSION
    popd
  fi

  log_info "Cleaning up druid state ..."
  rm -rf "./dist/druid/$DRUID_PKG/var/"
  # log_info "Applying the druid local configuration ..."
  rm -rf "./dist/druid/$DRUID_PKG/conf/druid"
  cp -r "./deploy/druid_conf_local/conf" "./dist/druid/$DRUID_PKG/"

  # setup exit_handler to kill Druid upon script exit
  trap exit_handler EXIT

  log_info "Starting druid ..."
  pushd .
  cd "dist/druid/$DRUID_PKG"
  # if the default ports are in use (e.g. Grafana, td-agent), abort
  if ! bin/verify-default-ports ; then
    log_err "Stop services using Druid default ports and re-run systest!"

    # run netstat command via ssh to localhost, as we want the PID/Program name
    ssh localhost bash -c "\"if uname | grep -q 'Darwin' ; then \
sudo lsof -iTCP -sTCP:LISTEN -P ; elif uname | grep -q 'Linux' ; then \
sudo netstat -ntlp ; fi\""
    exit 1
  fi

  ./bin/supervise -c conf/supervise/quickstart.conf &

  popd
  local eta
  if is_linux ; then
    eta=20
  elif is_osx ; then
    eta=60
  fi
  for i in {0..90}; do
    if is_druid_up ; then
      echo2 ""  # newline after progress line
      echo2_green "Druid is UP!"
      return
    fi
    echo2_yellow_ne "  Waiting for Druid ... ($i / ${eta} sec)\r"
    sleep 1
  done
  echo2 ""  # newline after progress line
  log_err "Druid failed to start! examine Druid logs."
  exit 1
}

################################################################################
# main
################################################################################

prog=$(basename "$0")
workdir=$(pwd)

verify_script_run_path
verify_in_nbdb_dev_box
verify_java_version_for_druid
verify_kafka_reachability_on_mac
verify_python_conditions
build_nbdb
start_druid

log_info "Starting systest ..."
if ! time python3.7 nbdb/tests/anomalydb_systest.py $@ ; then
  log_err "Systest FAILED!"
  exit 1
fi
# systest passed - mark this commit hash as safe to deploy (NOTE: if there are
# local changes or the branch is not synced to HEAD - deployment safety checks
# would fail upon attempt to deploy).
commit_hash=$(git rev-parse HEAD)
echo2_green "Systest PASSED! [HEAD: $commit_hash]"
echo "$commit_hash" > .deployment_safety_systest_passed
