#!/usr/bin/env bash

# (c) Copyright 2012 WibiData, Inc.
#
# See the NOTICE file distributed with this work for additional
# information regarding copyright ownership.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# ------------------------------------------------------------------------------

# The kiji script is the entry point to several tools for interacting with the
# kiji system.
#
# Tools are run as:
#
#     $ kiji <tool-name> [options]
#
# For full usage information, use:
#
#     $ kiji help
#
# Environment variables:
# - JAVA_HOME : path of the JDK/JRE installation. Required.
# - HADOOP_HOME : path of the Hadoop installation. Required.
# - HBASE_HOME : path of the HBase installation. Required.
# - KIJI_HOME : path of the Kiji installation. Required.
# - KIJI_MR_HOME : path of the Kiji MapReduce installation. Optional.
#
# - KIJI_CLASSPATH : extra classpath entries prepended to the actual classpath.
# - KIJI_HADOOP_DISTRO_VER : manually select the Hadoop distribution.
#       Either "hadoop1" or "hadoop2".
# - KIJI_JAVA_OPTS : extra options passed to the JVM.
# - JAVA_LIBRARY_PATH : extra paths of directories containing native libraries.
#
# - DEBUG : Run the script in debugging mode.
#     $ DEBUG=1 kiji ...
#
# - QUIET : silence the script log message.
#     $ QUIET=1 kiji ...
#
# Notes:
#  - If JAVA_HOME is defined and java is available on the PATH,
#    the java program available on the PATH takes precedence.
#
#  - If HADOOP_HOME is defined and hadoop is available on the PATH,
#    ${HADOOP_HOME}/bin/hadoop takes precedence.
#
#  - If HBASE_HOME is defined and hbase is available on the PATH,
#    ${HADOOP_HOME}/bin/hbase takes precedence.

# ------------------------------------------------------------------------------

set -o nounset   # Fail when referencing undefined variables
set -o errexit   # Script exits on the first error
set -o pipefail  # Pipeline status failure if any command fails
if [[ ! -z "${DEBUG:-}" ]]; then
  source=$(basename "${BASH_SOURCE}")
  PS4="# ${source}":'${LINENO}: '
  set -x
fi

# ------------------------------------------------------------------------------

function log() {
  if [[ -z "${QUIET:-}" ]]; then
    echo "$(date +"%F %T") $@" >&2
  fi
}

function info() {
  log "INFO $@"
}

function warn() {
  log "WARN $@"
}

function error() {
  log "ERROR $@"
}

# ------------------------------------------------------------------------------

# Canonicalize a path into an absolute, symlink free path.
#
# Portable implementation of the GNU coreutils "readlink -f path".
# The '-f' option of readlink does not exist on MacOS, for instance.
#
# Args:
#   param $1: path to canonicalize.
# Stdout:
#   Prints the canonicalized path on stdout.
function resolve_symlink() {
  local target_file=$1

  if [[ -z "${target_file}" ]]; then
    echo ""
    return 0
  fi

  cd "$(dirname "${target_file}")"
  target_file=$(basename "${target_file}")

  # Iterate down a (possible) chain of symlinks
  local count=0
  while [[ -L "${target_file}" ]]; do
    if [[ "${count}" -gt 1000 ]]; then
      # Just stop here, we've hit 1,000 recursive symlinks. (cycle?)
      break
    fi

    target_file=$(readlink "${target_file}")
    cd $(dirname "${target_file}")
    target_file=$(basename "${target_file}")
    count=$(( ${count} + 1 ))
  done

  # Compute the canonicalized name by finding the physical path
  # for the directory we're in and appending the target file.
  local phys_dir=$(pwd -P)
  echo "${phys_dir}/${target_file}"
}

# ------------------------------------------------------------------------------

kiji_bin_path="$0"
kiji_bin_path=$(resolve_symlink "${kiji_bin_path}")
kiji_bin=$(dirname "${kiji_bin_path}")
kiji_bin=$(cd "${kiji_bin}" && pwd -P)

if [[ -z "${KIJI_HOME:-}" ]]; then
  warn "KIJI_HOME environment variable is not set."
  KIJI_HOME=$(cd "${kiji_bin}/.."; pwd -P)
  if [[ ! -x "${KIJI_HOME}/bin/kiji" ]]; then
    error "Unable to infer KIJI_HOME."
    error "Please set the KIJI_HOME environment variable."
    exit 1
  fi
  warn "Using KIJI_HOME=${KIJI_HOME}"
fi

if [[ ! -f "${KIJI_HOME}/conf/kiji-schema.version" ]]; then
  error "Invalid KIJI_HOME=${KIJI_HOME}"
  error "Cannot find \${KIJI_HOME}/conf/kiji-schema.version"
  exit 1
fi
kiji_schema_version=$(cat "${KIJI_HOME}/conf/kiji-schema.version")

export BENTO_CHECKIN_SERVER=${BENTO_CHECKIN_SERVER:-"https://updates.kiji.org/api/1.0.0/"}

# If KIJI_MR_HOME is set, jars that are part of the kiji-mapreduce distribution
# will be added to the classpath.
KIJI_MR_HOME=${KIJI_MR_HOME:-""}

# Any user code you want to add to the kiji classpath may be done via this env var.
KIJI_CLASSPATH=${KIJI_CLASSPATH:-""}

# Any arguments you want to pass to kiji's java may be done via this env var.
KIJI_JAVA_OPTS=${KIJI_JAVA_OPTS:-""}

# This is a workaround for OS X Lion, where a bug in JRE 1.6
# creates a lot of 'SCDynamicStore' errors.
if [[ "$(uname)" = "Darwin" ]]; then
  KIJI_JAVA_OPTS="${KIJI_JAVA_OPTS} -Djava.security.krb5.realm= -Djava.security.krb5.kdc="
fi

# An existing set of directories to use for the java.library.path property
# should be set with JAVA_LIBRARY_PATH.
JAVA_LIBRARY_PATH=${JAVA_LIBRARY_PATH:-""}

# Either "hadoop1" or "hadoop2":
KIJI_HADOOP_DISTRO_VER=${KIJI_HADOOP_DISTRO_VER:-""}

# ------------------------------------------------------------------------------
# Locate the JVM.

if java_bin_path=$(which java); then
  : # No-op : java binary found.
elif [[ -z "${JAVA_HOME:-}" ]]; then
  error "Java binary 'java' not found and JAVA_HOME is not set."
  error "Please bring 'java' on your PATH and set JAVA_HOME accordingly."
  exit 1
else
  java_bin_path="${JAVA_HOME}/bin/java"
  if [[ ! -x "${java_bin_path}" ]]; then
    error "Java binary 'java' not found in ${java_bin_path}"
    error "Please bring 'java' on your PATH and set JAVA_HOME accordingly."
    exit 1
  fi
fi
java_bin_path=$(resolve_symlink "${java_bin_path}")

if [[ -z "${JAVA_HOME:-}" ]]; then
  warn "JAVA_HOME is not set."
  JAVA_HOME=$(dirname "$(dirname "${java_bin_path}")")
  JAVA_HOME=$(cd "${JAVA_HOME}"; pwd -P)
  if [[ -x "${JAVA_HOME}/bin/java" ]]; then
    warn "Using JAVA_HOME=${JAVA_HOME}"
  else
    error "Unable to infer JAVA_HOME."
    error "Please set the JAVA_HOME environment variable."
    exit 1
  fi
fi
JAVA_HOME=$(resolve_symlink "${JAVA_HOME}")

# JAVA_HOME is required for HBase CLI tool to function properly.
export JAVA_HOME

# ------------------------------------------------------------------------------
# Locate the Hadoop installation and the hadoop CLI tool.

if [[ ! -z "${HADOOP_HOME:-}" ]]; then
  HADOOP_HOME=$(cd "${HADOOP_HOME}"; pwd -P)
  hadoop_bin_path="${HADOOP_HOME}/bin/hadoop"
  if [[ ! -x "${hadoop_bin_path}" ]]; then
    error "Invalid HADOOP_HOME=${HADOOP_HOME}"
    error "Cannot find \${HADOOP_HOME}/bin/hadoop"
    error "Please verify and set the HADOOP_HOME environment variable."
    exit 1
  fi
else
  warn "HADOOP_HOME environment variable is not set."
  if hadoop_bin_path=$(which hadoop); then
    hadoop_bin_path=$(resolve_symlink "${hadoop_bin_path}")
    HADOOP_HOME=$(dirname "$(dirname "${hadoop_bin_path}")")
    if [[ -x "${HADOOP_HOME}/bin/hadoop" ]]; then
      warn "Using HADOOP_HOME=${HADOOP_HOME}"
    else
      error "Unable to infer HADOOP_HOME"
      error "Please set the HADOOP_HOME environment variable."
      exit 1
    fi
  else
    error "HADOOP_HOME is not set and the 'hadoop' tool is not on the PATH."
    error "Please set the HADOOP_HOME environment variable."
    exit 1
  fi
fi

# ------------------------------------------------------------------------------
# Locate the HBase installation and the HBase CLI tool.

if [[ ! -z "${HBASE_HOME:-}" ]]; then
  HBASE_HOME=$(cd "${HBASE_HOME}"; pwd -P)
  hbase_bin_path="${HBASE_HOME}/bin/hbase"
  if [[ ! -x "${hbase_bin_path}" ]]; then
    error "Invalid HBASE_HOME=${HBASE_HOME}"
    error "Cannot find \${HBASE_HOME}/bin/hbase"
    error "Please verify and set the HBASE_HOME environment variable."
    exit 1
  fi
else
  warn "HBASE_HOME environment variable is not set."
  if hbase_bin_path=$(which hbase); then
    hbase_bin_path=$(resolve_symlink "${hbase_bin_path}")
    HBASE_HOME=$(dirname "$(dirname "${hbase_bin_path}")")
    if [[ -x "${HBASE_HOME}/bin/hbase" ]]; then
      warn "Using HBASE_HOME=${HBASE_HOME}"
    else
      error "Unable to infer HBASE_HOME."
      error "Please set the HBASE_HOME environment variable."
      exit 1
    fi
  else
    error "HBASE_HOME is not set and the 'hbase' tool is not on the PATH."
    error "Please set the HBASE_HOME environment variable."
    exit 1
  fi
fi

# ------------------------------------------------------------------------------

# Detect and extract the current Hadoop version number. e.g. "Hadoop 2.x-..." -> "2"
# You can override this with ${KIJI_HADOOP_DISTRO_VER} (e.g. "hadoop1" or "hadoop2").
if [[ -z "${KIJI_HADOOP_DISTRO_VER}" ]]; then
  hadoop_major_version=$("${hadoop_bin_path}" version | head -1 | cut -c 8)
  if [[ -z "${hadoop_major_version}" ]]; then
    error "Unknown Hadoop version."
    error "Please set KIJI_HADOOP_DISTRO_VER manually to 'hadoop1' or 'hadoop2'."
    exit 1
  else
    # If the hadoop version is 0 (i.e. 0.20) then use the hadoop1 bridge
    if [[ ${hadoop_major_version} -eq 0 ]]; then
      hadoop_major_version=1
    fi
    KIJI_HADOOP_DISTRO_VER="hadoop${hadoop_major_version}"
  fi
fi

# ------------------------------------------------------------------------------
# Compute the Kiji base classpaths:

# Kiji base classpath.
# When running as part of a Bento distribution, this contains most Kiji
# components (including KijiSchema, KijiMR, KijiScoring and their dependencies).
kijis_cp="${KIJI_HOME}/lib/*"

# We may have Hadoop distribution-specific jars to load in
# ${KIJI_HOME}/lib/distribution/hadoop${N}, where N is the major digit of the
# Hadoop version. Only load at most one such set of jars.
#
# ${KIJI_HOME}/lib/distribution/hadoop${N} is part of the KijiBento distribution.

if [[ -d "${KIJI_HOME}/lib/distribution/${KIJI_HADOOP_DISTRO_VER}" ]]; then
  kijis_cp="${kijis_cp}:${KIJI_HOME}/lib/distribution/${KIJI_HADOOP_DISTRO_VER}/*"
fi

# Add KijiMR classpath, if bundled separately ie. if KIJI_HOME != KIJI_MR_HOME:
kijimr_cp=""
if [[ ! -z "${KIJI_MR_HOME}" && "${KIJI_HOME}" != "${KIJI_MR_HOME}" ]]; then
  kijimr_cp="${KIJI_MR_HOME}/lib/*"
  if [[ -d "${KIJI_MR_HOME}/lib/distribution/${KIJI_HADOOP_DISTRO_VER}" ]]; then
    kijimr_cp="${kijimr_cp}:${KIJI_MR_HOME}/lib/distribution/${KIJI_HADOOP_DISTRO_VER}/*"
  fi
fi

# ------------------------------------------------------------------------------
# Retrieve HBase and Hadoop classpaths

hbase_cp=$("${hbase_bin_path}" classpath)
if [[ $? -ne 0 ]]; then
  error "Error retrieving HBase classpath."
  exit 1
fi

hadoop_cp=$("${hadoop_bin_path}" classpath)
if [[ $? -ne 0 ]]; then
  error "Error retrieving Hadoop classpath."
  exit 1
fi

# ------------------------------------------------------------------------------
# Compute the Kiji classpath into ${kiji_cp}

# The KijiSchema jar needs to go at the head of the classpath to suppress slf4j
# warnings.  The KijiSchema jar filename will be either a "normal" KijiSchema
# jar, or one compiled with profiling enabled, denoted by ...-profiling.jar.
# Select the appropriate jar filename here:
if [[ -f "${KIJI_HOME}/lib/kiji-schema-${kiji_schema_version}-profiling.jar" ]]; then
  info "Using profiling JARs"
  schema_jar="${KIJI_HOME}/lib/kiji-schema-${kiji_schema_version}-profiling.jar"
else
  schema_jar="${KIJI_HOME}/lib/kiji-schema-${kiji_schema_version}.jar"
fi

# Note that we put the Kiji lib jars before the hbase jars,
# in case there are conflicts.
kiji_conf="${KIJI_HOME}/conf"

# By decreasing priority:
kiji_cp="${schema_jar}"  # KijiSchema (profiling or normal JAR)
kiji_cp="${kiji_cp}:${KIJI_CLASSPATH}"  # KIJI_CLASSPATH overrides
kiji_cp="${kiji_cp}:${kiji_conf}"
kiji_cp="${kiji_cp}:${kijimr_cp}"
kiji_cp="${kiji_cp}:${kijis_cp}"

# SCHEMA-860: Hadoop jars must appear before HBase jars or else you will see errors of the
# following form:
# Exception in thread "main" java.io.IOException: Cannot initialize Cluster. Please check your
# configuration for mapreduce.framework.name and the correspond server addresses.
kiji_cp="${kiji_cp}:${hadoop_cp}"
kiji_cp="${kiji_cp}:${hbase_cp}"

# ------------------------------------------------------------------------------
# Determine location of Hadoop native libraries and set java.library.path

if [[ ! -z "${HADOOP_HOME}" && -d "${HADOOP_HOME}/lib/native" ]]; then
  java_platform=$("${java_bin_path}" \
    -classpath "${hadoop_cp}" \
    -Xmx32m \
    org.apache.hadoop.util.PlatformName \
    | sed -e "s/ /_/g")

  if [[ -d "${HADOOP_HOME}/lib/native/${java_platform}" ]]; then
    # Platform-specific native libraries directory exists:
    JAVA_LIBRARY_PATH="${JAVA_LIBRARY_PATH}:${HADOOP_HOME}/lib/native/${java_platform}"
  fi
  if [[ -d "${HADOOP_HOME}/lib/native" ]]; then
    # Global native libraries directory exists:
    JAVA_LIBRARY_PATH="${JAVA_LIBRARY_PATH}:${HADOOP_HOME}/lib/native/"
  fi
fi

# ------------------------------------------------------------------------------

# Before running the command, update the "last used" timestamp to use with the
# upgrade check-in server, if we are running inside a Kiji BentoBox.  We know
# we're running in a Kiji BentoBox if the timestamp generation script exists.
usage_timestamp_script="${kiji_bin}/record-usage-timestamp.sh"
if [[ -f "${usage_timestamp_script}" && -x "${usage_timestamp_script}" ]]; then
  if ! "${usage_timestamp_script}"; then
    : # Ignore error
  fi
fi

# If we're running in a Kiji BentoBox, inform the user of any BentoBox upgrades
# that are available.
upgrade_informer_script="${kiji_bin}/upgrade-informer.sh"
if [[ -f "${upgrade_informer_script}" && -x "${upgrade_informer_script}" ]]; then
  if ! "${upgrade_informer_script}"; then
    : # Ignore error
  fi
fi

# ------------------------------------------------------------------------------

# Checks the consistency of the environment and report suspect parameters.
function check_environment() {
  code=0

  log "Checking JAVA environment:"
  info "JAVA_HOME=${JAVA_HOME}"
  info "java_bin_path=${java_bin_path}"

  log "Output of ${java_bin_path} -version"
  echo "----------"
  "${java_bin_path}" -version
  echo "----------"

  local which_hadoop
  if which_java=$(which java); then
    if [[ "$(resolve_symlink "${which_java}")" != "${JAVA_HOME}/bin/java" ]]; then
      error "Inconsistent Java environment:"
      error "java binary found on the PATH is ${which_java}"
      error "instead of ${JAVA_HOME}/bin/java"
      error "Please verify and set JAVA_HOME and PATH appropriately."
      code=1
    fi
  fi

  log "Checking Hadoop environment:"
  info "HADOOP_HOME=${HADOOP_HOME}"
  info "hadoop_bin_path=${hadoop_bin_path}"

  log "Output of ${hadoop_bin_path} version"
  echo "----------"
  "${hadoop_bin_path}" version
  echo "----------"

  local which_hadoop
  if which_hadoop=$(which hadoop); then
    if [[ "$(resolve_symlink "${which_hadoop}")" != "${hadoop_bin_path}" ]]; then
      error "Inconsistent Hadoop environment:"
      error "hadoop tool found on the path in ${which_hadoop}"
      error "instead of ${HADOOP_HOME}/bin/hadoop"
      error "Please verify and set HADOOP_HOME and PATH appropriately."
    fi
  fi

  log "Checking HBase environment:"
  info "HBASE_HOME=${HBASE_HOME}"
  info "hbase_bin_path=${hbase_bin_path}"

  log "Output of ${hbase_bin_path} version"
  echo "----------"
  "${hbase_bin_path}" version
  echo "----------"

  local which_hbase
  if which_hbase=$(which hbase); then
    if [[ "$(resolve_symlink "${which_hbase}")" != "${hbase_bin_path}" ]]; then
      error "Inconsistent HBase environment:"
      error "hbase tool found on the PATH is ${which_hbase}"
      error "instead of ${HBASE_HOME}/bin/hbase"
      error "Please verify and set HBASE_HOME and PATH appropriately."
    fi
  fi

  log "Checking Kiji environment:"
  info "KIJI_HOME=${KIJI_HOME}"
  info "KIJI_MR_HOME=${KIJI_MR_HOME}"
  info "KIJI_CLASSPATH=${KIJI_CLASSPATH}"
  info "KIJI_HADOOP_DISTRO_VER=${KIJI_HADOOP_DISTRO_VER}"
  info "KIJI_JAVA_OPTS=${KIJI_JAVA_OPTS}"
  info "JAVA_LIBRARY_PATH=${JAVA_LIBRARY_PATH}"
  if [[ ! -x "${KIJI_HOME}/bin/kiji" ]]; then
    error "Inconsistent Kiji environment: "
    error "KIJI_HOME=${KIJI_HOME} but Kiji tool not found."
    error "${KIJI_HOME}/bin/kiji is missing or not executable."
    code=1
  fi
  if [[ "${kiji_bin_path}" != "${KIJI_HOME}/bin/kiji" ]]; then
    error "Inconsistent Kiji environment: "
    error "Kiji tool ${kiji_bin_path} is external to KIJI_HOME=${KIJI_HOME}"
    code=1
  fi

  exit ${code}
}

# ------------------------------------------------------------------------------

if [[ -z "${1:-}" ]]; then
  echo "kiji: Tool launcher for the Kiji framework"
  echo "Run 'kiji help' to see a list of available tools."
  exit 1
fi

command="$1"

case "${command}" in
  env-check)
    check_environment
    exit 0
    ;;

  classpath)
    echo "${kiji_cp}"
    exit 0
    ;;

  jar)
    shift  # pop off the command
    if [[ $# > 0 && $1 == "--help" ]]; then
      echo "Usage: kiji jar <jarFile> <mainClass> [args...]"
      echo
      exit 0
    fi
    user_jar_file="$1"
    class="$2"
    shift
    shift
    if [[ -z "${user_jar_file}" ]]; then
      echo "Error: no jar file specified."
      echo "Usage: kiji jar <jarFile> <mainClass> [args...]"
      exit 1
    fi
    if [[ ! -f "${user_jar_file}" ]]; then
      echo "Error: Cannot find jar file ${user_jar_file}"
      echo "Usage: kiji jar <jarFile> <mainClass> [args...]"
      exit 1
    fi
    if [[ -z "${class}" ]]; then
      echo "Error: no main class specified."
      echo "Usage: kiji jar <jarFile> <mainClass> [args...]"
      exit 1
    fi
    kiji_cp="${user_jar_file}:${kiji_cp}"
    ;;

  *)
    class="org.kiji.schema.tools.KijiToolLauncher"
    ;;
esac

if [[ -z "${class}" ]]; then
  echo "Unknown command: ${command}"
  echo "Try:"
  echo "  kiji help"
  exit 1
fi

# Do NOT quote ${KIJI_JAVA_OPTS} as we want to expand it into many arguments.
exec "${java_bin_path}" \
  -classpath "${kiji_cp}" \
  -Djava.library.path="${JAVA_LIBRARY_PATH}" \
  ${KIJI_JAVA_OPTS} \
  "${class}" "$@"
