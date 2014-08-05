#!/usr/bin/env python3
# -*- mode: python -*-
# -*- coding: utf-8 -*-

# --------------------------------------------------------------------------------------------------

# (c) Copyright 2014 WibiData, Inc.
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

# --------------------------------------------------------------------------------------------------

# The kiji script provides supporting tools to run Kiji commands.
# For full usage information, run:
#
#   ${KIJI_HOME}/bin/kiji.py --help


import argparse
import glob
import hashlib
import itertools
import logging
import os
import re
import subprocess
import sys
import time
import zipfile


KIJI_TOOL_LAUNCHER = "org.kiji.schema.tools.KijiToolLauncher"

HADOOP_HOME = "HADOOP_HOME"
HBASE_HOME = "HBASE_HOME"
KIJI_HOME = "KIJI_HOME"
SCHEMA_SHELL_HOME = "SCHEMA_SHELL_HOME"

KIJI_CLASSPATH = "KIJI_CLASSPATH"


class Error(Exception):
  """Errors used in this module."""
  pass


# --------------------------------------------------------------------------------------------------
# Utilities


def md5_sum(file_path):
    """Computes the MD5 sum of a file.

    Args:
      file_path: Path of the file to compute the MD5 sum for.
    Returns:
      The file MD5 sum, represented as an hex string (32 characters).
    """
    md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        md5.update(f.read())
    return md5.hexdigest()


def expand_classpath_entry(entry):
    """Expand the specified classpath entry if it contains a wildcard.

    Expand the '*' classpath wildcard by applying the glob '*.jar'.

    Yields:
      Expanded classpath entries.
    """
    if os.path.basename(entry) != "*":
        yield entry
    else:
        expanded = glob.glob(os.path.join(os.path.dirname(entry), "*.jar"))
        expanded = sorted(expanded)
        yield from expanded


def flat_map(operator, iterable):
    """Concatenates the collections produced by an operator mapped on a given collection.

    Args:
      operator: Operator to apply on each input element from iterable.
          The expected signature for operator is: element -> iterable.
      iterable: Iterable of elements to apply the operator onto.
    Returns:
      An iterable of the concatenation of the resulting collections.
    """
    return itertools.chain.from_iterable(map(operator, iterable))


def unique(iterable, key=None):
    """Removes duplicate items from the specified iterable.

    Args:
      iterable: Collection of items to filter duplicates from.
      key: Optional function with a signature: item -> item_key.
          Specifying a custom key allows to identify duplicates through some indirect attributes
          of the items.
    Returns:
      Original iterable with duplicate items removed.
    """
    watched = set()
    if key is None:
        key = lambda x: x  # identity

    def watch(item):
        """Stateful filter that remembers items previously watched."""
        item_key = key(item)
        if item_key in watched:
            logging.debug("Filter %r out (key = %r)", item, item_key)
            return False
        else:
            watched.add(item_key)
            return True

    return filter(watch, iterable)


def tab_indent(text):
    """Left-indents a string of text."""
    return "\t" + text


def _exists_or_log(entry):
    exist = os.path.exists(entry)
    if not exist:
        logging.debug("Classpath entry does not exist: %r", entry)
    return exist


_RE_MANIFEST_CONT = re.compile(r"\r\n\s+")


def list_jar_manifest_entries(jar_path):
    """Lists the entries from the manifest of the specified JAR file.

    Args:
      jar_path: Path of the JAR file to report the manifest entries from.
    Yields:
      (key, value) pairs of the manifest entries.
    """
    if not os.path.isfile(jar_path):
        return None
    try:
        with zipfile.ZipFile(jar_path) as zfile:
            try:
                manifest_bytes = zfile.read("META-INF/MANIFEST.MF")
            except KeyError:
                logging.debug("JAR file has no manifest: %r", jar_path)
                return
            manifest_text = manifest_bytes.decode()
            manifest_text = _RE_MANIFEST_CONT.sub("", manifest_text)
            for entry in manifest_text.replace("\n ", "").splitlines():
                entry = entry.strip()
                if len(entry) == 0:
                    continue
                (key, value) = entry.split(":", 1)
                key = key.strip()
                value = value.strip()
                logging.debug("Manifest for %r entry: %r = %r", jar_path, key, value)
                yield (key, value)

    except zipfile.BadZipFile:
        logging.error('%r is not a valid JAR file.', jar_path)


def get_jar_bundle_version(jar_path):
    if not os.path.isfile(jar_path):
        return None
    manifest = dict(list_jar_manifest_entries(jar_path))
    bundle_name = manifest.get("Bundle-Name")
    if bundle_name is None:
        logging.debug("No bundle name defined in %r", jar_path)
        return None
    bundle_version = manifest.get("Bundle-Version")
    if bundle_version is None:
        logging.debug("No version for bundle %r defined in %r", bundle_name, jar_path)
        return None
    return (bundle_name, bundle_version)



_RE_META_INF_MAVEN_POM = re.compile(r"^META-INF/maven/(.*)/(.*)/pom.xml$")


def get_jar_maven_artifact(jar_path):
    """Reports the Maven artifacts the specified JAR file contains.

    Args:
      jar_path: Path of the JAR file to report the Maven artifact of.
    Yields:
      Maven artifact the specified JAR file contains.
    """
    if not os.path.isfile(jar_path):
        return None
    try:
        with zipfile.ZipFile(jar_path) as zfile:
            poms = list()
            for zip_file_path in zfile.namelist():
                match = _RE_META_INF_MAVEN_POM.match(zip_file_path)
                if match is not None:
                    yield (match.group(1), match.group(2))

    except zipfile.BadZipFile:
        logging.error('%r is not a valid JAR file.', jar_path)


def normalize_classpath(classpath):
    """Normalizes the given classpath entries.

    Performs the following normalizations:
     - Classpath wildcards are expanded.
     - Symlinks are expanded.
     - Paths are made absolute.
     - Duplicate paths are eliminated.
     - Non-existent paths are removed.

    Args:
      classpath: Iterable of classpath entries.
    Returns:
      Iterable of normalized classpath entries.
    """
    classpath = flat_map(expand_classpath_entry, classpath)
    classpath = filter(_exists_or_log, classpath)
    classpath = map(os.path.realpath, classpath)
    classpath = unique(classpath)

    # Filtering classpath entry duplicates based on file basename is very unreliable.
    # classpath = unique(classpath, key=os.path.basename)

    # Filter out JAR files whose MD5 is known already:
    def md5_or_path(path):
        if os.path.isfile(path):
            return 'md5:%s' % md5_sum(path)
        else:
            return 'path:%s' % path

    classpath = unique(classpath, key=md5_or_path)

    # Filter JAR files based on the Bundle-Name declared in the MANIFEST.MF file:
    if False:
        def jar_bundle_name(path):
            bundle_name_version = get_jar_bundle_version(path)
            if bundle_name_version is None:
                return "path:%s" % path
            else:
                (bundle_name, bundle_version) = bundle_name_version
                logging.debug("Processing classpath entry for JAR Bundle: %r = %r (from %r)",
                              bundle_name, bundle_version, path)
                return "bundle:%s" % bundle_name

        classpath = unique(classpath, key=jar_bundle_name)

    # Filter JAR files based on the Maven artifact name,
    # parsed from the META-INF/maven/group-id/artifact-id/pom.xml files:
    # Note: this is very imperfect.
    # We could probably parse the pom.xml file content directly.
    if False:
        def maven_artifact(path):
            artifacts = list(get_jar_maven_artifact(path))
            if len(artifacts) == 0:
                return "path:%s" % path
            elif len(artifacts) == 1:
                return "artifact:%s:%s" % artifacts[0]
            else:
                return "artifacts:%r" % artifacts

        classpath = unique(classpath, key=maven_artifact)

    return classpath


# --------------------------------------------------------------------------------------------------


class HomedTool(object):
    """Wraps an installation configured through a X_HOME environment variable.

    This assumes the installation provides a tool under "${X_HOME}/bin/<tool>".
    """

    def __init__(self, env=os.environ):
        self._env = env
        assert (self.home_env_key in self._env), \
            ("Environment variable undefined: %r" % self.home_env_key)
        self._home_dir = os.path.abspath(self._env[self.home_env_key])
        assert os.path.isdir(self.home_dir), ("Home directory not found: %r" % self.home_dir)

    @property
    def home_dir(self):
        return self._home_dir

    @property
    def tool_path(self):
        tool_path = os.path.join(self.home_dir, "bin", self.tool)
        assert os.path.isfile(tool_path), ("Command-line tool not found: %r" % tool_path)
        return tool_path

    def _acquire_classpath(self):
        stdout = subprocess.check_output([self.tool_path, "classpath"], universal_newlines=True)
        stdout = stdout.strip()
        classpath = stdout.split(":")
        classpath = filter(None, classpath)
        classpath = tuple(classpath)
        logging.debug("%r reported the following classpath:\n%s",
                      self.tool_path, "\n".join(map(tab_indent, classpath)))
        return classpath

    @property
    def classpath(self):
        """Reports the runtime classpath for this homed tool installation.

        Returns:
          A tuple of classpath entries for this installation.
        """
        if not hasattr(self, "_classpath"):
            self._classpath = tuple(normalize_classpath(self._acquire_classpath()))
        return self._classpath


# --------------------------------------------------------------------------------------------------


class HadoopTool(HomedTool):
    _RE_HADOOP_VERSION = re.compile(r"^Hadoop (.*)$")

    @property
    def home_env_key(self):
        return "HADOOP_HOME"

    @property
    def tool(self):
        return "hadoop"

    def _acquire_version(self):
        stdout = subprocess.check_output([self.tool_path, "version"], universal_newlines=True)
        stdout = stdout.strip()
        lines = stdout.splitlines()
        top_line = lines[0]
        match = self._RE_HADOOP_VERSION.match(top_line)
        assert (match is not None), ("Invalid output from command 'hadoop version': %r" % stdout)
        return match.group(1)

    @property
    def version(self):
        """Returns: the version ID of this Hadoop installation (eg. '2.0.0-mr1-cdh4.3.0')."""
        if not hasattr(self, "_version"):
            self._version = self._acquire_version()
        return self._version

    @property
    def major_version(self):
        """Returns: the major version of this Hadoop installation (eg. 1 or 2)."""
        return self.version.split(".")[0]  # Pick major version

    def _acquire_platform_name(self):
        cmd = [
            "java",
            "-cp",
            ":".join(self.classpath),
            "-Xmx32m",
            "org.apache.hadoop.util.PlatformName"
        ]
        output = subprocess.check_output(cmd, universal_newlines=True)
        java_platform = output.strip()
        logging.info("Using Hadoop platform: %r", java_platform)
        return java_platform

    @property
    def platform_name(self):
        """Returns: the Hadoop platform name."""
        if not hasattr(self, "_platform_name"):
            self._platform_name = self._acquire_platform_name()
        return self._platform_name

    def list_native_lib_paths(self):
        """Lists the paths of the Hadoop native libraries to specify in "java.library.path".

        Native libraries are expected under $HADOOP_HOME/lib/native
        and under $HADOOP_HOME/lib/native/${platform}.

        Returns:
          An iterable of directory paths to the Hadoop native libraries.
        """
        lib_paths = list()

        native_dir_path = os.path.join(self.home_dir, "lib", "native")
        if os.path.isdir(native_dir_path):
            lib_paths.append(native_dir_path)

            # Hadoop wants a certain platform version, then we hope to use it
            native_dirs = os.path.join(native_dir_path, self.platform_name.replace(" ", "_"))
            if os.path.isdir(native_dirs):
                lib_paths.append(native_dirs)

        return lib_paths



# --------------------------------------------------------------------------------------------------


class HBaseTool(HomedTool):
    @property
    def home_env_key(self):
        return "HBASE_HOME"

    @property
    def tool(self):
        return "hbase"


# --------------------------------------------------------------------------------------------------


def list_libdir_jars(home_env_key=None, home=None, lib=None):
    """Lists the JAR files in the specified lib/ directory.

    Exactly one of home_env_key, home or lib must be specified.

    Args:
      home_env_key: Optional environment variable defining the home directory.
      home: Optional home directory path.
      lib: Optional lib directory path.

    Yields:
      The classpath entries from the specified lib directory.
    """
    assert (len(list(filter(lambda x: x is not None, [home_env_key, home, lib]))) == 1), \
        "Exactly one of 'home_env_key', 'home', 'lib' must be set."

    if lib is None:
        if home is None:
            home = os.environ.get(home_env_key)
            assert (home is not None), ("Environment variable undefined: %r" % home_env_key)
        lib = os.path.join(home, "lib")

    # Classpath entries named '*' match the glob "*.jar":
    return glob.glob(os.path.join(lib, "*.jar"))


# --------------------------------------------------------------------------------------------------


class KijiTool(object):

    def __init__(
        self,
        env=os.environ,
        cp_filter=None,
        hadoop_distribution=None,
    ):
        """Initializes a wrapper for the Kiji installation.

        This expects a KijiBento installation.

        Args:
            env: Dictionary of environment variables.
            cp_filter: Optional filter for classpath entries.
        """
        self._env = env
        assert (self.home_env_key in self._env), \
            ("Environment variable undefined: %r" % self.home_env_key)
        self._home_dir = os.path.abspath(self._env[self.home_env_key])
        assert os.path.isdir(self.home_dir), ("Home directory not found: %r" % self.home_dir)

        self._hadoop = HadoopTool(env=self._env)
        self._hbase = HBaseTool(env=self._env)
        self._hadoop_distribution = hadoop_distribution

        self._cp_filter = cp_filter

    @property
    def home_env_key(self):
        return "KIJI_HOME"

    @property
    def home_dir(self):
        return self._home_dir

    @property
    def hadoop(self):
        return self._hadoop

    @property
    def hbase(self):
        return self._hbase

    def _list_classpath_entries(self):
        if KIJI_CLASSPATH in self._env:
            user_classpath = self._env[KIJI_CLASSPATH].split(":")
            yield from user_classpath

        yield os.path.join(self.home_dir, "conf")

        distrib = self._hadoop_distribution
        if (distrib is None) or (len(distrib) == 0):
            distrib = ("hadoop%s" % self.hadoop.major_version)
        yield from list_libdir_jars(lib=os.path.join(self.home_dir, "lib", "distribution", distrib))

        yield from list_libdir_jars(home=self.home_dir)
        yield from list_libdir_jars(home_env_key="SCHEMA_SHELL_HOME")
        yield from list_libdir_jars(home_env_key="KIJI_MR_HOME")
        # yield from list_libdir_jars(home_env_key="MODELING_HOME")
        # yield from list_libdir_jars(home_env_key="HBASE_HOME")
        yield from self.hbase.classpath

    def get_classpath(self, lib_jars=()):
        """Reports the Express classpath.

        Note: classpath entries are normalized through normalize_classpath().
            In particular, classpath wildcards are expanded, duplicates eliminated.

        Args:
          lib_jars: Optional collection of user-specified JARs to include.
        Returns:
          An iterable of classpath entries.
        """
        express_classpath = self._list_classpath_entries()
        classpath = itertools.chain(lib_jars, express_classpath)
        classpath = normalize_classpath(classpath)
        if self._cp_filter is not None:
            classpath = filter(self._cp_filter, classpath)
        return classpath

    def list_paths_for_dist_cache(self, lib_jars):
        """Lists the JAR files to send to the distributed cache.

        Args:
          lib_jars: Optional collection of user-specified JARs to include.
        Returns:
          Iterable of fully-qualified path URIs to send to the distributed cache.
        """
        dc_entries = list()
        for cp_entry in self.get_classpath(lib_jars=lib_jars):
            if os.path.isfile(cp_entry):
                dc_entries.append("file://%s" % cp_entry)
            else:
                logging.debug("Skipping classpath entry for distributed cache: %r", cp_entry)

        logging.debug("JARs sent to the distributed cache:\n%s",
                      "\n".join(map(tab_indent, dc_entries)))
        return dc_entries



# --------------------------------------------------------------------------------------------------


# Description of the environment variables used.
ENV_VAR_HELP = """
Environment Variables:
  JAVA_LIBRARY_PATH: Colon-separated paths to additional native libs.
  JAVA_OPTS: Java args to append to java command and sent to JVM.
  KIJI_CLASSPATH: Colon-separated jars for classpath and distributed cache.
"""


def make_arg_parser():
    text_formatter = argparse.RawTextHelpFormatter

    parser = argparse.ArgumentParser(
        description="Kiji command-line interface.",
        epilog=ENV_VAR_HELP,
        formatter_class=text_formatter,
        add_help=False,
    )

    # Global flags available in all commands:
    parser.add_argument("--log-level", default="info", help="Logging level.")
    parser.add_argument(
        "--cp-filter",
        default="",
        help=("Classpath entry filter, expressed as a Python expression. "
              "Available symbols are: 'path', 'name', 'os' and 're'. "
              "For example, the filter: --cp-filter='\"jasper\" not in path' "
              "excludes entries whose path includes the word 'jasper'. "
              """With regex: --cp-filter='not re.match(r"servlet.*[.]jar", name)' """
              "excludes any entry whose file name matchs 'servlet*.jar'."),
    )

    parser.add_argument(
        "--jars",
        nargs="*",
        help="List of JAR files to place on the classpath and the distributed cache.",
    )

    parser.add_argument(
        "--hadoop-distribution",
        default=None,
        help=("Name of the Hadoop distribution to use (eg. hadoop1 or hadoop2).\n"
              "By default, this is automatically inferred from Hadoop's self reported version."),
    )

    return parser


def make_jar_arg_parser():
    text_formatter = argparse.RawTextHelpFormatter

    parser = argparse.ArgumentParser(
        description="Kiji command-line JAR runner.",
        epilog=ENV_VAR_HELP,
        formatter_class=text_formatter,
        add_help=False,
    )

    # Flags specific to the 'jar' command:
    parser.add_argument(
        "--class",
        help="Class name of the Express job to run.",
    )
    parser.add_argument(
        "--java-opts",
        nargs="*",
        help=("Optional list of options for the JVM "
              "(eg. --java-opts -Xmx2G -Xms1G -Dprop=val ...)."),
    )

    return parser


# --------------------------------------------------------------------------------------------------


class KijiCLI(object):
    """CLI interface for Kiji tools."""

    def __init__(self, flags, args, env=os.environ):
        self._flags = flags
        self._args = args
        self._env = env

        logging.debug("KijiCLI flags: %r - args: %r - env: %r", flags, args, env)

        # Construct a classpath filter, if any:
        cp_filter = None
        classpath_filter = flags.cp_filter
        if classpath_filter is not None:
            classpath_filter = classpath_filter.strip()
        if (classpath_filter is not None) and (len(classpath_filter) > 0):
            logging.debug("Constructing classpath filter for %r", classpath_filter)
            def custom_filter(path):
                globals = dict(
                    path=path,
                    name=os.path.basename(path),
                    os=os,
                    re=re,
                )
                return eval(classpath_filter, globals)

            cp_filter = custom_filter

        self._kiji = KijiTool(
            env=self.env,
            cp_filter=cp_filter,
            hadoop_distribution=self.flags.hadoop_distribution,
        )

    @property
    def flags(self):
        """Returns: Namespace object with the parsed flags."""
        return self._flags

    @property
    def args(self):
        """Returns: the list of unknown command-line arguments that were not parsed."""
        return self._args

    @property
    def env(self):
        """Returns: the shell environment used throughout this tool."""
        return self._env

    @property
    def kiji(self):
        """Returns: the Kiji installation wrapper."""
        return self._kiji

    def pop_args_head(self):
        """Pops the first unparsed command-line argument.

        Returns:
          The first command-line argument.
        """
        head, self._args = self._args[0], self._args[1:]
        return head

    def classpath(self):
        """Performs the 'classpath' command."""
        assert (len(self.args) == 0), ("Unexpected command-line arguments: %r" % self.args)
        lib_jars = []
        if self.flags.jars is not None:
            lib_jars.extend(self.flags.jars.split(","))
        print(":".join(self.kiji.get_classpath(lib_jars=lib_jars)))
        return os.EX_OK

    def run_class(self, class_name):
        """Performs the 'run-class' command."""
        lib_jars = []
        if self.flags.jars is not None:
            lib_jars.extend(self.flags.jars)
        classpath = list(self.kiji.get_classpath(lib_jars=lib_jars))

        java_opts = []
        if self.flags.java_opts is not None:
            java_opts = [self.flags.java_opts]

        user_args = list(self.args)
        logging.info("Running java class %r with parameters: %r", class_name, user_args)

        cmd = [
            "java",
        ] + java_opts + [
            "-classpath", ":".join(classpath),
            class_name,
        ] + user_args

        logging.debug("Running command:\n%s\n", " \\\n\t".join(map(repr, cmd)))
        return subprocess.call(cmd)


# --------------------------------------------------------------------------------------------------


_LOGGING_INITIALIZED = False


def parse_log_level(level):
  """Parses a logging level command-line flag.

  Args:
    level: Logging level command-line flag (string).
  Returns:
    Logging level (integer).
  """
  log_level = getattr(logging, level.upper(), None)
  if type(log_level) == int:
    return log_level

  try:
    return int(level)
  except ValueError:
    raise Error("Invalid logging-level: %r" % level)


def get_log_level(args):
    for arg in args:
        if arg.startswith("--log-level="):
            return parse_log_level(arg[len("--log-level="):])
    return logging.INFO


def setup_logging(log_level):
  """Initializes the logging system.

  Args:
    log_level: Logging level.
  """
  global _LOGGING_INITIALIZED
  if _LOGGING_INITIALIZED:
    logging.debug("setup_logging: logging system already initialized")
    return

  log_formatter = logging.Formatter(
      fmt="%(asctime)s %(levelname)s %(filename)s:%(lineno)s : %(message)s",
  )

  # Override the log date formatter to include the time zone:
  def format_time(record, datefmt=None):
    time_tuple = time.localtime(record.created)
    tz_name = time.tzname[time_tuple.tm_isdst]
    return "%(date_time)s-%(millis)03d-%(tz_name)s" % dict(
        date_time=time.strftime("%Y%m%d-%H%M%S", time_tuple),
        millis=record.msecs,
        tz_name=tz_name,
    )
  log_formatter.formatTime = format_time

  logging.root.handlers.clear()
  logging.root.setLevel(log_level)

  console_handler = logging.StreamHandler()
  console_handler.setFormatter(log_formatter)
  console_handler.setLevel(log_level)
  logging.root.addHandler(console_handler)

  _LOGGING_INITIALIZED = True


# --------------------------------------------------------------------------------------------------


def merge_namespace(ns1, ns2):
    """Merges two argparse.Namespace instances.

    Args:
        ns1, ns2: Namespaces to merge.
    Returns:
        A new Namespace with ns1 and ns2; ns2 entries override/shadow ns1 entries.
    """
    ns = argparse.Namespace()
    for key, value in ns1._get_kwargs():
        setattr(ns, key, value)
    for key, value in ns2._get_kwargs():
        setattr(ns, key, value)
    return ns


def main(parser, flags, args):
    """Main entry of this Python program.

    Dispatches to the appropriate command.

    Args:
        parser: argparse.ArgumentParser used to parse the command-line flags.
        flags: argparse.Namespace with the parsed command-line flags.
        args: Unparsed command-line arguments.
    Returns:
        Shell exit code.
    """
    logging.debug("Main flags = %r - args = %r", flags, args)
    args = args[1:]  # discard program path

    command = None
    if len(args) > 0:
        command, args = args[0], args[1:]

    if command is None:
        parser.print_usage()
        return os.EX_USAGE

    if command == "classpath":
        assert (len(args) == 0), ("Unexpected extra command-line arguments: %r" % args)
        cli = KijiCLI(flags=flags, args=args, env=os.environ)
        return cli.classpath()

    if command == "run-class":
        jar_parser = make_jar_arg_parser()
        (jar_flags, jar_args) = jar_parser.parse_known_args(args)
        logging.debug("JAR CLI flags = %r - args = %r", jar_flags, jar_args)

        class_name = getattr(jar_flags, "class")
        if (class_name is None) and (len(jar_args) > 0):
            class_name, jar_args = jar_args[0], jar_args[1:]
        assert (class_name is not None), ("No class name specified with [--class=]<class>.")
    else:
        class_name = KIJI_TOOL_LAUNCHER
        args.insert(0, command)

        jar_parser = make_jar_arg_parser()
        (jar_flags, jar_args) = jar_parser.parse_known_args(args)

    flags = merge_namespace(flags, jar_flags)

    cli = KijiCLI(flags=flags, args=jar_args, env=os.environ)
    return cli.run_class(class_name=class_name)


def init(args):
    """Initializes this Python program and runs the main function.

    Args:
      args: Command-line argument specified to this Python program;
          args[0] is the path of this program (/path/to/kiji.py);
          args[1] is the first command-line argument, if any, etc.
    """
    parser = make_arg_parser()
    (flags, unparsed_args) = parser.parse_known_args(args)

    try:
        log_level = parse_log_level(flags.log_level)
        setup_logging(log_level=log_level)
    except Error as err:
        print(err)
        return os.EX_USAGE

    # Run program:
    sys.exit(main(parser, flags, unparsed_args))


if __name__ == "__main__":
    init(sys.argv)
