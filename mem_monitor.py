# memory-monitor polls process group memory usage and sends real-time
# updates and warnings by both terminal output and email.

# Copyright (C) 2020 Scott Gigante, scottgigante@gmail.com

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import subprocess
import time
import csv
import pandas as pd
import numpy as np
import os
import sys
import yaml
import platform
import shutil
import datetime
import pynvml

try:
    pynvml.nvmlInit()
    _N_GPU = pynvml.nvmlDeviceGetCount()
except pynvml.NVMLError_LibraryNotFound:
    _N_GPU = 0

# System constants
# Size of 1GB in B
_GIGABYTE = 1024.0 ** 3
# Size of 1KB in B
_KILOBYTE = 1024.0
# Hour in seconds
_HOUR = 3600.0
# Total system memory
_TOTAL_MEMORY = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / _GIGABYTE

_CONFIG_DIR = os.path.dirname(os.path.abspath(__file__))


def load_config():
    with open(os.path.join(_CONFIG_DIR, "config.yml"), "r") as handle:
        return yaml.load(handle.read(), Loader=yaml.FullLoader)


# Configuration
try:
    config = load_config()
except FileNotFoundError:
    # no config found, use default config
    shutil.copyfile(
        os.path.join(_CONFIG_DIR, "config.default"),
        os.path.join(_CONFIG_DIR, "config.yml"),
    )
    config = load_config()

# Proportion of available memory for which we launch an alert
_CRITICAL_FRACTION = config["memory"]["critical_fraction"]
# Proportion of available memory for which we launch an alert
_TERMINATE_ACTIVE = config["memory"]["terminate"]["active"]
# Proportion of available memory for which we launch an alert
_TERMINATE_FRACTION = config["memory"]["terminate"]["terminate_fraction"]
# Amount of time between updates
_UPDATE = config["time"]["update"]

# Process parameters
# Minimum CPU above which a process is considered active, in CPUs
_ACTIVE_USAGE = config["cpu"]["active_usage"]
# Minimum time to wait between warnnig the same process, in hours
_WARNING_COOLDOWN = config["time"]["warning_cooldown"]
# Maxmimum time after last usage to consider a process active
_MIN_IDLE_TIME = config["time"]["min_idle_time"]
# timeouts in percent memory vs time idle
# Defaults:
# 50% of memory, warn immediately
# 20% of memory, warn after 6h
# 10% of memory, warn after 1 day
# 5% of memory, warn after 1 week
# 1% of memory, warn after 1 month
_IDLE_TIMEOUT_HOURS = config["memory"]["idle_timeout_hours"]

_LOG_ACTIVE = config["log"]["active"]


def get_log_path(filename):
    filename = os.path.abspath(filename)
    log_dirname = os.path.dirname(filename)
    log_basename = os.path.basename(filename)
    filename = os.path.join(
        log_dirname, "{}_{}".format(datetime.date.today(), log_basename)
    )
    return filename


_LOG_FILENAME = get_log_path(config["log"]["filename"])

__print__ = print


def print(msg, file=sys.stderr):
    __print__(msg, file=file)
    file.flush()


def print_config():
    if _TERMINATE_ACTIVE:
        termination = "Active\n    System critical process termination memory threshold: {termination_total:.1f}GB ({termination_percent:.2f}%)".format(
            termination_percent=_TERMINATE_FRACTION * 100,
            termination_total=_TERMINATE_FRACTION * _TOTAL_MEMORY,
        )
    else:
        termination = "Inactive"
    if _LOG_ACTIVE:
        logging = _LOG_FILENAME
    else:
        logging = "Inactive"
    group_warnings = "\n".join(
        [
            "    {percent:.1f}% of memory ({total:.1f}GB), warn after {time:d} hours".format(
                percent=fraction * 100, total=fraction * _TOTAL_MEMORY, time=time,
            )
            for fraction, time in _IDLE_TIMEOUT_HOURS.items()
        ]
    )
    config_log = """memory-monitor

Configuration (config.yml):
  System memory: {total_memory:.1f}GB
  System critical warning memory threshold: {critical_total:.1f}GB ({critical_percent:.2f}%)
  System critical process termination: {termination}
  Process group warnings:
{group_warnings:s}
  Processes considered idle after: {min_idle_time:d} seconds
  Processes considered idle with CPU usage less than: {active_usage:.1f}%
  Processes polled every: {update:d} seconds
  Maximum warning frequency: {warning_cooldown:d} seconds
  Warnings will be sent to: {email:s}
  Usage logging: {logging:s}
""".format(
        total_memory=_TOTAL_MEMORY,
        critical_percent=_CRITICAL_FRACTION * 100,
        critical_total=_CRITICAL_FRACTION * _TOTAL_MEMORY,
        termination=termination,
        group_warnings=group_warnings,
        min_idle_time=_MIN_IDLE_TIME,
        warning_cooldown=_WARNING_COOLDOWN,
        active_usage=_ACTIVE_USAGE * 100,
        update=_UPDATE,
        email=config["email"],
        logging=logging,
    )
    print(config_log)


# Slack parameters
_SYSTEM_WARNING = """Critical warning: {uname} memory usage high: {available:.1f}GB of {total:.1f}GB available ({percentage:.2f}%)."""
_TERMINATE_WARNING = """\n\nTerminated {user}'s process group {pgid} and freed {memory:.1f}GB ({percentage:.2f}%) of RAM."""
_IDLE_MESSAGE = """has been idle since {last_cpu} ({idle_hours:.1f} hours ago) and """
_USER_WARNING = """Warning: {user}'s process group {pgid} {idle_message}is using {memory:.1f}GB ({percentage:.2f}%) of RAM. Kill it with `kill -- -{pgid}`."""


def send_mail(subject, message):
    subprocess.run(["mail", "-s", subject, config["email"]], input=message.encode())


def format_time(t):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t))


def fetch_pid_memory_usage(pid):
    # add 0.5KB as average error due to truncation
    pss_adjust = 0.5
    pss = 0
    try:
        with open("/proc/{}/smaps".format(pid), "r") as smaps:
            for line in smaps:
                if line.startswith("Pss"):
                    pss += int(line.split(" ")[-2]) + pss_adjust
    except (FileNotFoundError, ProcessLookupError, PermissionError):
        pass
    return pss


class ProcessGroup:
    def __init__(self, pgid, user, cputime, memory):
        self.pgid = pgid
        self.user = user
        self.memory = memory
        self.cputime = cputime
        self.cputime_since_update = 0
        self.start_time = time.time()
        self.last_cpu_time = time.time()
        self.last_warning = None
        self.total_warnings = 0

    @property
    def idle_seconds(self):
        return time.time() - self.last_cpu_time

    @property
    def idle_hours(self):
        global _HOUR
        return self.idle_seconds / _HOUR

    @property
    def memory_fraction(self):
        global _TOTAL_MEMORY
        return self.memory / _TOTAL_MEMORY

    @property
    def memory_percent(self):
        return self.memory_fraction * 100

    def recently_warned(self, timeout):
        global _WARNING_COOLDOWN
        global _HOUR
        if self.last_warning is None:
            return False
        else:
            since_last_warning = time.time() - self.last_warning
            return since_last_warning <= max(timeout * _HOUR, _WARNING_COOLDOWN)

    def update(self, cputime, memory):
        global _ACTIVE_USAGE
        self.memory = memory
        self.cputime_since_update = max(cputime - self.cputime, 0)
        if self.cputime_since_update > _ACTIVE_USAGE * _UPDATE:
            self.last_cpu_time = time.time()
        self.cputime = cputime

    def check(self):
        global _IDLE_TIMEOUT_HOURS
        cutoffs = np.array(list(_IDLE_TIMEOUT_HOURS.keys()))
        if self.memory_fraction > np.min(cutoffs):
            cutoff = np.max(cutoffs[cutoffs < self.memory_fraction])
            timeout = _IDLE_TIMEOUT_HOURS[cutoff]
            if self.idle_hours > timeout:
                if not self.recently_warned(timeout):
                    # warn
                    self.warn()
                    self.log(self.warning_string())
                    return 1
                else:
                    self.log("{}, muted".format(self.warning_string()))
                    return 1
            else:
                self.log("OK")
        return 0

    def log(self, code="OK"):
        print("{}: {}".format(code, self))

    def warning_string(self):
        if self.total_warnings < 2:
            return "Warning"
        else:
            return "Warning ({}x)".format(self.total_warnings)

    def format_warning(self):
        global _USER_WARNING
        global _IDLE_MESSAGE
        idle_message = "" if self.idle_hours == 0 else _IDLE_MESSAGE.format(
            last_cpu=format_time(self.last_cpu_time),
            idle_hours=self.idle_hours,
        )
        return _USER_WARNING.format(
            user=self.user,
            pgid=self.pgid,
            idle_message=idle_message,
            memory=self.memory,
            percentage=self.memory_percent,
        )

    def warn(self):
        self.total_warnings += 1
        self.last_warning = time.time()
        send_mail(
            subject="Memory Usage {}: {}".format(self.warning_string(), self.user),
            message=self.format_warning(),
        )

    def terminate(self):
        subprocess.run(["kill", "--", "-{}".format(self.pgid)])

    def __repr__(self):
        return "<PGID {} ({})>".format(self.pgid, self.user)

    def __str__(self):
        global _MIN_IDLE_TIME
        if self.idle_seconds > _MIN_IDLE_TIME:
            idle_str = "idle for {:.2f} hours".format(self.idle_hours)
        else:
            idle_str = "active"
        return "PGID {} ({}), memory {:.1f}GB ({:.2f}%), {}".format(
            self.pgid, self.user, self.memory, self.memory_percent, idle_str
        )


class MemoryMonitor:
    def __init__(self):
        self.superuser = self.check_superuser()
        self.processes = dict()
        self.init_logfile()

    def init_logfile(self):
        if _LOG_ACTIVE:
            self.logfile = _LOG_FILENAME
            if not os.path.isfile(self.logfile):
                with open(self.logfile, "w") as handle:
                    headers = ["date", "time", "cpu", "ram"]
                    for i in range(_N_GPU):
                        headers += ["gpu{}_util".format(i), "gpu{}_ram".format(i)]
                    print("\t".join(headers), file=handle)

    def check_superuser(self):
        superuser = os.geteuid() == 0
        if not superuser:
            print(
                "memory-monitor does not have superuser privileges. "
                "Monitoring user processes only."
            )
        return superuser

    def fetch_processes(self):
        global _KILOBYTE
        global _GIGABYTE
        # run ps
        stdout, _ = subprocess.Popen(
            ["ps", "-e", "--no-headers", "-o", "pgid,pid,rss,cputimes,user"],
            stdout=subprocess.PIPE,
        ).communicate()
        # read into data frame
        reader = csv.DictReader(
            stdout.decode("ascii").splitlines(),
            delimiter=" ",
            skipinitialspace=True,
            fieldnames=["pgid", "pid", "rss", "cputime", "user"],
        )
        df = pd.DataFrame([r for r in reader])
        if df.shape[0] == 0:
            raise RuntimeError("ps output is empty.")
        if not self.superuser:
            # only local user
            df = df.loc[df["user"] == os.environ["USER"]]
        # convert to numeric
        df["pgid"] = df["pgid"].values.astype(int)
        df["pid"] = df["pid"].values.astype(int)
        df["rss"] = df["rss"].values.astype(int)
        df["cputime"] = df["cputime"].values.astype(float)
        # pre-filter
        df = df.loc[df["rss"] > 0]
        df["memory"] = (
            np.array([fetch_pid_memory_usage(pid) for pid in df["pid"]])
            * _KILOBYTE
            / _GIGABYTE
        )
        # filter
        df = df.loc[df["memory"] > 0]
        df = df.loc[df["user"] != "root"]
        df = df.loc[df["user"] != "sddm"]
        # sum over process groups
        df = (
            df[["pgid", "user", "cputime", "memory"]]
            .groupby(["pgid", "user"])
            .agg(np.sum)
            .reset_index()
            .set_index("pgid")
            .sort_values("memory", ascending=False)
        )
        return df

    def fetch_total_memory(self):
        global _KILOBYTE
        global _GIGABYTE
        fieldnames = ["source", "total", "used", "free", "shared", "cache", "available"]
        # run ps
        stdout, _ = subprocess.Popen(["free"], stdout=subprocess.PIPE).communicate()
        # read into data frame
        reader = csv.DictReader(
            stdout.decode("ascii").splitlines()[1:],
            delimiter=" ",
            skipinitialspace=True,
            fieldnames=fieldnames,
        )
        # total system memory memory
        system_mem = {fn:0 for fn in fieldnames}
        for curr_mem in reader:
            for k, v in curr_mem.items():
                if k == "source": continue
                if v is None: continue
                v_readable = int(v) * _KILOBYTE / _GIGABYTE
                system_mem[k] += v_readable
                # For swap, add "free" to "available"
                if curr_mem["source"] == "Swap:" and k == "free":
                    system_mem["available"] += v_readable
        return system_mem

    def fetch_total_cpu(self):
        cputime = 0
        for process in self.processes.values():
            cputime += process.cputime_since_update
        return cputime / _UPDATE

    def fetch_gpu_stats(self):
        stats = {}
        for i in range(_N_GPU):
            handle = pynvml.nvmlDeviceGetHandleByIndex(i)
            memory_info = pynvml.nvmlDeviceGetMemoryInfo(handle)
            util = pynvml.nvmlDeviceGetUtilizationRates(handle).gpu
            stats[i] = {
                "gpu_util": util,
                "ram_free": memory_info.free,
                "ram_total": memory_info.total,
            }
        return stats

    def update_processes(self):
        print("[{}]".format(format_time(time.time())))
        df = self.fetch_processes()
        for pgid in df.index:
            record = df.loc[pgid]
            try:
                # process exists, update
                process = self.processes[pgid]
                process.update(record["cputime"], record["memory"])
            except KeyError:
                # new process
                process = ProcessGroup(
                    pgid, record["user"], record["cputime"], record["memory"]
                )
                self.processes[pgid] = process
            # check memory/runtime
            process.check()
        for pgid in set(self.processes).difference(df.index):
            # pgid disappeared, must have ended
            del self.processes[pgid]

    def highest_usage_process(self):
        highest_usage = 0
        for pgid, process in self.processes.items():
            if process.memory > highest_usage:
                highest_usage_process = process
                highest_usage = process.memory
        return highest_usage_process

    def check(self):
        system_mem = self.fetch_total_memory()
        global _GIGABYTE
        global _CRITICAL_FRACTION
        global _TERMINATE_FRACTION
        global _TERMINATE_ACTIVE
        if (
            _TERMINATE_ACTIVE
            and system_mem["available"] < _TERMINATE_FRACTION * system_mem["total"]
        ):
            terminate_process = self.highest_usage_process()
            terminate_process.terminate()
            self.log(
                system_mem, "Warning (terminated {})".format(terminate_process.pgid)
            )
            self.warn(system_mem, terminate_process=terminate_process)
            self.update_processes()
            return self.check()
        elif system_mem["available"] < _CRITICAL_FRACTION * system_mem["total"]:
            self.log(system_mem, "Warning")
            self.warn(system_mem)
            return 1
        else:
            self.log(system_mem, "OK")
        return 0

    def system_available_percent(self, system_mem):
        return system_mem["available"] / system_mem["total"] * 100

    def log_usage(self, system_mem):
        gpu_stats = self.fetch_gpu_stats()
        date, time = datetime.datetime.now().isoformat("@", "seconds").split("@")
        cpu = self.fetch_total_cpu()
        fmt = lambda x, p: str(np.round(x, p))
        output = [
            date,
            time,
            fmt(cpu, 2),
            fmt(1 - system_mem["free"] / system_mem["total"], 3),
        ]
        gpu_stats = self.fetch_gpu_stats()
        for i in range(_N_GPU):
            output += [
                str(gpu_stats[i]["gpu_util"]),
                fmt(1 - gpu_stats[i]["ram_free"] / gpu_stats[i]["ram_total"], 3),
            ]
        with open(self.logfile, "a") as handle:
            print("\t".join(output), file=handle)

    def log(self, system_mem, code="OK"):
        if _LOG_ACTIVE:
            self.log_usage(system_mem)
        print(
            "{}: {:.1f}GB of {:.1f}GB available ({:.2f}%).".format(
                code,
                system_mem["available"],
                system_mem["total"],
                self.system_available_percent(system_mem),
            )
        )

    def format_warning(self, system_mem, terminate_process=None):
        global _SYSTEM_WARNING
        global _TERMINATE_WARNING
        warning = _SYSTEM_WARNING.format(
            uname=platform.uname().node,
            available=system_mem["available"],
            total=system_mem["total"],
            percentage=self.system_available_percent(system_mem),
        )
        if terminate_process is not None:
            warning += _TERMINATE_WARNING.format(
                user=terminate_process.user,
                pgid=terminate_process.pgid,
                memory=terminate_process.memory,
                percentage=terminate_process.memory_percent,
            )
        return warning

    def warn(self, system_mem, terminate_process=None):
        if terminate_process is None:
            subject = "System Memory Critical"
        else:
            subject = "System Memory Critical (Terminated {})".format(
                terminate_process.pgid
            )
        send_mail(
            subject=subject,
            message=self.format_warning(
                system_mem, terminate_process=terminate_process
            ),
        )

    def update(self):
        self.update_processes()
        self.check()


if __name__ == "__main__":
    print_config()
    m = MemoryMonitor()
    while True:
        m.update()
        time.sleep(_UPDATE)
