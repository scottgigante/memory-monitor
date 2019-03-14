import subprocess
import time
import csv
import pandas as pd
import numpy as np
import os
import sys
import yaml
import platform

# System constants
# Size of 1GB in B
_GIGABYTE = (1024.**3)
# Size of 1KB in B
_KILOBYTE = 1024.
# Hour in seconds
_HOUR = 3600.
# Total system memory
_TOTAL_MEMORY = os.sysconf('SC_PAGE_SIZE') * \
    os.sysconf('SC_PHYS_PAGES') / _GIGABYTE

# Configuration
with open("config.yml", 'r') as handle:
    config = yaml.load(handle.read())

# Proportion of available memory for which we launch an alert
_MEMORY_CRITICAL = config['memory']['critical_fraction']
# Amount of time between updates
_UPDATE_STEP = config['time']['update']

# Process parameters
# Minimum CPU above which a process is considered active, in CPUs
_MIN_CPU_USAGE = config['cpu']['active_usage']
# Minimum time to wait between warnnig the same process, in hours
_MIN_WARNING_WAIT = config['time']['warning_cooldown']
# Maxmimum time after last usage to consider a process active
_ACTIVE_TIME = config['time']['min_idle_time']
# timeouts in percent memory vs time idle
# Defaults:
# 50% of memory, warn immediately
# 20% of memory, warn after 6h
# 10% of memory, warn after 1 day
# 5% of memory, warn after 1 week
# 1% of memory, warn after 1 month
_MEMORY_IDLE_TIMEOUTS = config['memory']['idle_timeout_hours']

# Slack parameters
_SYSTEM_WARNING = """Critical warning: {uname} memory usage high: {available:.1f}GB of {total:.1f}GB available ({percentage:.2f}%)."""
_USER_WARNING = """Warning: {user}'s process group {pgid} has been idle since {last_cpu} ({idle_hours:.1f} hours ago) and is using {memory:.1f}GB ({percentage:.2f}%) of RAM. Kill it with `kill -- -{pgid}`."""


def send_mail(subject, message):
    subprocess.run(["mail", "-s", subject,
                    config['email']],
                   input=message.encode())


def format_time(t):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t))


def fetch_pid_memory_usage(pid):
    # add 0.5KB as average error due to truncation
    pss_adjust = 0.5
    pss = 0
    try:
        with open("/proc/{}/smaps".format(pid), 'r') as smaps:
            for line in smaps:
                if line.startswith("Pss"):
                    pss += int(line.split(" ")[-2]) + pss_adjust
    except (FileNotFoundError, ProcessLookupError):
        pass
    return pss


class ProcessGroup():

    def __init__(self, pgid, user, cputime, memory):
        self.pgid = pgid
        self.user = user
        self.memory = memory
        self.cputime = cputime
        self.start_time = time.time()
        self.last_cpu_time = time.time()
        self.last_warning = None

    @property
    def idle_seconds(self):
        return (time.time() - self.last_cpu_time)

    @property
    def idle_hours(self):
        return self.idle_seconds / _HOUR

    @property
    def memory_fraction(self):
        global _TOTAL_MEMORY
        return self.memory / _TOTAL_MEMORY

    @property
    def memory_percent(self):
        return self.memory_fraction * 100

    def recently_warned(self, timeout):
        global _MIN_WARNING_WAIT
        if self.last_warning is None:
            return False
        else:
            since_last_warning = (time.time() - self.last_warning)
            return since_last_warning <= max(timeout, _MIN_WARNING_WAIT)

    def update(self, cputime, memory):
        global _MIN_CPU_USAGE
        self.memory = memory
        cpu_since_update = cputime - self.cputime
        if cpu_since_update > _MIN_CPU_USAGE * _UPDATE_STEP:
            self.last_cpu_time = time.time()
        self.cputime = cputime

    def check(self):
        global _MEMORY_IDLE_TIMEOUTS
        cutoffs = np.array(list(_MEMORY_IDLE_TIMEOUTS.keys()))
        if self.memory_fraction > np.min(cutoffs):
            cutoff = np.max(cutoffs[cutoffs < self.memory_fraction])
            timeout = _MEMORY_IDLE_TIMEOUTS[cutoff]
            if self.idle_hours > timeout:
                if not self.recently_warned(timeout):
                    # warn
                    self.log("Warning")
                    self.warn()
                    return 1
                else:
                    self.log("Warning (muted)")
                    return 1
            else:
                self.log("OK")
        return 0

    def log(self, code="OK"):
        print("{}: {}".format(code, self))

    def format_warning(self):
        return _USER_WARNING.format(
            user=self.user,
            pgid=self.pgid,
            last_cpu=format_time(self.last_cpu_time),
            idle_hours=self.idle_hours,
            memory=self.memory,
            percentage=self.memory_percent)

    def warn(self):
        global _USER_WARNING
        send_mail(subject="Memory Usage Warning",
                  message=self.format_warning())

    def __repr__(self):
        return "<PGID {} ({})>".format(self.pgid, self.user)

    def __str__(self):
        global _ACTIVE_TIME
        if self.idle_seconds > _ACTIVE_TIME:
            idle_str = "idle for {:.2f} hours".format(self.idle_hours)
        else:
            idle_str = "active"
        return "PGID {} ({}), memory {:.1f}GB ({:.2f}%), {}".format(
            self.pgid, self.user, self.memory, self.memory_percent, idle_str)


class MemoryMonitor():

    def __init__(self):
        self.processes = dict()

    def fetch_processes(self):
        global _KILOBYTE
        global _GIGABYTE
        # run ps
        stdout, _ = subprocess.Popen(["ps", "-e", "--no-headers",
                                      "-o", "pgid,pid,rss,cputimes,user"],
                                     stdout=subprocess.PIPE).communicate()
        # read into data frame
        reader = csv.DictReader(stdout.decode('ascii').splitlines(),
                                delimiter=' ', skipinitialspace=True,
                                fieldnames=['pgid', 'pid', 'rss', 'cputime', 'user'])
        df = pd.DataFrame([r for r in reader])
        # convert to numeric
        df['pgid'] = df['pgid'].values.astype(int)
        df['pid'] = df['pid'].values.astype(int)
        df['rss'] = df['rss'].values.astype(int)
        df['cputime'] = df['cputime'].values.astype(float)
        # pre-filter
        df = df.loc[df['rss'] > 0]
        df['memory'] = np.array([fetch_pid_memory_usage(pid)
                                 for pid in df['pid']]) * _KILOBYTE / _GIGABYTE
        # filter
        df = df.loc[df['memory'] > 0]
        df = df.loc[df['user'] != 'root']
        df = df.loc[df['user'] != 'sddm']
        # sum over process groups
        df = df[['pgid', 'user', 'cputime', 'memory']].groupby(
            ["pgid", "user"]).agg(np.sum).reset_index().set_index(
            'pgid').sort_values('memory', ascending=False)
        return df

    def fetch_total(self):
        global _KILOBYTE
        global _GIGABYTE
        # run ps
        stdout, _ = subprocess.Popen(["free"],
                                     stdout=subprocess.PIPE).communicate()
        # read into data frame
        reader = csv.DictReader(stdout.decode('ascii').splitlines()[1:],
                                delimiter=' ', skipinitialspace=True,
                                fieldnames=["", "total", "used", "free",
                                            "shared", "cache", "available"])
        # total system memory memory
        system_mem = next(reader)
        del system_mem[""]
        for k, v in system_mem.items():
            system_mem[k] = int(v) * _KILOBYTE / _GIGABYTE
        return system_mem

    def update_processes(self):
        print('[{}]'.format(format_time(time.time())))
        df = self.fetch_processes()
        for pgid in df.index:
            record = df.loc[pgid]
            try:
                # process exists, update
                process = self.processes[pgid]
                process.update(record['cputime'], record['memory'])
            except KeyError:
                # new process
                process = ProcessGroup(pgid, record['user'],
                                       record['cputime'], record['memory'])
                self.processes[pgid] = process
            # check memory/runtime
            process.check()
        for pgid in set(self.processes).difference(df.index):
            # pgid disappeared, must have ended
            del self.processes[pgid]

    def check(self):
        system_mem = self.fetch_total()
        global _GIGABYTE
        global _MEMORY_CRITICAL
        if system_mem['available'] < _MEMORY_CRITICAL * system_mem['total']:
            self.log(system_mem, "Warning")
            self.warn(system_mem)
            return 1
        else:
            self.log(system_mem, "OK")
        return 0

    def system_available_percent(self, system_mem):
        return system_mem['available'] / system_mem['total'] * 100

    def log(self, system_mem, code="OK"):
        print("{}: {:.1f}GB of {:.1f}GB available ({:.2f}%).".format(
            code,
            system_mem['available'],
            system_mem['total'],
            self.system_available_percent(system_mem)
        ), file=sys.stderr)

    def format_warning(self, system_mem):
        global _SYSTEM_WARNING
        return _SYSTEM_WARNING.format(
            uname=platform.uname().node,
            available=system_mem['available'],
            total=system_mem['total'],
            percentage=self.system_available_percent(system_mem))

    def warn(self, system_mem):
        send_mail(subject="Memory Critical",
                  message=self.format_warning(system_mem))

    def update(self):
        self.update_processes()
        self.check()


if __name__ == "__main__":
    m = MemoryMonitor()
    while True:
        m.update()
        time.sleep(_UPDATE_STEP)
