import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import sys
import os
import socket
import multiprocessing
import datetime
import pynvml

try:
    pynvml.nvmlInit()
except pynvml.NVMLError_LibraryNotFound:
    pass


def total_memory():
    mem_bytes = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    mem_gib = mem_bytes / (1024.0 ** 3)
    return mem_gib


def gpu_memory(gpu_idx):
    handle = pynvml.nvmlDeviceGetHandleByIndex(gpu_idx)
    mem_bytes = pynvml.nvmlDeviceGetMemoryInfo(handle).total
    mem_gib = mem_bytes / (1024.0 ** 3)
    return mem_gib


def plot_logs(filename, bins=200):
    df = pd.read_csv(filename, sep="\t")
    df["datetime"] = df["date"] + "T" + df["time"]
    df["datetime"] = [datetime.datetime.fromisoformat(dt) for dt in df["datetime"]]
    n_gpu = np.sum(["gpu" in c for c in df.columns]) // 2
    df = df.groupby(pd.cut(df["datetime"], bins, include_lowest=True)).mean()
    df["datetime"] = [interval.mid.to_pydatetime() for interval in df.index]
    out_filename = os.path.basename(filename).split(".")[0]
    plot_usage(df, out_filename)
    for gpu in range(n_gpu):
        plot_usage(df, out_filename, gpu=gpu)


def plot_usage(df, filename, gpu=None, c1="tab:red", c2="tab:blue"):
    if gpu is None:
        cpu, ram = "cpu", "ram"
        cpu_label, ram_label = "CPU Utilization (threads)", "RAM Utilization (GB)"
        total_ram = total_memory()
        totals = "(Total: {} CPUs, {:.0f}GB RAM)".format(
            multiprocessing.cpu_count(), total_ram
        )
    else:
        cpu, ram = "gpu{}_util".format(gpu), "gpu{}_ram".format(gpu)
        cpu_label, ram_label = (
            "GPU {} Utilization (%)".format(gpu),
            "GPU {} vRAM Utilization (GB)".format(gpu),
        )
        total_ram = gpu_memory(gpu)
        totals = "(Total: {:.0f}GB vRAM)".format(total_ram)

    df[ram] *= total_ram

    fig, ax1 = plt.subplots()
    ax2 = ax1.twinx()
    ax1.plot(df["datetime"], df[cpu], c=c1)
    ax2.plot(df["datetime"], df[ram], c=c2)
    ax1.axhline(np.mean(df[cpu]), c=c1, linestyle="--")
    ax2.axhline(np.mean(df[ram]), c=c2, linestyle="--")

    ax1.set_ylabel(cpu_label)
    ax2.set_ylabel(ram_label)
    ax1.set_xlabel("Date")

    ax1.set_ylim(0, np.max(df[cpu]) * 1.05)
    ax2.set_ylim(0, np.max(df[ram]) * 1.05)

    ax1.yaxis.label.set_color(c1)
    ax2.yaxis.label.set_color(c2)

    ax1.spines["left"].set_edgecolor(c1)
    ax2.spines["right"].set_edgecolor(c2)

    ax1.tick_params(axis="y", colors=c1)
    ax2.tick_params(axis="y", colors=c2)

    ax1.set_title("Memory monitor: {} {}".format(socket.gethostname(), totals))

    fig.autofmt_xdate(bottom=0.2, rotation=30, ha="right")
    fig.tight_layout()
    fig.savefig("{}_{}.png".format(filename, cpu))


if __name__ == "__main__":
    filename = sys.argv[1]
    plot_logs(filename)
