import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import sys
import os
import socket
import multiprocessing
import datetime

def plot_logs(filename):
    df = pd.read_csv(filename, sep='\t')
    df['cpu'] /= multiprocessing.cpu_count()
    df['datetime'] = df['date'] + 'T' + df['time']
    df['datetime'] = [datetime.datetime.fromisoformat(dt) for dt in df['datetime']]
    n_gpu = np.sum(['gpu' in c for c in df.columns]) // 2
    
    out_filename = os.path.basename(filename).split(".")[0]
    plot_usage(df, out_filename)
    for gpu in range(n_gpu):
        df['gpu{}_util'.format(gpu)] /= 100
        plot_usage(df, out_filename, gpu=gpu)
    

def plot_usage(df, filename, gpu=None, c1='tab:red', c2='tab:blue'):
    if gpu is None:
        cpu, ram = 'cpu', 'ram'
        cpu_label, ram_label = 'CPU Utilization', 'RAM Utilization'
    else:
        cpu, ram = 'gpu{}_util'.format(gpu), 'gpu{}_ram'.format(gpu)
        cpu_label, ram_label = 'GPU {} Utilization'.format(gpu), 'GPU {} vRAM Utilization'.format(gpu)

    fig, ax1 = plt.subplots()
    ax2 = ax1.twinx()
    ax1.plot(df['datetime'], df[cpu], c=c1)
    ax2.plot(df['datetime'], df[ram], c=c2)
    ax1.axhline(np.mean(df[cpu]), c=c1, linestyle='--')
    ax2.axhline(np.mean(df[ram]), c=c2, linestyle='--')

    ax1.set_ylabel(cpu_label)
    ax2.set_ylabel(ram_label)
    ax1.set_xlabel("Date")
    
    ax1.set_ylim(0, np.max(df[cpu]))
    ax2.set_ylim(0, np.max(df[ram]))

    ax1.yaxis.label.set_color(c1)
    ax2.yaxis.label.set_color(c2)

    ax1.spines["left"].set_edgecolor(c1)
    ax2.spines["right"].set_edgecolor(c2)

    ax1.tick_params(axis="y", colors=c1)
    ax2.tick_params(axis="y", colors=c2)
    
    ax1.set_title("Memory monitor: {}".format(socket.gethostname()))
    
    fig.savefig("{}_{}.png".format(filename, cpu))

if __name__ == "__main__":
    filename = sys.argv[1]
    plot_logs(filename)
