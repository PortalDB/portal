#!/usr/bin/env pytho

import sys;
sys.path.append('/usr/local/lib/python2.7/dist-packages')
import pandas as pd;
import locale;
import models;
import ConfigParser;
import numpy as np;
import matplotlib;
import matplotlib.pyplot as plt;
import os;
from peewee import *;
import seaborn as sns
sns.set_context('notebook', font_scale=1)
sns.set_style('whitegrid')
plt.ioff()
colorList = ["#E6E600", "#01DF01", "#FF4000", "#5882FA", "#8A0886", "#FAAC58", "#A9D0F5", "#81F781"]   
db = None
import matplotlib.ticker as mtick
locale.setlocale(locale.LC_ALL, 'en_US.utf8')

    
def removeCloseValues(alignment, ticks,xmax):
    n = 0
    while n < (len(alignment) - 1):
        while (alignment[n+1] - alignment[n] < xmax/8):
            del alignment[n + 1]
            del ticks[n + 1]
            if(n == len(alignment) -1):
                return alignment, ticks
        n = n + 1
    return alignment, ticks

def smallPlot(name, dataframes):
    sns.set_context('notebook', font_scale=1)
    keys = ['RGS', 'RG', 'OGS', 'OG', 'HGS', 'HG', 'VES', 'VE', 'RG_E2D', 'OG_E2D', 'HG_E2D']
    lines = ['-', ':', '-', '--', '-', '-.', '-', '-', '-', '-', '-']
    colors = sns.color_palette("Paired", n_colors=11)
    markers = ['', 's', '', 'o', '', 'D',  "", ">", "", "", ""]
    linewitdhs = [1, 3, 1, 3, 1, 3, 1, 1, 1, 1, 1]
    linesDict =  dict(zip(keys, lines))
    colorsDict = dict(zip(keys, colors))
    markersDict = dict(zip(keys, markers))
    linewidthsDict = dict(zip(keys, linewitdhs))
    items = [value for key, value in dataframes.items() if name in key]
    if not items:
        print "No data found for that dataset"
        return
    else:
        items = items[0]
    for i, item in enumerate(items):
#         pprint.pprint(item[0])
        labels = list(item[0].columns.values)
        fig = plt.figure(figsize=(3, 2.25))
        ax1 = plt.gca()
        plt.ylabel(item[2])
        plt.locator_params(nbins=5, axis="x")
        plt.locator_params(nbins=4, axis="y")
        plt.grid(False)


        for label in labels:
            #plotting a column only if its data are not None
            if any(item[0][label].tolist()):
                plt.plot(item[0][label], label=label, linestyle=linesDict[label], color=colorsDict[label], linewidth=linewidthsDict[label], marker=markersDict[label])
        #setting up secondary x=axis if avaliable
        if item[4] is not None:
            xmin, xmax = ax1.get_xlim()
            ax2 = ax1.twiny()
            ax2.set_xlabel(item[6])
            ax2.grid(False)
            #if the tick labels are too close they overlap, so we remove close values. The function removes values which are closer than 1/16th of the graph
            new_tick_locations, ticks = removeCloseValues(item[5], item[4],xmax) 
            ax2.set_xticklabels(ticks)
            ax2.set_xticks(new_tick_locations)
            #plt.title(item[1] + " build_num=" + str(item[3]), y=1.26)

        #finding number of digits
        xmax = max(item[0].index.values)
        x = len(str(xmax).replace(".0", ""))
        divider = 1
        xlabelextra = ""
        if (x >  6):
            divider = 1000000
            xlabelextra = " (Millions)"
#         elif(x > 5):
#             divider = 100000;
#             xlabelextra = " (Hundred Thousands)"
        elif(x > 4):
            divider = 1000;
            xlabelextra = " (Thousands)"

        ax1.set_xlabel(item[0].index.name + xlabelextra)
        #sorting the legends to match the order of the graph
        handles, labels = ax1.get_legend_handles_labels()
        #filtering out the none values
        lastValues = filter(None, item[0].tail(1).values[0])
        legends = zip(*sorted(zip(lastValues, labels, handles), key=lambda t: t[0], reverse=True))
        legends.pop(0)
        labels, handles = legends
        lgd = plt.legend(handles, labels, bbox_to_anchor=(1.02, 0.90), loc=2, borderaxespad=0.)

        ax1.xaxis.set_major_formatter(mtick.FuncFormatter(lambda y, pos: ('%.f')%(y/divider)))
        #setting up the maximum of x axis to the maximum value
        ax1.set_xlim(xmin=0, xmax=xmax)

        #figure save logic
        dirName = "graphs/" + item[1].split(' ')[0]
        fileName = item[1].split(' ', 1)[1].replace(' ', '_') + "_build_num=" + str(item[3])
        if not os.path.exists(dirName):
                os.makedirs(dirName)
        plt.savefig(dirName + "/" + fileName, bbox_extra_artists=(lgd,), bbox_inches='tight')
	plt.clf()
