#!/usr/bin/env python


import sys;
sys.path.append('/usr/local/lib/python2.7/dist-packages')
import pandas as pd;
import pprint;
import locale;
import models;
import ConfigParser;
import numpy as np;
import matplotlib;
import matplotlib.pyplot as plt;
import os;
from peewee import *;
import seaborn as sns
import smallPlot
sns.set_context('notebook', font_scale=1.4)
sns.set_style('whitegrid')

colorList = ["#E6E600", "#01DF01", "#FF4000", "#5882FA", "#8A0886", "#FAAC58", "#A9D0F5", "#81F781"]   
db = None

locale.setlocale(locale.LC_ALL, 'en_US.utf8')

def getDataframesDictionary(configFile):
    global db
    graphsToPlot = {}
    myDefaults = {'axis-sql':None, 'secondaryXLabel':None}
    parser = ConfigParser.ConfigParser(defaults=myDefaults)
    parser.read(configFile)

    prefix = "query-"
    numSections = len(parser.sections())

    for qNum in range (1, numSections+1):
        sectionN = prefix + str(qNum)
        
        chartType = parser.get(sectionN, 'type')
        chartName = parser.get(sectionN, 'name')
        xlabel = parser.get(sectionN, 'xlabel')
        ylabel = parser.get(sectionN, 'ylabel')
        args = parser.get(sectionN, 'sql').split("\n")
        
        secondaryAxisSql = parser.get(sectionN, 'axis-sql')
        secondaryXLabel = parser.get(sectionN, 'secondaryxLabel')
                
        s = filter(None, args)
        sql = " ".join(s)   
        
        if chartType in graphsToPlot:
            graphsToPlot[chartType].update({chartName : {"sql":sql, "xlabel":xlabel, "ylabel":ylabel, "axis-sql":secondaryAxisSql, "secondaryXLabel": secondaryXLabel}})
        else:
            graphsToPlot.update({chartType : {chartName : {"sql":sql, "xlabel":xlabel, "ylabel":ylabel, "axis-sql":secondaryAxisSql, "secondaryXLabel": secondaryXLabel}}})

    dataframes = {}
    sqls = graphsToPlot["line"]
            
#     pprint.pprint(graphsToPlot["line"], width=1)

    dictByColors = {}
    lineColorIndex = 0    

    for chartName,chartDesc in sqls.iteritems():
        for build_num in range(3, 20):
            xlabel = chartDesc["xlabel"]
            ylabel = chartDesc["ylabel"]
            query = chartDesc["sql"]
            query = query.replace("e.build_num=0", "e.build_num=" + str(build_num))
            res = db.execute_sql(query)
            
            if res.rowcount == 0:
#                 print "Empty result set for ", chartName, " build_num=", build_num
                continue
    
            secondaryXTicks = None
            secondaryXAlignment = None
            secondaryXLabel = None
            if chartDesc["axis-sql"] is not None:
                axisQuery = chartDesc["axis-sql"]
                axisRes = db.execute_sql(axisQuery)
                secondaryAxes = list(axisRes.fetchall())
                secondaryXTicks = [x[1] for x in secondaryAxes]
                secondaryXAlignment = [x[0] for x in secondaryAxes]
                secondaryXLabel = chartDesc["secondaryXLabel"]
    

            resDesc = res.description
            columns = [x[0] for x in resDesc]
            columns[0] = xlabel
            allData = list(res.fetchall())

            df = pd.DataFrame.from_records(allData, columns=columns, index=xlabel)
            df.index = df.index.astype(float)
            chartName = chartName.replace("_", " ")
            
            if chartName in dataframes:
                dataframes[chartName].append([df, chartName, ylabel, build_num, secondaryXTicks, secondaryXAlignment, secondaryXLabel])
            else:
                dataframes[chartName] = [[df, chartName, ylabel, build_num, secondaryXTicks, secondaryXAlignment, secondaryXLabel]]

    return dataframes
        

def runConfig(configFile):
    global db
    db = models.BaseModel._meta.database
    db.connect()
    dataframes = getDataframesDictionary(configFile)
    db.close()
    return dataframes
    
    
def removeCloseValues(alignment, ticks, xmax):
    n = 0
    while n < (len(alignment) - 1):
        while (alignment[n+1] - alignment[n] < xmax/16):
            del alignment[n + 1]
            del ticks[n + 1]
	    if(n == len(alignment) -1):
                return alignment, ticks
        n = n + 1
    return alignment, ticks

def drawGraph(name,dataframes):
    sns.set_context('notebook', font_scale=1.4)
    sns.set_style('whitegrid')
    keys = ['RGS', 'RG', 'OGS', 'OG', 'HGS', 'HG', 'VES', 'VE', 'RG_E2D', 'OG_E2D', 'HG_E2D']
    lines = ['-', ':', '-', '--', '-', '-.', '-', '-', '-', '-', '-']
    colors = sns.color_palette("Paired", n_colors=11)
    markers = ['', 's', '', 'o', '', 'D',  "", ">", "", "", ""]
    linewitdhs = [2, 3, 2, 3, 2, 3, 2, 2, 2, 2, 2]
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
        fig = plt.figure(figsize=(10, 7.5))
        ax1 = plt.gca()
        ax1.set_xlabel(item[0].index.name)
        plt.ylabel(item[2])
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
            plt.title(item[1] + " build_num=" + str(item[3]), y=1.12)
        else:
            #title in the regular position
            plt.title(item[1] + " build_num=" + str(item[3]))

        #sorting the legends to match the order of the graph
        handles, labels = ax1.get_legend_handles_labels()
        #filtering out the none values
        lastValues = filter(None, item[0].tail(1).values[0])
        legends = zip(*sorted(zip(lastValues, labels, handles), key=lambda t: t[0], reverse=True))
        legends.pop(0)
        labels, handles = legends
        lgd = plt.legend(handles, labels, bbox_to_anchor=(1.02, 0.60), loc=2, borderaxespad=0.)

        #setting up the maximum of x axis to the maximum value
        xmax = max(item[0].index.values)
        ax1.set_xlim(xmin=0, xmax=xmax)
	plt.show()

