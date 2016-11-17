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

	    #checking for all None values. If the values are all None, do not create a dataframe
            emptyValues = True
            for data in allData:
                if(not all(value is None for value in data[1:])):
                        emptyValues = False
            if(emptyValues):
                continue

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
    
    
def removeCloseValues(alignment, ticks, xmax, removeCloseValuesRatio):
    n = 0
    while n < (len(alignment) - 1):
        while (alignment[n+1] - alignment[n] < (xmax/removeCloseValuesRatio)):
            del alignment[n + 1]
            del ticks[n + 1]
	    if(n == len(alignment) -1):
                return alignment, ticks
        n = n + 1
    return alignment, ticks

def drawBigPlotsToDisplay(name,dataframes,keys):
    config = {}
    config['fontScale'] = 1.4;
    config['figSizeX'] = 10;
    config['figSizeY'] = 7.5;
    config['nbins'] = False;
    config['grid'] = True;
    config['title'] = True;
    config['convertValuesToThousandsAndMillions'] = False;
    config['displayPlot'] = True;
    config['savePlot'] = False;
    config['removeCloseValuesRatio'] = 16;
    plot(name, dataframes, config, keys);

def drawSmallPlotForPaper(name,dataframes,keys):
    config = {}
    config['fontScale'] = 1;
    config['figSizeX'] = 3;
    config['figSizeY'] = 2.25;
    config['nbins'] = True;
    config['nbinsX'] = 5;
    config['nbinsY'] = 4;
    config['grid'] = False;
    config['title'] = False;
    config['convertValuesToThousandsAndMillions'] = False;
    config['displayPlot'] = False;
    config['savePlot'] = True;
    config['removeCloseValuesRatio'] = 8;
    plot(name, dataframes, config, keys);

def plot(name,dataframes, config, keys):
    sns.set_context('notebook', font_scale=config['fontScale'])
    sns.set_style('whitegrid')
    lines = ['-', ':', '-', '--', '-', '-.', '-', '-', '-', '-', '-', '-']
    colors = sns.color_palette("Paired", n_colors=12)
    markers = ['', 's', '', 'o', '', 'D',  "", ">", "", "", "", ""]
    linewitdhs = [2, 3, 2, 3, 2, 3, 2, 2, 2, 2, 2, 2]
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
        labels = list(item[0].columns.values)
        fig = plt.figure(figsize=(config['figSizeX'], config['figSizeY']))
        ax1 = plt.gca()
        plt.ylabel(item[2])
        if(config['nbins']):
            plt.locator_params(nbins=config['nbinsX'], axis="x")
            plt.locator_params(nbins=config['nbinsY'], axis="y")
        plt.grid(config['grid'])

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
            new_tick_locations, ticks = removeCloseValues(item[5], item[4],xmax,config['removeCloseValuesRatio'])
            ax2.set_xticklabels(ticks)
            ax2.set_xticks(new_tick_locations)
            if(config['title']):
                plt.title(item[1] + " build_num=" + str(item[3]), y=1.12)
        else:
            #title in the regular position
            if(config['title']):
                plt.title(item[1] + " build_num=" + str(item[3]))

        xlabelextra = ""
        if(config['convertValuesToThousandsAndMillions']):
            #finding number of digits
            xmax = max(item[0].index.values)
            x = len(str(xmax).replace(".0", ""))
            divider = 1
            if (x >  6):
                divider = 1000000
                xlabelextra = " (Millions)"
            #         elif(x > 5):
            #             divider = 100000;
            #             xlabelextra = " (Hundred Thousands)"
            elif(x > 4):
                divider = 1000;
                xlabelextra = " (Thousands)"
            ax1.xaxis.set_major_formatter(mtick.FuncFormatter(lambda y, pos: ('%.f')%(y/divider)))


        ax1.set_xlabel(item[0].index.name + xlabelextra)

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

        if(config['savePlot']):
            #figure save logic
            dirName = "graphs/" + item[1].split(' ')[0]
            fileName = item[1].split(' ', 1)[1].replace(' ', '_') + "_build_num=" + str(item[3])
            if not os.path.exists(dirName):
                os.makedirs(dirName)
            plt.savefig(dirName + "/" + fileName, bbox_extra_artists=(lgd,), bbox_inches='tight')
        
        if(config['displayPlot']):
            plt.show()
        else:
            plt.close()
