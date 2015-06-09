#!/usr/bin/env python

import sys;
sys.path.insert(0, './driverUtils')
import os;
import locale;
import traceback;
import collections;
import models;
import configparser;
import numpy as np;
import matplotlib;
matplotlib.use('Agg') #force matplotlib to not use any Xwindows backend
import matplotlib.pyplot as plt;
from peewee import *;

colorList = ["#E6E600", "#01DF01", "#FF4000", "#5882FA", "#8A0886", "#FAAC58", "#A9D0F5", "#81F781", "#FA5858", "#FA5882", "#F5BCA9", "#01DFA5", "#F4FA58",
            "#A9F5A9", "#81DAF5", "#82FA58"] #add more html colors to this if not enough     
graphsToPlot = {}
#stratList = ["None", "CanonicalRandomVertexCut", "EdgePartition2D", "NaiveTemporal", "ConsecutiveTemporal", "HybridRandomTemporal", "Hybrid2DTemporal"]
#graphs = ['SG', 'SGP', 'MG']
#numParts = [8, 16, 32]
#allQueries = []

locale.setlocale(locale.LC_ALL, 'en_US.utf8')

def gen_line_graphs(saveDir):
    sqls = graphsToPlot["line"]
        
    dictByColors = {}
    lineColorIndex = 0    

    for chartName,chartDesc in sqls.iteritems():
        xlabel = chartDesc["xlabel"]
        ylabel = chartDesc["ylabel"]
        query = chartDesc["sql"]

        res = db.execute_sql(query)
        resDesc = res.description
        legendTitle = resDesc[0][0]

        plt.figure(0, figsize=(12,10))
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        ax = plt.gca()
        ax.set_autoscale_on(False)

        legendKeys = {}
        maxValue = -1
        xPoints = []

        #get column headings from res starting at the second column
        for desc in resDesc[1::]:
            xPoints.append(int(desc[0]))

        print "[status]: Plotting graph for", chartName
        for r in res.fetchall():
            lineName = r[0] #line name is expected to be the first argument
            yPoints = r[1::]
        
            print "lineName:", lineName, "-- points:", yPoints
          
            groupMax = max(yPoints)
            maxValue = max(maxValue, groupMax) #set new max

            if not lineName in dictByColors:
                dictByColors.update({lineName : colorList[lineColorIndex]}) 
                lineColorIndex += 1 

            lineColor = dictByColors[lineName] #get color to use for line based on lineName
            p1 = plt.scatter(xPoints, yPoints, color=lineColor, s=30) #make scatter plot
            plt.plot(xPoints, yPoints, color=lineColor, linewidth=3.5) # add connect points in the plot

            #update plot legend
            legendKeys.update({lineName : p1})

        #collect lines from the plot
        lineColors = legendKeys.keys()
        lines = legendKeys.values()

        #set legend and axes
        pltLegend = plt.legend(lines, lineColors, title=legendTitle, fontsize=10)

        xtick = 5
        ytick = 20 #this is in seconds

        maxX = max(xPoints) + 3
        plt.xticks(np.arange(0, 35, xtick))
        plt.xlim(0, maxX) 
        maxY = maxValue + (ytick * 3)
        plt.ylim(0, maxY)
        plt.yticks(np.arange(0, maxY, ytick))

        #save generated plot
        if not saveDir.endswith("/"): #error checking
            saveDir += "/"
       
        pltName = saveDir + "plot-" + chartName + ".png"
        print "Plot name:", pltName, "\n" 
        
        plt.savefig(pltName)
        plt.clf() #clear figure
        

def parse_config(configFile):
    global grapsToPlot

    parser = configparser.ConfigParser()
    parser.read(configFile)

    prefix = "query-"
    numSections = len(parser.sections())

    for qNum in range (1, numSections+1):
        sectionN = prefix + str(qNum)
        
        chartType = parser[sectionN]['type']
        chartName = parser[sectionN]['name']
        xlabel = parser[sectionN]['xlabel']
        ylabel = parser[sectionN]['ylabel']
        args = parser[sectionN]['sql'].split("\n")
        s = filter(None, args)
        sql = " ".join(s)   
 
        #print "Type", qNum, ":", chartType
        #print "SQL", qNum, ":", sql
        #print "\n"
    
        if chartType in graphsToPlot:
            graphsToPlot[chartType].update({chartName : {"sql":sql, "xlabel":xlabel, "ylabel":ylabel}})
        else:
            graphsToPlot.update({chartType : {chartName : {"sql":sql, "xlabel":xlabel, "ylabel":ylabel}}})


def run(configFile, saveDir):
    parse_config(configFile)
    gen_line_graphs(saveDir)

if __name__ == "__main__":
    if(not len(sys.argv) > 2):
        print ("Usage: <path-to-graphs-gen.config> <dir-to-save-plots>")
        exit();

    db = models.BaseModel._meta.database
    arg1 = sys.argv[1]
    arg2 = sys.argv[2]
    run(arg1, arg2);

