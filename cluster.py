"""  

  http://github.com/sfcta/cluster

  Cluster, Copyright 2014 San Francisco County Transportation Authority
                            San Francisco, CA, USA
                            http://www.sfcta.org/
                            info@sfcta.org

  This file is part of cluster.

  Cluster is free software: you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation, either version 3 of the License, or
  (at your option) any later version.

  Cluster is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with Timesheet.  If not, see <http://www.gnu.org/licenses/>.
  
"""

from __future__ import with_statement
import os,sys,re,subprocess,time

# regex expression for expanding env variables
re_env = re.compile(r'%\w+%')
def expander(mo):
    return os.environ.get(mo.group()[1:-1], mo.group()[1:-1])

def exceptcatcher(type,value,traceback):
    print "\nHalted."

# Write final cluster cmds to bottom of script file
def writescript(numcmds,scripts):

    footer = "\nWait4Files Files="

    # write it now
    script = open('clusterscript.s','w')
    for nodenum in sorted(numcmds.keys()):
        script.write(scripts[nodenum])
        
        if len(numcmds[nodenum])>0:
            # output multistep footer
            script.write("\nEndDistributeMultistep\n\n")
            footer += "CHAMP"+str(nodenum)+".script.end,"
            
    footer += " deldistribfiles=t,CheckReturnCode=t\n"

    script.write(footer)
    return

# Take a line from a jobset file, and build the script file from it
def parseline(line, script, numcmds, COMMPATH, mynodenum):
    # replace env variables
    line = re_env.sub(expander, line)

    # add it
    numscripts = len(numcmds[mynodenum])
    numcmds[mynodenum].append(line)
            
    # write multistep header (using COMMPATH) if no other scripts yet
    if numscripts == 0:
        scripts[mynodenum] += "DistributeMultistep ProcessID=\"CHAMP\",ProcessNum=%d, CommPath='%s'\n" % (mynodenum, COMMPATH)

    # read/output file contents
    scripts[mynodenum] += "\n; **DISPATCHER: reading "+line+"\n"

    try:
        with open(line) as f:
            for cmd in f:
            	# lmz addition
            	cmd = re_env.sub(expander, cmd)
                scripts[mynodenum] += cmd
    except:
        print "ERROR: couldn't read",line
        raw_input("\n*** PRESS CTRL-C to quit. ***")

    # don't output multistep footer -- we'll do this later
    return

# Now call the runtpp command!
def callcluster(numcmds,jset="",grouped_jset=False):
    min_cmds = 9999
    max_cmds = 0
    num_nodes= 0
    num_scripts= 0
    for nodenum in sorted(numcmds.keys()):
        min_cmds = min(min_cmds, len(numcmds[nodenum]))
        max_cmds = max(max_cmds, len(numcmds[nodenum]))
        num_nodes += 1 if (len(numcmds[nodenum]) > 0) else 0
        num_scripts += len(numcmds[nodenum])
        
    print time.asctime()+":  Calling cluster with %d %scommands (%s each) on %d nodes" % \
        (num_scripts, "grouped " if grouped_jset else "",
         "%d - %d" % (min_cmds, max_cmds) if min_cmds != max_cmds else "%d" % min_cmds, 
         num_nodes)
    rtncode = 11

    outlog = open('clusterscript.log','a')
    outlog.write('\n--------------- START '+time.asctime()+' == '+jset)
    outlog.flush()
    
    try:
        rtncode = subprocess.call(["runtpp.exe",'clusterscript.s'], stdout=outlog, stderr=outlog, shell=True)
    except:
        print "Couldn't spawn runtpp.exe; is it installed?"
    
    outlog.write('\n--------------- DONE  '+time.asctime()+' == '+jset+'\n')
    outlog.flush()
    outlog.close()

    # Check TPP errorcode:  2 and above means fatal error
    if (rtncode >= 2):
        print "ERROR: Returned error code",rtncode
        raw_input("\n*** PRESS CTRL-C to quit. ***")

    print time.asctime()+":  Done.  "
    return

# ------------------------------------------------------------
# Main entry point

if (__name__ == "__main__"):
    NODES = int(os.getenv("NODES","8"))
    COMMPATH = os.getenv("COMMPATH","X:\\COMMPATH")

    # sys.excepthook = exceptcatcher  # remove this to get tracebacks on errors!!

    # will map nodenum -> [ list of script txt ]
    scripts = {}

    # open jset file from cmdline
    jset = re_env.sub(expander, sys.argv[1])
    print   "\n------- Reading",jset,"--------"
    
    # will map nodenum -> [ list of script names ]
    grouped_jset = False
    numcmds = {}
    for nodenum in range(1,NODES+1):
        scripts[nodenum]  = ""
        numcmds[nodenum] = []

    # If argument is not a .jset script, send it on its way directly
    if (not jset.endswith(".jset") and not jset.endswith(".gjset")):
        parseline(jset,scripts,numcmds,COMMPATH,1)

    # Otherwise, assume it's a jset that needs to be processed
    else:
        if jset.endswith(".gjset"): 
            grouped_jset = True
        mynodenum = 1
        
        # Read each line in the jset file and process it
        with open(jset,"r") as jsetfile:
            for line in jsetfile:
                # skip if it's just whitespace
                line = line.strip()
                
                # blank line for grouped jset means we can go to the next node
                if len(line)==0 and grouped_jset:
                    mynodenum += 1
                    if mynodenum > NODES: mynodenum = 1
                      
                if len(line)>0:                                    
                    # truncate runtpp keyword if it's there
                    if (line.find("runtpp ")==0):
                        line = line[7:]
                    parseline(line,scripts,numcmds,COMMPATH,mynodenum)
                    
                    # not-grouped jset: go to next node automatically
                    if not grouped_jset:
                        mynodenum += 1
                        if mynodenum > NODES: mynodenum = 1

    # time to spawn!
    writescript(numcmds,scripts)
    callcluster(numcmds,jset,grouped_jset)
