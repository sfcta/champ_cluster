"""  

  http://github.com/sfcta/cluster

  Cluster, Copyright 2013 San Francisco County Transportation Authority
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
  along with Cluster.  If not, see <http://www.gnu.org/licenses/>.
  
"""

from __future__ import with_statement
import os,sys,re,subprocess,time

# regex expression for expanding env variables
re_env = re.compile(r'%\w+%')
def expander(mo):
    return os.environ.get(mo.group()[1:-1], mo.group()[1:-1])

def exceptcatcher(type,value,traceback):
    print "\nHalted."

def createscript():
    script = open('clusterscript.s','w')
    return script

# Write final cluster cmds to bottom of script file
def finalfooter(numcmds,script):
    footer = "\nWait4Files Files="
    for x in range(1,numcmds+1):
        footer += "CHAMP"+str(x)+".script.end,"
    footer += " deldistribfiles=t,CheckReturnCode=t\n"

    script.write(footer)
    return

# Take a line from a jobset file, and build the script file from it
def parseline(line, script, numcmds, COMMPATH):
    # replace env variables
    line = re_env.sub(expander, line)

    # write multistep header (using COMMPATH)
    hdrtext = "DistributeMultistep ProcessID=\"CHAMP\",ProcessNum="+str(numcmds)+", CommPath='"+COMMPATH+"'\n"
    script.write(hdrtext)

    # read/output file contents
    script.write("; **DISPATCHER: reading "+line+"\n")

    try:
        with open(line) as f:
            for cmd in f:
              # lmz addition
            	cmd = re_env.sub(expander, cmd)
                script.write(cmd)
    except:
        print "ERROR: couldn't read",line
        raw_input("\n*** PRESS CTRL-C to quit. ***")

    # output multistep footer
    script.write("\nEndDistributeMultistep\n\n")
    return

# Now call the runtpp command!
def callcluster(numcmds,jset=""):
    print time.asctime()+":  Calling cluster with",numcmds,"command(s)"
    rtncode = 11

    outlog = open('clusterscript.log','a')
    outlog.write('\n--------------- START '+time.asctime()+' == '+jset)
    outlog.flush()
    
    try:
        rtncode = subprocess.call(["runtpp.exe",'clusterscript.s'], stdout=outlog, stderr=outlog)
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

    # create/overwrite new file dispatchscript.s
    script = createscript()

    # open jset file from cmdline
    jset = re_env.sub(expander, sys.argv[1])
    print   "\n------- Reading",jset,"--------"
    numcmds = 0

    # If argument is not a .jset script, send it on its way directly
    if (False == jset.endswith(".jset")):
        numcmds += 1
        parseline(jset,script,numcmds,COMMPATH)

    # Otherwise, assume it's a jset that needs to be processed
    else:
        # Read each line in the jset file and process it
        with open(jset,"r") as jsetfile:
            for line in jsetfile:
                # skip if it's just whitespace
                line = line.strip()
                if len(line)>0:
                    # truncate runtpp keyword if it's there
                    if (line.find("runtpp ")==0):
                        line = line[7:]
                    numcmds+=1
                    parseline(line,script,numcmds,COMMPATH)

                    # if we've filled up all our nodes, then spawn a process.
                    if (numcmds >= NODES): # time to spawn!
                        finalfooter(numcmds,script)
                        script.close()
                        callcluster(numcmds)
                        numcmds = 0
                        script = createscript()

    # Write multistep closing-block (using COMMPATH and NODES)
    if (numcmds > 0):
        finalfooter(numcmds,script)
        script.close()
        callcluster(numcmds,jset)
