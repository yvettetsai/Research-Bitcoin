'''
Created on Mar 8, 2013

@author: YvetteTsai

FileName: BitcoinBlockScraping.py
Overview:
    This script scrap block data from blockexplorer.com and process
    it to compute user's behavior in using Bitcoin

Description:
    - Step 1: Scrap and process block data
    - Step 2: Obtain new addresses and lines need to be merged
    - Step 3: Merge lines and update accordingly to a new user vertice file
    - Step 4: Compute the new user edges file
    - Step 5: Calculate the user behavior's statistic

Input:
    - start block Id
    - end block Id
    - varioius of exisitng file names 
    - various of going to be outputed file names

'''

from bs4 import BeautifulSoup
from datetime import date, time, datetime
import urllib2, httplib, re, numpy
import numpy as np


#
# Scrap the block's html file from blockexplorer.com providing 
#   a blockID 
#
def connect_to_block(blockId):
    blckURL = "http://blockexplorer.com/b/" + str(blockId)
    catchError = False
    
    try:
        blckPage = urllib2.urlopen(blckURL)
    except urllib2.URLError, e:
        catchError = True
        print "Bid: " + str(blockId) + " " + str(e) + "\n"
        
    if (not catchError):
        blckSoup = BeautifulSoup(blckPage.read())
        return blckSoup
    else:
        return "error"


#
# - each line represents a transaction
#        - each column is separated by a tab
#        - each column means the following:
#            [0] Time Stamp in YEAR-MONTH-DAY HOUR:MINUTE:SECOND
#            [1] Transaction ID
#            [2] Transaction Fee
#            [3] Input Address [(address, amount)]
#            [4] Output Address [(address, amount)]
#        - if such transaction is a result of mining, column [3] is empty
#
def process_block(blockId, fin):
    fout = open("processed_block_" + str(blockId), "w+")
    
    blockS = BeautifulSoup(''.join(fin))
    
    rawTime = str(blockS.findAll("li")[3]).split("</sup>: ")[1].split("<")[0].strip()
    timeStamp = datetime.strptime(rawTime, '%Y-%m-%d %H:%M:%S')
       
    alltr = blockS.findAll("tr")
    
    for tr in alltr:
        trax = tr.findAll("td")
        
        if(len(trax) > 1):
            # transaction Id
            traxId = ((str(trax[0]).split(">")[1]).split("\"")[1]).split("/")[2].rstrip()
            
            # transaction fee
            traxFee = str(trax[1]).split(">")[1].split("<")[0].rstrip()

            # all input addresses in this transaction
            fromAdd = []
            fromAdd_List = trax[3].findAll("li")
            
            if("Generation" not in str(fromAdd_List)):
                for fa in fromAdd_List:
                    if("Unknown: " not in str(fa)):
                        add = str(fa).split("/address/")[1].split("\"")[0].rstrip()
                        amount = str(fa).split(" ")[2].split("<")[0].rstrip()
                        fromAdd.append((add,amount))
                    else:
                        amount = str(fa).split(" ")[1].split("<")[0].rstrip()
                        fromAdd.append(("unknown", amount))

            # all output addresses in this transaction 
            toAdd = []
            toAdd_List = trax[4].findAll("li")
            
            for ta in toAdd_List:
                if("Unknown: " not in str(ta)):
                    add = str(ta).split("/address/")[1].split("\"")[0].rstrip()
                    amount = str(ta).split(" ")[2].split("<")[0].rstrip()
                    toAdd.append((add,amount))
                else:
                    amount = str(ta).split(" ")[1].split("<")[0].rstrip()
                    toAdd.append(("unknown", amount))
            
            # output all transactions in such block
            fout.write(str(timeStamp) + "\t" + traxId + "\t" + traxFee + "\t" + str(fromAdd) + "\t" + str(toAdd) + "\n")
    fout.close()

#
# Reformat the list of [(add, btc)] into a real
# list in order to use all the operation provided 
# in the library
#
def fix(s):
    addPairList = []
    s = s.replace("[", "")
    s = s.replace("]", "")
    s = s.replace("'", "")
    if(")" in s):
        ls = s.split(")")
        for l in ls:
            if(len(l) > 1):
                l = l.split("(")[1]
                add = l.split(",")[0]
                btc = l.split(",")[1]
                addPairList.append((str(add), float(btc)))
                
    return addPairList


#
# Create the initial dictionary from existing file with collapsed address till
# 2011-07-13 from others. The {key:value} pair is defined as {addressID: lineNum}
# Input: fileName of collapsed address file
# Return: dictionary in the form {addressID: lineNum}  
#
def build_initDictionary(fileName):
    collapseDic = {}

    f = open(fileName, "r")
    lineNum = 0
    
    for line in f:
        lineNum += 1
        addList = (line.rstrip()).split("\t")
        # populate the dictionary with {address:lineNum}
        for add in addList:
            collapseDic.update({add: lineNum})
      
    return collapseDic, lineNum


#
# Update the existing dictionary with all the blocks we are interested in
#   output two files
#       - outputUnion_file: record all lines number should be merged
#       - outputNewAdd_file: recorad all new address should be add into
#           the exisiting dictionary
#
def update_dictionary(start_blockId, end_blockId, existing_vertices_file, outputUnion_file, outputNewAdd_file):
    # build the initial dictionary from exisiting user_vertices file
    #   and obtain the maximum line number
    myDict, maxLine = build_initDictionary(existing_vertices_file)
    
    # output file of set of line numbers should be unioned
    fUnion = open(outputUnion_file, "w+")

    # output file of new address - line number pair should 
    #   be added to the exisiting user_vertices file
    fnewAdd = open(outputNewAdd_file, "w+")

    for bid in range(start_blockId, end_blockId+1):
        
        fblock = open("processed_block_" + str(bid), "r+")
        count = 0
    
        for line in fblock:
            count += 1
            inputAddPairs = fix(line.rstrip().split("\t")[3])
            outputAddPairs = fix(line.rstrip().split("\t")[4])
        
            ######################################
            ### begins collapse_inputAdd_dict
            ######################################
            inputLineNumList = []
            inputRealLineNums = []
            inputLineNum = 0
            isNewAdd = True
            needUnion = False
    
            # obtain line number for each address in inputAddressList
            for add in inputAddPairs:        
                if(inputAddPairs != [] and "unknown" not in add[0]):
                    tempLineNum = myDict.get(add[0])
                    inputLineNumList.append(tempLineNum)
                    if(tempLineNum != None):
                        isNewAdd = False
                        inputRealLineNums.append(int(tempLineNum))
                    
            # if the address did not appear before, assign to a new line
            if(isNewAdd):
                inputLineNum = maxLine
                maxLine += 1
                
            else:
                inputRealLineNums = np.unique(inputRealLineNums)
            
                # if the address associates to multiple lines.
                # assign it to the smallest line number
                if(len(inputRealLineNums) > 1):
                    needUnion = True

                inputLineNum = np.min(inputRealLineNums)
                        
            ######################################
            ###  ends collapse_inputAdd_dict
            ######################################

            # if new collapsing information is obtained, record those collapsed line in separate file
            if(needUnion):
                for element in inputRealLineNums:
                    fUnion.write("%d\t" % element)
                fUnion.write("\n")
                
            for i in range(0, len(inputLineNumList)):
                if(inputLineNumList[i] == None):
                    myDict.update({inputAddPairs[i][0]:inputLineNum})
                    fnewAdd.write("%s\t%d\n" % (inputAddPairs[i][0], inputLineNum))
        
            for j in outputAddPairs:
                if("unknown" not in j[0]):
                    outputLineNum = myDict.get(j[0])
                    if(outputLineNum == None):
                        outputLineNum = maxLine                        
                        myDict.update({j[0]:outputLineNum})
                        fnewAdd.write("%s\t%d\n" % (j[0], outputLineNum))
                        maxLine += 1
        fblock.close()
    fUnion.close()
    fnewAdd.close()
    return maxLine

#
# This function repeated merge the line together until 
#   no further merging can be carried out. And then it return
#   the last version of merged line file
# This function take the outputUnion_file as parameter
#
def unioned_line(outputUnion_file):
    reRun = True
    firstRun = True
    index = 0
    while(reRun):
        reRun = False
        if(firstRun):
            f1 = open(outputUnion_file, "r")
            f2 = open(outputUnion_file, "r")
            firstRun = False
        else:
            f1 = open(outputUnion_file + str(index-1), "r")
            f2 = open(outputUnion_file + str(index-1), "r")

        linDic = {}
        lineNum = 0
        arr2D = []
        
        for l in f1:
            arr2D.append([])
        arr2D.append([])
        
        for line in f2:
            lineNum += 1
            ls = line.rstrip().split("\t")
            for ele in ls:
                val = linDic.get(ele)
                if(val != None):
                    reRun = True
                    break

            if(val == None):
                val = lineNum
            for ele in ls:      
                linDic.update({ele:val})
                arr2D[val-1] += [int(ele)]

        fout = open(outputUnion_file + str(index), "w+")
        for i in arr2D:
            if(len(i) > 0):
                npA = np.sort(np.unique(np.array(i)))
                for j in npA:
                    fout.write(str(j) + "\t")
                fout.write("\n")
        fout.close()
        f1.close()
        f2.close()
        index += 1
    return (index-1)


#
# This function update the existing vertice with the new addresses after the cut off date
# This function takes the following parameters,
#   existing_vertices_file: file name for the user_vertices file from previous researcher
#   outputNewAdd_file: file name for new address - line number pair
#   new_vertces_before_merge_file: file name of vertices after adding new addresses yet before merging 
#   maxLine: max line number of user
#
def update_vertices_helper(existing_vertices_file, outputNewAdd_file, new_vertices_before_merge_file, maxLine):
    addLine = []
    oldFile = existing_vertices_file
    newFile = outputNewAdd_file
    outputFile = new_vertices_before_merge_file
    
    for i in range(1, maxLine+1):
        addLine.append([])

    finit = open(oldFile, "r")
    counter = 0
    for line in finit:
        counter += 1
        addLine[counter - 1] = line.rstrip()

    fnew = open(newFile, "r")
    for line in fnew:
        add = line.split("\t")[0]
        lineNum = int(line.rstrip().split("\t")[1])
        if(addLine[lineNum - 1] != []):
            addLine[lineNum - 1] = str(addLine[lineNum - 1]) + "\t" + add
        else:
            addLine[lineNum - 1] = add

    foutput = open(outputFile, "w+")
    
    for element in addLine:
        s = str(element).replace("[", "").replace("]", "").rstrip()
        foutput.write(s + "\n")
        

#
# This function update the user_vertices file merging lines if necessary and
#   return the file name for final merged_lines
# This function takes the following parameters,
#   existing_vertices_file: file name for the user_vertices file from previous researcher
#   outputNewAdd_file: file name for new address - line number pair
#   outputUnion_file: file name of lines need to be merged together
#   new_vertces_before_merge_file: file name of vertices after adding new addresses yet before merging 
#   new_vertices_after_merge_file: file name of vertices after adding new addresses after merging
#   maxLine: max line number of user
#
def update_vertices_file(existing_vertices_file, outputNewAdd_file, outputUnion_file, new_vertices_before_merge_file, new_vertices_after_merge_file, maxLine):
    addLine = []
    update_vertices_helper(existing_vertices_file, outputNewAdd_file, new_vertices_before_merge_file, maxLine)
    oldFile = new_vertices_before_merge_file
    print "\tStart unioning..."
    newFile = outputUnion_file + str(unioned_line(outputUnion_file))
    print "\tEnd unioning..."
    outputFile = new_vertices_after_merge_file

    lineDic = {}
    
    fnew = open(newFile, "r")
    for line in fnew:
        val = line.rstrip().split("\t")[0]
        lss = line.rstrip().split("\t")
        for ls in lss:
            lineDic.update({int(ls):val})

    for i in range(1, maxLine+1):
        addLine.append([])

    finit = open(oldFile, "r")
    counter = 0
    for line in finit:
        counter += 1
        newVal = lineDic.get(counter)
        if(newVal != None):
            addLine[int(newVal) - 1].append(line.rstrip())
        else:
            addLine[counter - 1].append(line.rstrip())
    
    foutput = open(outputFile, "w+")
    
    lc = 0
    for element in addLine:
        lc += 1
        if(element != []):
            foutput.write(str(element[0]))
            for e in element[1:]:
                foutput.write("\t" + str(e))
            foutput.write("\n")
        else:
            foutput.write("\n")
    
    return newFile, lc


#
# This function output the user_edges file (combining both old and new). An
#   edge (transaction) is only recorded if it is between two different user. 
#   Then the function returns the max line number of user.
# This function takes the following input parameters,
#   start_blockId: 
#   end_blockId:
#   final_vertice_file: file name for final user_vertices file
#   outputTrax_file: file name for final user_edges file
#   collapsed_lines_file: the final version of collapsed line file
#   old_edges_file: file name for existing user_edges file
#
def output_user_edges(start_blockId, end_blockId, final_vertices_file, outputTrax_file, collapsed_lines_file, old_edges_file):

    myDict, maxLine = build_initDictionary(final_vertices_file)
    foutput = open(outputTrax_file, "w+")

    lineDic = {}
    for line in open(collapsed_lines_file, "r"):
        lss = line.rstrip().split("\t")
        val = int(lss[0])
        for ls in lss:
            lineDic.update({int(ls):val})
    
    # update the exisiting user_edge file to the newest line number    
    for line in open(old_edges_file, "r"):
        i = int(line.rstrip().split("\t")[0])
        o = int(line.rstrip().split("\t")[1])
        btc = line.rstrip().split("\t")[2]
        t = datetime.strptime((line.rstrip().split("\t")[3]).rstrip(), '%Y-%m-%d-%H-%M-%S')

        iNew = lineDic.get(i)
        oNew = lineDic.get(o)

        if(iNew == None):
            foutput.write("I" +"\t"+ str(i) +"\t"+ str(btc) +"\t"+ str(t) +"\n")
        else:
            foutput.write("I" +"\t"+ str(iNew) +"\t"+ str(btc) +"\t"+ str(t) +"\n")

        if(oNew == None):
            foutput.write("O" +"\t"+ str(o) +"\t"+ str(btc) +"\t"+ str(t) +"\n")
        else:
            foutput.write("O" +"\t"+ str(oNew) +"\t"+ str(btc) +"\t"+ str(t) +"\n")
    print "\tDone updating exisiting (old) user_edge files"
    

    for bid in range(start_blockId, end_blockId+1):
  
        fblock = open("processed_block_" + str(bid), "r+")
        count = 0
    
        for line in fblock:
            count += 1
            timeStamp = datetime.strptime((line.split("\t")[0]).rstrip(), '%Y-%m-%d %H:%M:%S')
            inputAddPairs = fix(line.rstrip().split("\t")[3])
            outputAddPairs = fix(line.rstrip().split("\t")[4])
            
            inputLineNumList = []
            inputRealLineNums = []
            inputLineNum = 0
            isNewAdd = True
            
            for add in inputAddPairs:   
                if(inputAddPairs != [] and "unknown" not in add[0]):
                    tempLineNum = myDict.get(add[0])
                    inputLineNumList.append(tempLineNum)
                    if(tempLineNum != None):
                        isNewAdd = False
                        inputRealLineNums.append(int(tempLineNum))
            
            if(not isNewAdd):
                inputLineNum = np.min(np.unique(inputRealLineNums))
                
            if(isNewAdd and len(np.unique(inputRealLineNums)) > 1):
                print "in blocks " + str(bid) + " line " + str(count) + " need further union"
            
            for i in range(0, len(inputLineNumList)):
                if(inputLineNumList[i] != None):
                    foutput.write("%s\t%d\t%s\t%s\n" % ("I", inputLineNum, inputAddPairs[i][1], str(timeStamp)))
            
            if(inputAddPairs != []):
                for j in outputAddPairs:
                    if("unknown" not in j[0]):
                        outputLineNum = myDict.get(j[0])
                        if(outputLineNum != None and (int(outputLineNum) not in inputLineNumList)):
                            foutput.write("%s\t%d\t%s\t%s\n" % ("O", outputLineNum, j[1], str(timeStamp))) 
            else:
                for j in outputAddPairs:
                    if("unknown" not in j[0]):
                        outputLineNum = myDict.get(j[0])
                        foutput.write("%s\t%d\t%s\t%s\n" % ("M", outputLineNum, j[1], str(timeStamp))) 
        fblock.close()    
    foutput.close()

    return maxLine


#
# The function compute the following statistic for each line of user,
#   user's line number
#   num outgoing transactions (user act as input)
#   num incoming transactions (user act as output)
#   average bitcoin of outgoing transactions 
#   average bitcoin of incoming transactions
#   start of bitcoin use (in days from January 3, 2009)
#   length of bitcoin use (in day, rounded up)
#   number transactions/day 
#   number of addresses
# This function takes the following input:
#   maxLine: max num of user in line number
#   outputTrax_file: file name for user_edge file (i.e. all transaction)
#   new_vertices_after_merge_file: file name for user_vertices_file
#   final_user_data_file: file name for final output user file
#
def user_data(maxLine, outputTrax_file, new_vertices_after_merge_file, final_user_data_file):
    
    MAX_USER = maxLine
    BITEND = datetime(2012, 10, 31, 23, 59, 59)
    tempOLD = datetime(2012, 11, 1)
    tempNEW = datetime(2009, 1, 2)
    
    # UD:
    # 0 num outgoing transactions (int) (user is sender/trax input)
    # 1 num incoming transactions (int) (user is receiver/trax output)
    # 2 total outgoing transactions amount (float)
    # 3 total incoming transaction amount (float)
    # 4 oldest transaction date (datetime)
    # 5 newest transaction date (datetime)
    # 6 number of addresses
    # 7 is the miner for the block
    UD = {}
    for i in range(MAX_USER+1)[1:]:
        UD[i] = []

    for line in open(outputTrax_file, "r"):
        tok = line.strip().split("\t")
        x = int(tok[1]) 
        bc = float(tok[2])
        t = datetime.strptime(tok[3],"%Y-%m-%d %H:%M:%S")
        if(True):
            # double check if correct: input transactions are == sender (transmitting)
            if ("I" in tok[0]):
                if (UD[x] == []):
                    UD[x] = [1,0,bc,0,t,t,0,0]
                else:
                    UD[x][0] += 1
                    UD[x][2] += bc
                    if t < UD[x][4]:
                        UD[x][4] = t
                    if t > UD[x][5]:
                        UD[x][5] = t
    
            # double check if correct: output transactions are == receiver (reciving)
            elif ("O" in tok[0]):
                if (UD[x] == []):
                    UD[x] = [0,1,0,bc,t,t,0,0]
                else:
                    UD[x][1] += 1
                    UD[x][3] += bc
                    if t < UD[x][4]:
                        UD[x][4] = t
                    if t > UD[x][5]:
                        UD[x][5] = t
            elif ("M" in tok[0]):
                if (UD[x] == []):
                    UD[x] = [0,0,0,bc,t,t,0,1]
                else:
                    UD[x][7] += 1
                    UD[x][3] += bc
                    if t < UD[x][4]:
                        UD[x][4] = t
                    if t > UD[x][5]:
                        UD[x][5] = t

    print "\tDone going through user_edges file"

    count = 0 
    for line in open(new_vertices_after_merge_file,"r"):
        count += 1
        ls = line.rstrip().split("\t")
        if(line != "\n" and (not (len(ls) <= 1 and ls[0] == ""))):
            tok = line.strip().split("\t")
            if(UD[count] == []):
                if(count > 926615):
                    print str(count)
                    UD[count] = [0,0,0.0,0.0,tempOLD,tempNEW,len(tok),0]
                else:
                    UD[count] = [0,0,0.0,0.0,tempOLD,tempNEW,len(tok),1]
            else:
                UD[count][6] = len(tok)
    
    
    fout = open(final_user_data_file, 'w')
    c = 0
    for i in UD.keys():
        ud = UD[i]
        if(ud != []):
            c += 1
            sent = ud[0]
            receive = ud[1]
            nt = sent + receive
    
            # average bitcoin per receiver/sender transaction
            asent, areceive = 0, 0
            if sent > 0: asent = ud[2]/sent
            if receive > 0: areceive = ud[3]/receive
        
            fout.write("%d\t%d\t%d\t%f\t%f" % ((i), sent, receive, asent, areceive))
    
            #calculating period of use
            useperiod = ud[5] - ud[4]
            startstring = ud[4].strftime('%Y-%m-%d')
            endstring = ud[5].strftime('%Y-%m-%d')
    
            # always rounding up, thus +1
            usedays = useperiod.days + 1

            addresses = ud[6]
    
            fout.write("\t%s\t%s\t%d\t%f\t%i\t%d\n" % (startstring, endstring, usedays, float(nt)/usedays, addresses, int(ud[7])))
    
    fout.close()
    print "total line written to user_data file " + str(c)


###############################################################################
###                                                                         ###
###                             Main Method                                 ###
###                                                                         ###
###############################################################################

if __name__ == '__main__':        
    
    ########################################
    ### Input parameters for the script  ###
    ########################################

    # the first and last block Id want to scrap
    start_blockId = 136166
    end_blockId = 205918

    # file name of exisiting user_vertices from others
    existing_vertices_file = "user_vertices_2011-07-13.txt"
    # file name of exisiting user_edges from others
    old_edges_file = "user_edges_2011-07-13.txt"
    # file name of lines need to be merged (unioned)
    outputUnion_file = "20130324-NEW_union_following_lines_"
    # file name of new add-line pair (comparing with exisitng user_vertice file)
    outputNewAdd_file = "20130324-NEW_new_address-line_pair"
    # file name of updated user_vertices file before the merging
    new_vertices_before_merge_file = "20130324-NEW_user_vertices_BEFORE_union"
    # file name of updated user_vertices file after the merging
    new_vertices_after_merge_file = "20130324-NEW_user_vertices_AFTER_union"
    # file name of updated user_edges
    outputTrax_file = "20130324-NEW_user_edges"
    # file name of user_data for each line of user
    final_user_data_file = "20130324-NEW_NEW_user_data"


    ###############################################
    ### Step 1. Scrap and process block data    ###
    ###############################################
    print "Starting Step 1 ..."
    ts = datetime.now()
    
    for blockId in range(start_blockId, end_blockId + 1):
        # scarp the block html file
        s = str(connect_to_block(blockId))
        if(s == "error"):
            print "ERROR-BLOCK-" + str(blockId)
        else:
            # process raw block html file into desired format
            process_block(blockId, s)

        print "done_" + str(blockId) 
    
    print "End Step 1 ... " + str(datetime.now() - ts)
    ##################################################################
    ### Step 2. Produce two output files to update user_vertices   ###
    ##################################################################

    # return the maximum line number recored in outputNewAdd_file and
    # {line number: address} dictionary
    print "Starting Step 2 ... "
    ts = datetime.now()
    maxLine = update_dictionary(start_blockId, end_blockId, existing_vertices_file, outputUnion_file, outputNewAdd_file)
    print "End Step 2 ... " + str(datetime.now() - ts)

    ##########################################################################
    ### Step 3. Merging all those lines and produce new_user_vertices file ###
    ##########################################################################

    # update vertices file to including all new first-seen addresses
    # mering all the line that need to be merged according to transaction input rule
    # return the final merging line files and total line of users
    print "Starting Step 3 ..."
    ts = datetime.now()
    collapsed_lines_file, maxLine = update_vertices_file(existing_vertices_file, outputNewAdd_file, outputUnion_file, new_vertices_before_merge_file, new_vertices_after_merge_file, maxLine)
    print "End Step 3 ... " + str(datetime.now() - ts)
    
    ##############################################
    ### Step 4. Produce a new_user_edges file  ###
    ##############################################

    # Create the user_edge file according to updated after_merging_user_vertices
    # and again, return the maximum number of lines
    print "Starting Step 4 ... "
    ts = datetime.now()
    maxLine = output_user_edges(start_blockId, end_blockId, new_vertices_after_merge_file, outputTrax_file, collapsed_lines_file, old_edges_file)
    print "End Step 4 ... " + str(datetime.now() - ts)
    #print maxLine
    
    ##############################################
    ### Step 5. Produce user_data_file      ###
    ##############################################
    print "Starting Step 5 ..."
    ts = datetime.now()
    # calculate the statistic for each user
    user_data(7162490, outputTrax_file, new_vertices_after_merge_file, final_user_data_file)
    print "End Step 5 ... " + str(datetime.now() - ts)

