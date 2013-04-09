import re, os, sys, csv
import string
import xml.etree.ElementTree#imported for py2exe to work
from xml.etree import cElementTree as ET
import numpy as N
import time
import yasdbf_main

class XT_RESULTS:
    def __init__(self, fileName = None,  parseFile = True,  evalue_cutoff = None,  ppm_cutoff = None, getFlanking = False):
        '''
        Parses an X!Tandem output file and places results in a dictionary named dataDict
        Parameters are self explanitory...
        getFlanking is the exception where the parsing will also include the flanking
        amino acids in the peptide sequence stored in the dictionary.
        
        Apparently the scan number assigned by XTandem is not related to the actual scan number in the file.
            --Need to account for this
        '''
        t1 = time.clock()
        self.fileName = fileName

        if evalue_cutoff:
            self.evalue_cutoff = evalue_cutoff
        else:
            self.evalue_cutoff = 1

        if ppm_cutoff:
            self.ppm_cutoff = ppm_cutoff
        else:
            self.ppm_cutoff = 10000

        if fileName:
            self.fileName = fileName
        
        pepID = []
        #pepID_modded = []
        proID = []
        pepStart = []
        pepStop = []

        ppm_error = []
        theoMZ = []
        scanID = []#this is the ID XTandem assigns
        #~ scanNum = []#this is the location in the file...
        scanIntensity = []
        pro_eVal=[]
        pep_eValue=[]
        charge = []
        hScore = []
        nextScore = []
        pepLen = []
        deltaH = []
        sumIntensity = []
        intensityFactor = []
        descript = []
        fragXVals = []
        fragYVals = []
        protLen = []
        self.dataLen = 0
        self.dataDict = {}

        if parseFile:
            pttrn = re.compile('.*id=(\d+)\s.*')
            tree = ET.parse(fileName)
            r = tree.getroot()
            groups = r.getchildren()

            n = 0
            m = 0
            for group in groups:
                if group.get('type') != 'no model obtained':
                    tempMaxI = group.get('maxI')
                    tempSumI = group.get('sumI')
                    tempFactor = group.get('fI')
                    
                    if tempMaxI != None and tempSumI != None and tempFactor != None:
                        curIntensity = N.float(tempMaxI)
                        cur_sumI = N.float(tempSumI)
                        cur_fI = N.float(tempFactor)
                    else:
                        curIntensity = 0.0
                        cur_sumI = 0.0
                        cur_fI = 0.0
                    for protein in group.findall('protein'):
                        cur_scan  = protein.get('id')
                        cur_protID = protein.attrib['label']
                        cur_pro_eVal = N.float(protein.attrib['expect'])
                        for peptide in protein.findall('peptide'):
                            curLen = len(peptide.text)#this is actually the length of the protein...blame BIOML
                            curPro = peptide.text
                            curPro = curPro.strip('\n')
                            curPro = curPro.strip('\t')
                            curPro = curPro.strip(' ')
                            
                            for domain in peptide.findall('domain'):
                                cur_pepSeq = domain.attrib['seq']
                                cur_seqStart = N.int(domain.attrib['start'])
                                cur_seqStop = N.int(domain.attrib['end'])                                
                                if getFlanking:
                                    pepSeqList = []
                                    for origAA in cur_pepSeq:
                                        pepSeqList.append(origAA)
                                   
                                    for v, aa in enumerate(domain.findall('aa')):
                                        modVal = aa.get('modified')
                                        modAA = aa.get('type')
                                        modPos = N.int(aa.get('at'))-cur_seqStart #+positionShift

                                        if '-' in modVal:#Case where it is a subtraction
                                            pepSeqList[modPos] = modAA+modVal
                                        else:
                                            if len(pepSeqList[modPos])==1:
                                                pepSeqList[modPos] = modAA+"+"+modVal
                                            else:
                                                newModVal = float(modVal) + float(pepSeqList[modPos][2:])
                                                pepSeqList[modPos] = modAA+'+'+str(newModVal)
                                        
                                        #cur_pepSeq_modded = ''.join(pepSeqList)
                                        cur_pepSeq = ''.join(pepSeqList)

                                    preFlank = domain.attrib['pre'][-1]
                                    postFlank = domain.attrib['post'][0]
                                    if preFlank == '[' or preFlank == '\t' or preFlank == '\n':
                                        '''
                                        Used to denote the start or stop of the sequence
                                        '''
                                        preFlank = '*'
                                        
                                    if postFlank == ']' or postFlank == '\t' or postFlank == '\n':
                                        postFlank = '*'
                                    cur_pepSeq = preFlank + '.' + cur_pepSeq + '.' + postFlank
                                    #cur_pepSeq_modded = preFlank + '.' + cur_pepSeq_modded + '.' + postFlank
                                
                                cur_eValue = N.float(domain.attrib['expect'])
                                cur_hscore = N.float(domain.attrib['hyperscore'])
                                cur_nextscore = N.float(domain.attrib['nextscore'])
                                cur_deltaH = cur_hscore - cur_nextscore
                                cur_mzTheor = N.float(domain.get('mh'))
                                cur_ppm = 1e6*(N.float(domain.get('delta')))/cur_mzTheor
                                if cur_eValue < self.evalue_cutoff and abs(cur_ppm) < self.ppm_cutoff:
                                    ppm_error.append(N.float(cur_ppm))
                                    theoMZ.append(cur_mzTheor)
                                    scanID.append(int(cur_scan.split('.')[0]))
                                    pro_eVal.append(cur_pro_eVal)
                                    proID.append(cur_protID)
                                    pep_eValue.append(cur_eValue)
                                    pepID.append(cur_pepSeq)
                                    #pepID_modded.append(cur_pepSeq_modded)
                                    if getFlanking:
                                        pepLen.append(len(cur_pepSeq.split('.')[1]))#need to remove flanking aa's
                                    else:
                                        pepLen.append(len(cur_pepSeq))
                                    hScore.append(cur_hscore)
                                    nextScore.append(cur_nextscore)
                                    sumIntensity.append(cur_sumI)
                                    intensityFactor.append(cur_fI)
                                    deltaH.append(cur_deltaH)
                                    scanIntensity.append(curIntensity)
                                    protLen.append(curLen)
                                    pepStart.append(cur_seqStart)
                                    pepStop.append(cur_seqStop)
                                    for subGroup in group.getchildren():
                                        if subGroup.get('label') == "fragment ion mass spectrum":
                                            fragText, fragInfo = subGroup.getchildren()
                                            tempStr = 'scan '+str(scanID[-1])
                                            descript.append(fragText.text.strip())
                                            for fragElem in fragInfo.getchildren():                                                    
                                                if fragElem.get('type') == 'charge':
                                                    charge.append(N.int(fragElem.text))
                                                if 'Xdata' in fragElem.tag:
                                                    xStr = fragElem[0].text
                                                    strXSplit = xStr.split('\n')
                                                    tempXStr = ''
                                                    for xStr in strXSplit:
                                                        tempXStr+=' '#needed so that sequence joins without decimals
                                                        tempXStr+=xStr
                                                    fragXVals.append(tempXStr)#need to split because there are the return characters
                                                elif 'Ydata' in fragElem.tag:
                                                    yStr = fragElem[0].text
                                                    strYSplit = yStr.split('\n')
                                                    tempYStr = ''
                                                    for yStr in strYSplit:
                                                        tempYStr+=' '
                                                        tempYStr+=yStr
                                                    fragYVals.append(tempYStr)

            '''
            So I'm not convinced that this is the best way.
            Perhaps a list of individual dictionaries would be more efficient
            But I'm needed this thing to work yesterday!
            '''

            t2 = time.clock()
            print "Initial Read Time (s): ",(t2-t1)
            self.iterLen = len(scanID)
            scanOrder = N.array(scanID).argsort()
            if len(pepID) != 0:
                self.dataDict = {
                    'pepID': N.array(pepID)[scanOrder],
                    #'pepID_modded': N.array(pepID_modded)[scanOrder],
                    'descript': N.array(descript)[scanOrder],
                    'charge': N.array(charge)[scanOrder],
                    'pep_eVal' : N.array(pep_eValue)[scanOrder],
                    'scanID' : N.array(scanID)[scanOrder],
                    'ppm_error':N.array(ppm_error)[scanOrder],
                    'theoMZ':N.array(theoMZ)[scanOrder],
                    'hScore':N.array(hScore)[scanOrder],
                    'nextScore':N.array(nextScore)[scanOrder],
                    'sumIntensity':N.array(sumIntensity)[scanOrder],
                    'intensityFactor':N.array(intensityFactor)[scanOrder],
                    'pepLen':N.array(pepLen)[scanOrder],
                    'proID':N.array(proID)[scanOrder],
                    'pro_eVal':N.array(pro_eVal)[scanOrder],
                    'deltaH':N.array(deltaH)[scanOrder],
                    'xFrags':N.array(fragXVals)[scanOrder],
                    'yFrags':N.array(fragYVals)[scanOrder],
                    'scanIntensity':N.array(scanIntensity)[scanOrder],
                    'protLen':N.array(protLen)[scanOrder],
                    'pepStart':N.array(pepStart)[scanOrder],
                    'pepStop':N.array(pepStop)[scanOrder]
                    }
                self.keys = self.dataDict.keys()
                self.dataLen = len(pepID)
            else:
                self.dataDict = False

    def getProteinInfo(self):
        dd = self.dataDict

        if dd == False: # if no results return None
            return None

        # AARON: 02152012 - why are there dupplicates?
        # weed out duplicates
        data = set()
        for i in xrange(len(dd['descript'])):
            data.add( (dd['descript'][i], dd['proID'][i], dd['pro_eVal'][i], dd['pepStart'][i], dd['pepStop'][i]) )

        results = {}
        for entry in data:
            spectraName = entry[0]
            if spectraName not in results:
                results[spectraName] = []
            results[spectraName].append( entry[1:] )

        return results

def appendProteinInfo(con, filename):
    x = XT_RESULTS(filename, getFlanking = True)
    results = x.getProteinInfo()
    if results != None:
        yasdbf_main.insertTandemProInfo(con, results)
