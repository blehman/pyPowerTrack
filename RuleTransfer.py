'''
   Script for copying a rule set from one PowerTrack Stream to another.
   Source and Target streams are configured in a RuleTransfer.ini file.
'''
__author__ = 'jmoffitt'

from ConfigParser import SafeConfigParser
import ptHTTP
import base64
import json
import ptRules

configParser = SafeConfigParser()
configParser.read('./RulesTransfer.ini')

#Create a PowerTrack HTTP object to use.
oHTTP = ptHTTP.ptHTTP()
#Create Rules object.
oRules = ptRules.Rules()

#GET ALL rules from source.  Can get all with a single call, no need to manage multiple calls
user = configParser.get('source','user')
passwordEncoded =  configParser.get('source','password')
auth = (user, base64.b64decode(passwordEncoded))
account_name = configParser.get('source','account_name')

#Set source URL and auth
source_url = configParser.get('source','streamRulesURL')
print 'Getting rules from: ' + source_url
#https://api.gnip.com:443/accounts/<account_name>/publishers/<publisher>/streams/track/prod/rules.json
oHTTP.setURL(source_url)
oHTTP.setAuth(auth)
oHTTP.addHeader("X-ON-BEHALF-OF-ACCOUNT:" + account_name)

#Load the rules from the Source stream.
dictRulesAdd = oRules.listRules(oHTTP)
#print dictRulesAdd["rules"][0] #print first rule

#-----------------------------------------------------------------------------------------------------------------------
#OK, we are done with source details, time to set up the HTTP POSTs to the target stream.
#In this case, let's create a fresh oHTTP object.  Otherwise, we need to reset headers.
oHTTP = ptHTTP.ptHTTP()  #default header is {'content-type': 'application/json'}

user = configParser.get('target','user')
passwordEncoded =  configParser.get('target','password')
auth = (user, base64.b64decode(passwordEncoded))
#Set target URL and auth
target_url = configParser.get('target','streamRulesURL')
print 'Posting rules to: ' + target_url
#https://api.gnip.com:443/accounts/<account_name>/publishers/t<publisher>/replay/track/<>/rules.json
oHTTP.setURL(target_url)
oHTTP.setAuth(auth)

#Delete rules from Target stream if config file says so.
#Note that dashboard provides a mechanism to do this.
clear_rules = bool(configParser.get('target','clear_rules'))
if clear_rules == True:
    dictRulesDelete = oRules.listRules(oHTTP)
    oRules.deleteRules(dictRulesDelete,oHTTP)

#Add rules to Target stream.
oRules.addRules(dictRulesAdd, oHTTP)



# Code to save/load rules from a file.
#import pickle
#rulesFile = "rules_dict.pkl"
#ptCommon.saveData(dictRules,rulesFile)
#dictRules = ptCommon.getData(rulesFile)




