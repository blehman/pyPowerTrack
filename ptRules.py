__author__ = 'jmoffitt'

#TODO: add error handling.
#TODO: return success/error flags.
#TODO: add 'verbose' switch.... add logging?

import json

class Rules:

    MAX_API_BATCH = 5000

    def __init__(self):
        #print "Creating Rules object..."
        pass


    '''
        Determine the number of necessary requests
    '''
    def numRequests(self, num_rules):
        i = num_rules/self.MAX_API_BATCH
        if num_rules % self.MAX_API_BATCH > 0:
            i = i + 1
        return i

    '''
        Simple conversion function based on a core assumption: rules are either a dictionary, a list, or JSON format.

        When rules are converted from JSON, the Python data structure is a Dictionary, with a "rules" key and a value
        that is a List of rules.  When managing rules with Python code, the rules list is typically 'popped' out of the
        Dictionary and managed directly.

        So, this function handles rebuilding the dictionary if the rules list is passed in.

    '''
    def convert(self,data):

        #convert into the Gnip JSON format
        if type(data) == list:  #Passing in the rules list, rebuilt the dictionary with the 'rules' key containing
            #the rules list.
            rulesDict = {}
            #Add on the root "rules" object.
            rulesDict["rules"] = data
            return json.dumps(rulesDict)
        if type(data) == dict:
            return json.dumps(data)
        else:
            #Must be JSON format, need to put into dictionary.
            #TODO: confirm this type!  str?
            return json.loads(data)



    '''
    DELETE rules from oHTTP Stream.  Delete up to 5000 per request, one request per second.
    '''
    def deleteRules(self, rulesDict, oHTTP):

        lsRules = rulesDict["rules"]  #Get the rules List from the dictionary.
        #Now see how many there are to delete, and delete up to 5000 per request.
        num_rules = len(lsRules)
        print 'Target stream currently has ' + str(num_rules)  + ' rules.'
        print 'Need to DELETE ' + str(self.numRequests(num_rules)) + ' batch(es) of rules. '

        lsRulePayload = []
        i = 0

        if len(lsRules) <= self.MAX_API_BATCH: #then we are ready to roll, nothing more to do.
            jsonRules = self.convert(lsRules)
            if len(lsRules) > 0:
                response = oHTTP.DELETE(jsonRules)
                print response.content
        else: #We need to break up pieces, up to 5000 rules per payload...
            for rule in lsRules:
                i = i + 1
                lsRulePayload.append(rule)
                if len(lsRulePayload) == self.MAX_API_BATCH or i == num_rules:
                    #Convert payload rule dictionary back to Gnip JSON ruleset.
                    jsonRules = self.convert(lsRulePayload)
                    #Send this rule payload to target stream.
                    print 'Making request to DELETE ' + str(len(lsRulePayload)) + ' rules...'
                    response = oHTTP.DELETE(jsonRules)
                    status = response.status_code
                    if status >= 200 and status < 300:
                        print 'Successful request (status code: ' + str(status) + ')'
                    else:
                        print 'Problem with request (status code: ' + str(status) + ')'

                    #Sent rules payload, initialize list.
                    lsRulePayload = []


    '''
    POST rules to oHTTP Stream.  Up to 5000 per POST, one per request per second
    '''
    def addRules(self, rulesDict, oHTTP):

        #Inspect rules dictionary, breaking up into payloads of no more than 5000 rules.
        lsRules = rulesDict["rules"]  #Get the rules List from the dictionary.

        num_rules = len(lsRules)
        print 'Adding ' + str(num_rules)  + ' rules...'
        print 'Need to send ' + str(self.numRequests(num_rules)) + ' batch(es) of rules. '

        lsRulePayload = []
        i = 0

        if len(lsRules) <= self.MAX_API_BATCH: #then we are ready to roll, nothing more to do.
            jsonRules = self.convert(lsRules)
            response = oHTTP.POST(jsonRules)
            print response.content
        else: #We need to break up pieces, up to 5000 rules per payload...
            for rule in lsRules:
                i = i + 1
                lsRulePayload.append(rule)
                if len(lsRulePayload) == self.MAX_API_BATCH or i == num_rules:
                    #Convert payload rule dictionary back to Gnip JSON ruleset.
                    jsonRules = self.convert(lsRulePayload)
                    #Send this rule payload to target stream.
                    print 'Making request to ADD ' + str(len(lsRulePayload)) + ' rules...'
                    response = oHTTP.POST(jsonRules)
                    status = response.status_code
                    if status >= 200 and status < 300:
                        print 'Successful request (status code: ' + str(status) + ')'
                    else:
                        print 'Problem with request (status code: ' + str(status) + ')'
                    #Sent rules payload, initialize list.
                    lsRulePayload = []


    '''
    List rules from Stream oHTTP is configured with.
    '''
    def listRules(self,oHTTP):
        response = oHTTP.GET()
        return json.loads(response.content)  #Cast into a dictionary and return.