'''
Created on Jan 9, 2016

@author: harbeck
'''

import database
import logging

import matplotlib.pyplot as plt
import numpy as np


class MyClass(object):
    '''
    classdocs

    Map structure


    '''


    def __init__(self, params):
        '''
        Constructor
        '''
        
    
    def correctZPforObjects(db, objects):
        '''
       
        '''   
        for object in objects:
       
            for k,v in object.visits.iteritems():
           
           
                exposure = db.getExposure(k)
                zp = exposure.data['photzp']
                airmass = exposure.data['airmass']
                fwhm = exposure.data['fwhm']
                dateobs = exposure.data['dateobs']
                v.data['mag'] += zp
                v.data['dateobs'] = dateobs
        
        




if __name__ == "__main__":
    
     logging.basicConfig(format='%(asctime)s %(message)s')
     
     db = database.database('localhost', 4001, 'stardb', 'stardb', 'stardb_test')
     
     exposures = db.getExposureIDs('odi_u')
     print  exposures
     
     objects = db.findObjects(48.66, 41.3, 3600)
     
     ra = []
     dec = []
     stddev = []
     mean = []
     delta = []
     
     for object in objects:
         db.getVisitsForObject(object, minVisits=1)
         if len(object.visits) > 0:
             refmag =  object.data['sdss_u']
            
             d = []
             if refmag != None:
                 for v in object.visits:
                     x,y = v.getGlobalXY()
                     delta.append (v.data['mag'] - refmag)
                     ra.append(x)
                     dec.append(y)
                     
    
     print np.median(delta)
     print np.std(delta)
    
     plt.scatter (ra,dec, c= delta, marker="s",linewidths=0, vmin=-0.2,vmax=0.2)  
     plt.colorbar()
     plt.show()
     
     
     
     
     
    
        
        
        
    