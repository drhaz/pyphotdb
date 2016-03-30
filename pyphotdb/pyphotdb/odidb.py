'''
Created on Jan 8, 2016

@author: harbeck
'''
import astropy.io.fits as fits
import database
import math
import numpy as np
from pyphotdb.database import photObject, photVisit
from datetime import date, datetime, timedelta
import logging

class odiQRIngester(object):
    '''
    Utility class to ingest ODI quickreduce photometry.
    '''
    log = logging.getLogger('odiQRIngester')
    
    def __init__(self, odifilename, db):
        '''
        Constructor
        '''
        self.odifilename = odifilename
        self.db = db
        
        self.start()
        
    def start(self):
        self.readPhotTable()
        
    def readPhotTable(self):
        self.log.debug ("Start ingestion of file: %s", self.odifilename)
        hdulist = fits.open(self.odifilename)
        
        ### Megtadata first
        obsid = hdulist[0].header['OBSID']
        filter= hdulist[0].header['FILTER']
        airmass= float (hdulist[0].header['AIRMASS'])
        exptime= float (hdulist[0].header['EXPTIME'])
        photzp = float (hdulist[0].header['PHOTZP'])
        
        expObject = database.photExposure(obsid,datetime.now(), filter, airmass, exptime, 0.6)
        expObject.data['photzp'] = photzp
        self.db.addExposure(expObject)
        
        phottbl = hdulist['CAT.PHOTCALIB'].data
        phottbl.columns.info()
          
        objects = []
        visits = []
        self.log.debug ("Process reference catalog")
        for row in phottbl:
            objra = np.float(row['SDSS_RA'])
            objdec = np.float(row['SDSS_DEC'])
            
            obj = photObject (objra, objdec)
            obj.data['sdss_u'] = np.float (row['SDSS_MAG_U'])
            obj.data['sdss_g'] = np.float (row['SDSS_MAG_G'])
            obj.data['sdss_r'] = np.float (row['SDSS_MAG_R'])
            obj.data['sdss_i'] = np.float (row['SDSS_MAG_I'])
            obj.data['sdss_z'] = np.float (row['SDSS_MAG_Z'])
                  
            # objectid is unknown at this time, set to None
            visit = photVisit(obsid, None, np.float(row['ODI_RA']), np.float(row['ODI_DEC']), 
                              np.float(row['ODI_MAG_AUTO']), np.float(row['ODI_ERR_AUTO']), np.int (row['ODI_X']), np.int(row['ODI_Y']), np.int(row['ODI_OTA']))
                      
            objects.append (obj)
            visits.append (visit)
            
        self.log.info ("Ingesting objects into database")
        self.db.findaddObjects(objects)
        self.log.info ("Ingesting visits into database")
        self.db.addVisits (visits)
        self.log.info ("Done ingesting file")
        
    def pairVisits (self):
        '''
        Queries the database for unmatched objects and finds its reference object.
        '''
        
        
        
        

if __name__ == "__main__":

    logging.basicConfig(format='%(asctime)s %(message)s')

    # ## prep the database
    db = database.database('localhost', 4001, 'stardb', 'stardb', 'stardb_test')
    #db.cleanSlateDatabase()
    #db.createDatabase()

    # ## ingest data
    odidata = odiQRIngester("/Users/harbeck/Astronomy/odiu/calibrated/20151204T005306.2_Perseus_35_arcmin_west_odi_u.5830/20151204T005306.2_Perseus_35_arcmin_west_odi_u.5830.fits", db)
   
    #odidata = odiQRIngester("/Users/harbeck/Astronomy/odiu/calibrated/20151204T005306.3_Perseus_35_arcmin_west_odi_u.5830/20151204T005306.3_Perseus_35_arcmin_west_odi_u.5830.fits", db)
    #odidata = odiQRIngester("/Users/harbeck/Astronomy/odiu/calibrated/20151204T005306.4_Perseus_35_arcmin_west_odi_u.5830/20151204T005306.4_Perseus_35_arcmin_west_odi_u.5830.fits", db)
    #odidata = odiQRIngester("/Users/harbeck/Astronomy/odiu/calibrated/20151204T005306.5_Perseus_35_arcmin_west_odi_u.5830/20151204T005306.5_Perseus_35_arcmin_west_odi_u.5830.fits", db)


    ###
    db.matchVisits(1.0)

    ### clean up
    db.closeDataBase()