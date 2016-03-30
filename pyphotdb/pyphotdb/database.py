'''
Created on Jan 8, 2016

@author: harbeck
'''

import mysql.connector as mysql

import math
import numpy as np
from datetime import date, datetime, timedelta
import logging 
from _curses import ERR




class photObject (object):
    '''
    Structure to hold a reference object that can contain sdss photoemtry.
    
    '''
    
    def __init__ (self, ra, dec, objectID=None,):
        self.data = {}
        self.data['ra'] = ra
        self.data['decl'] = dec
        self.data['objectid'] = objectID
        self.data['sdss_u'] = None
        self.data['sdss_g'] = None
        self.data['sdss_r'] = None
        self.data['sdss_i'] = None
        self.data['sdss_z'] = None
        
        self.visits = {}
        
    def getMeanMag(self):
        '''
        Iterates through all visits (if exitent) and returns the mean, stddev magnitude of all visits
        '''
        
        if self.visits != None:
            tmp = []
            for v in self.visits:
                tmp.append (v.data['mag'])
            m  = np.mean (tmp)
            s = np.std(tmp)
            return (m,s)
        return (None,None)
        
class photExposure (object):
    
    def __init__(self, exposureID=None, dateObs=None, filter=None, airmass=None, exptime=None, fwhm=None, instrument='5odi'):
        self.data = {}
        self.data['exposureid'] = exposureID
        self.data['instrument'] = instrument
        self.data['filter'] = filter
        self.data['airmass'] = airmass
        self.data['exptime'] = exptime
        self.data['fwhm'] = fwhm
        self.data['dateobs'] = dateObs
        self.data['photzp'] = None
    
class photVisit(object):
    '''
    Data holding class for a single ODI visit to an object. Maps to database VISITS table.
    
    ra,dec,mag,magerr, odix, oduy, ota, are to be extracted from ODi photometry. 
    
    exposureid maps to the exposure entry and its metadata
    
    objectid, if set, refers to an object identifier in the objcts table
    '''
    
    def __init__(self, exposureid, objectid, ra, dec, mag, magerr, odix=-1, odiy=-1, ota=-1):
        self.data = {}
        self.data['ra'] = ra
        self.data['decl'] = dec
        self.data['mag'] = mag
        self.data['magerr'] = magerr
        self.data['exposureid'] = exposureid
        self.data['odix'] = odix
        self.data['odiy'] = odiy
        self.data['ota'] = ota
        self.data['objectid'] = objectid
        pass
    
    def getGlobalXY(self):
        ota = int (self.data['ota'])
        otax, otay = divmod(ota, 10)
               
        coox = 4200 * otax + self.data['odix']
        cooy = 4200 * otay + self.data['odiy']
        return (coox,cooy)
    
def collateDataField (array, datafield):
    retVal = []
    for obj in array:
        try:
            d = obj.data[datafield]
            retVal.append (d)
        except:
            pass
            
    return np.asarray(retVal)


class database(object):
    '''
    Basic configuration to connect, setup, and interact with a mysql database
    '''

    log = logging.getLogger('database')
    TABLES = {}
    
    TABLES['exposures'] = ("CREATE TABLE IF NOT EXISTS `exposures` ("
                           " `exposureid` varchar(20) NOT NULL,"
                           " `instrument` varchar(10),"
                           " `filter` varchar(10),"
                           " `airmass` float,"
                           " `exptime` float,"
                           " `fwhm` float, "
                           " `dateobs` datetime,"
                           " `photzp` float,"
                           " PRIMARY KEY (`exposureid`)"
                           ") ENGINE=InnoDB DEFAULT CHARSET=latin1;"
                           )
    
    
    TABLES['visits'] = ("CREATE TABLE IF NOT EXISTS `visits` ("
  " `visitid` bigint(20) NOT NULL AUTO_INCREMENT,"
  " `objectid` bigint(20), "  # reference to which object we belong
  " `exposureid` varchar(20) NOT NULL, "
  " `ra` double NOT NULL,"
  " `decl` double NOT NULL,"
  " `mag` float NOT NULL,"
  " `magerr` float NOT NULL,"
  " `class1` float NOT NULL,"
  " `class2` float NOT NULL,"
  " `odix` INT,"
  " `odiy` INT,"
  " `ota` INT,"
  " PRIMARY KEY (`visitid`),"
  " INDEX (`objectid`),"
  " INDEX (`ra`),"
  " INDEX (`decl`)" 
  
  " ) ENGINE=InnoDB DEFAULT CHARSET=latin1;")
    
    TABLES['objects'] = ("CREATE TABLE IF NOT EXISTS `objects` ("
     " `objectid` bigint(20) NOT NULL AUTO_INCREMENT,"
     " `ra` double NOT NULL,"
     " `decl` double NOT NULL,"
     " `sdss_u` float,"
     " `sdss_g` float,"
     " `sdss_r` float,"
     " `sdss_i` float,"
     " `sdss_z` float,"
     
     " PRIMARY KEY (`objectid`)"
     
     " ) ENGINE=InnoDB DEFAULT CHARSET=latin1;")

    STATEMENTS = {}
    STATEMENTS['insertVisit'] = ()

    instruments = {'odi', 'sdss'}

    def __init__(self, host, port, dbuser, dbpass, dbname):
        '''
        Constructor
        '''
        self.dbhost = host
        self.dbport = port
        self.dbuser = dbuser
        self.dbpass = dbpass
        self.dbname = dbname
        
        self.connectDataBase (self.dbhost, self.dbport, self.dbuser, self.dbpass, self.dbname)
        
        self.EXPOSUREBUFFER = {}
        
    def __exit__(self):
        self.db.close()
      
    def connectDataBase (self, host, port, user, passwd, name):
        try:
            self.db = mysql.connect (host=host, port=port, user=user, password=passwd, database=name)  
            self.db.set_charset_collation('latin1')
            self.db.set_unicode(False)
            
        except mysql.Error as err:
            self.log.exception (err)
            
    
    def closeDataBase (self):
        try:
            self.db.close()
        except mysql.Error as err:
             self.log.error (err)
            
         
            
    def cleanSlateDatabase (self):
        self.log.info("DROPPING all tables in database %s" % self.dbname)
        try :
            cur = self.db.cursor()
            for name, sql in self.TABLES.iteritems():
                cur.execute ("DROP TABLE %s" % name)
        except mysql.Error as err:
            self.log.error (err.msg)
                
                
    def createDatabase (self):
        # table 1: a basic photometry table that contains only a measurement with epoch, filter, mag, magerr etc
        
        try:
            cur = self.db.cursor()
            for name, sql  in self.TABLES.iteritems():
                self.log.info ("Creating table %s" % name)
                self.log.debug ("Command is:\n%s\n" % sql)
                cur.execute (sql)
                    
        except mysql.Error as err:
            if err.errno == mysql.errorcode.ER_TABLE_EXISTS_ERROR:
                self.log.warn ("already exists.")
            else:
                self.log.error(err.msg)
        else:
            self.log.info("OK")
            
    def addExposure (self, exposureObject, cursor=None):
        '''
        
        '''
        sqlCommand = ("INSERT INTO `exposures`"
                      " (`exposureid`, `instrument`, `filter`, `airmass`, `exptime`, `fwhm`, `dateobs`, `photzp`)"
                      "VALUES (%(exposureid)s, %(instrument)s, %(filter)s, %(airmass)s, %(exptime)s, %(fwhm)s, %(dateobs)s, %(photzp)s);")
    
        try:
            if cursor == None:
                cursor = self.db.cursor()
            
            cursor.execute (sqlCommand, exposureObject.data);
            self.db.commit()
            
        except mysql.Error as err:
            self.log.exception("While addExposure:")
    
    
    
    def getExposure (self, exposureid):
        '''
        returns a photExposure object for the given exposureid
        '''
        exposure = None
        
        try:
            exposure = self.EXPOSUREBUFFER[exposureid]
            print "buffer success"
            
        except:
        
            self.log.info ("Entering getExposure")
            sqlCommand = ("select * from `exposures` where `exposureid`=%s")
            result = None
            data = None
            try:
                cursor = self.db.cursor()
                cursor.execute (sqlCommand, (exposureid,))
            
                data = cursor.fetchone()
                self.log.debug(data)
           
            except Exception as err:
                self.log.exception("While finding exposure by id")
            
            exposure = photExposure()
            if data != None:
                exposure.data = data
                self.EXPOSUREBUFFER[exposureid] = exposure
            else:
                self.log.info("No exposure for id %s found" % exposureid)
        
        return exposure
    
    def getExposureIDs (self, filter):
        ''' 
        return a list of exposureids that match the fitler inputs
        '''
        results = []
        try:
            cursor = self.db.cursor()
            cursor.execute("select `exposureid` from `exposures` WHERE `filter` = %s", (filter,))
           
              
            for row in cursor:
                results.append(str(row[0]))
            cursor.close()
        except mysql.Error as err:
            self.log.exception (err)
       
        return results
        
    def getVisitsForObject (self, object, minVisits=0, exposureids = None):
        
        object.visits = self.getVisits(object.data['objectid'], minVisits = minVisits, exposureids = exposureids) 
        
        
    
    def getVisits (self, objectid, minVisits=0, exposureids = None, instrument=None, filter=None):
        '''
        get all visits  for a given object ID
        
        TODO:
        1. move minVisits condition into SQL query
        2. 
        '''
        
        queryCommand = ("select v.visitid,v.ra,v.decl,v.mag,v.magerr,v.ota,v.odix,v.odiy,e.filter,e.exposureid, e.photzp, e.exptime, v.objectid"
         " FROM  visits v INNER JOIN exposures e ON e.exposureid = v.exposureid"
         " WHERE (objectid= %(objectid)s) ")
       
        minVisitsCond = " HAVING COUNT(v.visitid) > %(minvisits)s"
       
       
       
        if exposureids != None:
            e = []
            for id in exposureids:
                e.append ("\'%s\'" % id)
            expIDcond = " AND (v.exposureid IN (%s))" % (','.join(e))
            queryCommand += expIDcond
            
        results = {}
        c = 0;
        count = 0
        try:
            cursor = self.db.cursor()
            data = {
                    'objectid' : objectid,
                    'minvisits' : minVisits,
                    'filter': filter,
                    'instrument': instrument,
                    #'exposureids': exposureids,
                    }
           
            cursor.execute (queryCommand, data)
            #print cursor.statement
            
            for visitid, ra, dec, mag, magerr,ota,odix,odiy, filter, exposureid, photzp, exptime, objid in cursor:        
                absmag = float(mag) + float(photzp) + 2.5 * math.log10 (exptime)
                # print mag, photzp, exptime, absmag
                
                visit = photVisit(exposureid, objid, ra, dec, absmag, magerr)
                visit.data['ota'] = ota
                visit.data['odix'] = odix
                visit.data['odiy'] = odiy
                visit.data['visitid'] = visitid
                results[str(exposureid)] = visit
                
                c += 1
        
            
        except mysql.Error as err:
            self.log.exception("During getVisits: ")
        
        finally:
            pass
            # print("Find visits for objectid %d returned %d entries (should be %d)" % (objectid, c, count))
        
        if c >= minVisits:
            return results
        return {}
    
    def matchVisits (self, tolerance=1.0):
        ''' 
            selects all unmatched visits and finds a nearest matched reference object within the tolerance.
            If a reference object is not found, a new reference object will be created out of the visit.
            
            The visit will be linked to the reference object
            
        '''
        
        # ## query all unmatched visits
        unmatchedQuery = "SELECT `visitid`, `ra`, `decl` FROM `visits` where `objectid` is NULL"
        
        try:
            unmatchedDB = database(self.dbhost, self.dbport, self.dbuser, self.dbpass, self.dbname)
            unmatchedCursor = unmatchedDB.db.cursor()
            unmatchedCursor.execute (unmatchedQuery)
            
            
            for (visitid, ra, dec) in unmatchedCursor:
                    # ## find a viable reference object
                    refObj = self.findObject(ra, dec, tolerance=tolerance)
                    newid = None
                    if refObj == None:
                        print ("Create new photObject") 
                       
                        newid = self.addObject(photObject(ra, dec))
                    else:
                        newid = refObj.data['objectid']
                        
                    
                    print ("Reference object present %d" % newid)
                    updateQuery = ("UPDATE `visits` set `objectid`=%s" 
                                       " WHERE `visitid`=%s;")
                    cursor = self.db.cursor()
                    cursor.execute (updateQuery, (newid, visitid))
                        
            self.db.commit()
        except mysql.Error as err:
            print ("Error: %s" % err)
        
        
       
        
        # ## if needed, create a reference object
        
        
        # ## link visit to reference object
        
        
        pass
    
    
    def addVisits (self, visits):
        '''
        Add an array of photVisit objects for batch uploading.
        '''
        try:
            cursor = self.db.cursor()
            
            for visit in visits:
                self.addVisit(visit, cursor)
            
            cursor.close()
            
        except mysql.Error as err:
            print (err)
        
    def addVisit (self, photVisit, cursor=None):
        '''
        Insert a single photVisit into the database.
        
        If cursor database cursor is give, it will be used. Otherwise, a cursor will be opened. 
        
        
        '''
        
        sqlcommand = ("INSERT INTO `visits`"
                    " ( `exposureid`, `objectid`, `ra`, `decl`, `mag`, `magerr`, `odix`, `odiy`, `ota`)"
                    " VALUES (%(exposureid)s, %(objectid)s,  %(ra)s, %(decl)s, %(mag)s, %(magerr)s, %(odix)s, %(odiy)s, %(ota)s)")
        
        try:
            if cursor == None:
                cursor = self.db.cursor()
            
            cursor.execute (sqlcommand, photVisit.data);
            self.db.commit()
            
        except mysql.Error as err:
            print (err.msg)
            
    
    def findaddObjects (self, objects):
        '''
        Add a list of photObjects to the database if they are not yet present. 
        '''
        newobj = 0
        existobj = 0
        try:
            cursor = self.db.cursor()
            
            for obj in objects:
              
                exist = self.findObject(obj.data['ra'], obj.data['decl'], cursor=cursor)
                if exist == None:
                  
                    newobj += 1
                    newid = self.addObject(obj, cursor)
                    obj.data['objectid'] = newid
                    self.log.debug ("Object does not exist. Adding new one and its ID is %d" % newid)
                else:    
                    existobj += 1
                    obj.data['objectid'] = exist.data['objectid']
          
            cursor.close()
        
        except mysql.Error as err:
            self.log.exception("During findaddObecjts:")
            
        self.log.info (" New objects    : % 10d" % newobj)
        self.log.info (" Existing objets: % 10d" % existobj)
    
                    
    def addObject (self, newObject, cursor=None):
        ''' 
        Add as single photObject to the database
        '''
        
        sqlcommand = ("INSERT INTO `objects`"
                      " (`ra`, `decl`, `sdss_u`, `sdss_g`, `sdss_r`, `sdss_i`, `sdss_z`)"
                      " VALUES  (%(ra)s, %(decl)s, %(sdss_u)s, %(sdss_g)s, %(sdss_r)s, %(sdss_i)s, %(sdss_z)s)")
       
        newid = None
        try:
            if cursor == None:
                cursor = self.db.cursor()
            
            cursor.execute (sqlcommand, newObject.data);
            newid = cursor.lastrowid
            self.db.commit()
            
        except mysql.Error as err:
            self.log.exception("While inserting single phot object:")
        return newid
    
    
    def distance2 (self, ra, dec, object):
        '''
        Calcualte the distance squared (sqrt ommitted to save computation) between ra, dec 
        and a photObject. 
        
        photObject's data['ra'] and data['decl'] fields are used, i.e., this will work for every 
        photXXX class that has these fields.
        '''
        
        dra = ra - object.data['ra']
        ddec = dec - object.data['decl']
        
        if (math.fabs(dec) < 90.):
            dra = dra / math.sin (dec * math.pi / 180.)
        
        d2 = dra * dra + ddec * ddec
        return d2
        
        
    def findObject (self, ra, dec, tolerance=0.5, cursor=None):
        
        ret = None
        tolerance /= 3600.
        tolerance *= tolerance  # skip the sqrt
        results = self.findObjects (ra, dec, cursor=cursor)
        for obj in results:
            d = self.distance2(ra, dec, obj)
            if (d < tolerance):
                ret = obj
                tolerance = d
                
        return ret

    def findObjects (self, ra, dec, sqr=3., cursor=None):
        '''
        Queries all reference objects within a certain square in sky, 
        and returns a list of all objects found in the form of photObject.           
        '''
        sqlQuery = ("SELECT  `objectid`,`ra`,`decl`,`sdss_u`, `sdss_g`, `sdss_r`, `sdss_i`, `sdss_z` FROM `objects`"
            " WHERE  ("
            " ( ABS ( (`ra`   - %(raref)s )  )  <= %(tol)s )"
            " AND"
            " ( ABS ( `decl` - %(declref)s) <= %(tol)s )"
            " )")
        
        
        data = { "raref" : ra,
                
                "tol" : sqr / 3600.,
                "declref": dec
                 
                }
        
        results = []
        try:
            if cursor == None:
                cursor = self.db.cursor()
            
            cursor.execute (sqlQuery, data)
            for (objectid, ra, dec, u,g,r,i,z) in cursor:
                obj = photObject(ra, dec, objectid)
                obj.data['sdss_u'] = u
                obj.data['sdss_g'] = g
                obj.data['sdss_r'] = r
                obj.data['sdss_i'] = i
                obj.data['sdss_z'] = z
                results.append (obj)
                
        except mysql.Error as err:
            self.log.exception("While finding photObjects:")
        return results
  
  
if __name__ == "__main__":      
    logging.basicConfig(format='%(asctime)s %(message)s')
    db = database('localhost', 3306, 'stardb', 'stardb', 'm33')
    # db.cleanSlateDatabase()
    # sdb.createDatabase()

    # db.addObject(3.4, 1.1)
    # res = db.findObject(3.4, 1.1)
    # print res.data

    # exp = photExposure('20151205T181818.1', datetime.now(), 'odi_u', 1.2, 300, 0.6)
    # db.addExposure (exp)

    # db.addVisit(photVisit (exp.data['exposureid'], 1.1, 2.2, 24, 0.1))
    # db.addVisit(photVisit (exp.data['exposureid'], 1.2, 2.3, 24, 0.1))
    
    
    # db.matchVisits()
    
    # db.getVisits(1)
    # db.getVisits(2)
    
    exposures = db.getExposureIDs('odi_u')   
    
    exposure1 = db.getExposure(exposures[0])
    exposure1 = db.getExposure(exposures[0])
    print exposure1.data
    
    
    
    
    db.closeDataBase()
