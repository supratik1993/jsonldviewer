#import statements : web related 
import os
import shutil
import sqlite3
from datetime import datetime
import re
from flask import Flask, request, session, g, redirect, url_for, abort, render_template, flash, jsonify
from werkzeug.utils import secure_filename
import json

#import statements: rdf related
import rdflib
from rdflib.plugins.sparql import prepareQuery
import rdflib.plugins.sparql.results.jsonlayer as jl


#-------------------------------------------------------------------------
#constants : app level
APP_ROOT = os.path.dirname(os.path.abspath(__file__))
UPLOAD_FOLDER = os.path.join(APP_ROOT, 'static/uploads')
ALLOWED_EXTENSIONS = set(['jsonld'])


#-------------------------------------------------------------------------
#constants : graph URIs and rdf related folders
BRICKFRAME_GRAPH = 'http://buildsys.org/ontologies/BrickFrameGraph'
BRICKTAG_GRAPH = 'http://buildsys.org/ontologies/BrickTagGraph'
BRICK_GRAPH = 'http://buildsys.org/ontologies/BrickGraph'
UPLOAD_GRAPH = 'http://jci.commissioned.buildings.org/'

#paths
SLEEPYCAT_DB_FOLDER = os.path.join(APP_ROOT, 'sleepycat_db')
BRICKFRAME_PATH = os.path.join(APP_ROOT, 'brick_schema', 'BrickFrame.ttl')
BRICKTAG_PATH = os.path.join(APP_ROOT, 'brick_schema', 'BrickTag.ttl')
BRICK_PATH = os.path.join(APP_ROOT, 'brick_schema', 'Brick.ttl')


#uris
rdf_uri = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#'
rdfs_uri = 'http://www.w3.org/2000/01/rdf-schema#'
owl_uri = 'http://www.w3.org/2002/07/owl#'
skos_uri = 'http://www.w3.org/2004/02/skos/core#'
brickframe_uri = 'http://buildsys.org/ontologies/BrickFrame#'
bricktag_uri = 'http://buildsys.org/ontologies/BrickTag#'
brick_uri = 'http://buildsys.org/ontologies/Brick#'
site_uri = 'http://jci.buildings.org/ontology/carsongulley#'

sf_ttl = 'turtle'
sf_jsonld = 'jsonld'

rdf_store_name = 'Sleepycat'

#http methods
GET = 'GET'
POST = 'POST'


#-------------------------------------------------------------------------
#create flask application and set its name
app = Flask(__name__)
app.config.from_object(__name__)

#-------------------------------------------------------------------------
#Load default config and override config from env variable
app.config.update(dict(
    DATABASE=os.path.join(app.root_path, 'jsonldviewer.db'),
    SECRET_KEY='brickschemajsonld',
    USERNAME='admin',
    PASSWORD='admin'
))

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config.from_envvar('JSONLDVIEWER_SETTINGS', silent=True)

#-------------------------------------------------------------------------
def connect_db():
    "This function connects to a local sqlite db"
    rv = sqlite3.connect(app.config['DATABASE'])
    rv.row_factory = sqlite3.Row
    return rv

#-------------------------------------------------------------------------
def get_db():
    """Opens a new database connection if there is none yet for the
    current application context."""
    if not hasattr(g, 'sqlite_db'):
        g.sqlite_db = connect_db()
    return g.sqlite_db
    
#-------------------------------------------------------------------------
def init_db():
    """This function initialises the sqlite db and 
    creates necessary tables from a local sql file."""
    db = get_db()
    with app.open_resource('schema.sql', mode='r') as f:
        db.cursor().executescript(f.read())
    db.commit()

#-------------------------------------------------------------------------
@app.cli.command('initdb')
def initdb_command():
    """Registers a command with flask so that it can used to call
    the init_db() function."""
    init_db()
    print('Initialized the database.')


#-------------------------------------------------------------------------
@app.teardown_appcontext
def close_db(error):
    """Closes the database again at the end of the request."""
    if hasattr(g, 'sqlite_db'):
        g.sqlite_db.close()


#-------------------------------------------------------------------------
@app.errorhandler(401)
def unauthorized_handler(e):
    return render_template('Unauthorized_404.html')

@app.errorhandler(400)
def bad_request_handler():
    return render_template('Bad_Request_400.html')

#-------------------------------------------------------------------------
def getFiles():
    """This function fetches all the files uploaded todate
    from the sqlite db"""
    db = get_db()
    cur = db.execute('select filetitle, description, \
        uploadedtime, filename from jsonfiles order by uploadedtime desc')
    files = cur.fetchall()
    return files

#-------------------------------------------------------------------------
@app.route('/', methods=['GET', 'POST'])
@app.route('/index', methods=['GET', 'POST'])
def index():
    """This is the view function for the home page"""
    error = None

    #for GET requests
    if request.method == 'GET':
        #if not logged_in
        if not session.get('logged_in'):
            return render_template('index.html')
        #if logged_in
        else:
            files = getFiles()
            return render_template('index.html', files=files)
    
    #for POST requests
    elif request.method == 'POST':
        #if not logged_in
        if not session.get('logged_in'):
            if request.form['username'] != app.config['USERNAME'] or request.form['passwd'] != app.config['PASSWORD']:
                error = 'Invalid credentials'
                return render_template('index.html', error=error)
            else:
                files = getFiles()
                session['logged_in'] = True
                session['username'] = app.config['USERNAME']
                return render_template('index.html', files=files)
        #if logged_in
        else:
            return abort(401)
    else:
        abort(400)


#-------------------------------------------------------------------------
@app.route('/logout')
def logout():
    "This is the view function for logging out of the current session"
    session.pop('logged_in', None)
    return redirect(url_for('index'))

#-------------------------------------------------------------------------
def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


#-------------------------------------------------------------------------
def save_in_sleepycat(dbname, jsonldfilepath):
    ttl = 'turtle'
    jld = 'json-ld'

    dbpath = os.path.join(SLEEPYCAT_DB_FOLDER, dbname)

    ds = rdflib.Dataset(store='Sleepycat', default_union=True)
    rt = ds.open(dbpath, create=False)

    if rt == rdflib.store.NO_STORE:
        ds.open(dbpath, create=True)
    
    brickframe = rdflib.URIRef(BRICKFRAME_GRAPH)
    g1 = ds.graph(brickframe)
    g1.parse(BRICKFRAME_PATH, format=ttl)

    #print len(g1)

    bricktag = rdflib.URIRef(BRICKTAG_GRAPH)
    g2 = ds.graph(bricktag)
    g2.parse(BRICKTAG_PATH, format=ttl)

    #print len(g2)

    brick = rdflib.URIRef(BRICK_GRAPH)
    g3 = ds.graph(brick)
    g3.parse(BRICK_PATH, format=ttl)

    #print len(g3)

    
    #print uploadedBldg
    #print uploadedBldg.identifier

    g4tmp = rdflib.ConjunctiveGraph()
    g4tmp.parse(jsonldfilepath, format=jld)

    uploadedBldg =rdflib.URIRef(UPLOAD_GRAPH + dbname + 'graph')
    g4 = ds.graph(uploadedBldg)

    for t in g4tmp.triples((None, None, None)):
        g4.add(t)

    #print len(g4)

    #print len(g1) + len(g2) + len(g3) + len(g4)

    #print len(ds)
    ts = len(ds)

    ds.close()

    return ts

#-------------------------------------------------------------------------
@app.route('/addfile', methods=['GET', 'POST'])
def addfile():
    if request.method == 'GET':
        if session.get('logged_in'):
            return render_template('addfile.html')
        else:
            return abort(401)

    else:
        filefield = 'jsonldfile'
        if filefield not in request.files:
            uploadmsg = 'No File was uploaded!'
            uploadstatus = 'F'
            return render_template('addfile.html', uploadmsg=uploadmsg, uploadstatus=uploadstatus)

        elif request.files[filefield].filename == '' or request.form['filetitle'] == '' or request.form['filedesc'] == '':
            uploadmsg = 'ERROR! Fields were empty OR No File was selected!'
            uploadstatus = 'F'
            return render_template('addfile.html', uploadmsg=uploadmsg, uploadstatus=uploadstatus)

        elif not allowed_file(request.files[filefield].filename):
            uploadmsg = 'ERROR! Wrong file format'
            uploadstatus = 'F'
            return render_template('addfile.html', uploadmsg=uploadmsg, uploadstatus=uploadstatus)

        else:
            uploadedfile = request.files[filefield]
            uploadedfilename = secure_filename(uploadedfile.filename)
            uploadedfilesavepath = os.path.join(app.config['UPLOAD_FOLDER'], uploadedfilename)
            if os.path.exists(uploadedfilesavepath):
                uploadmsg = 'ERROR! A file with the same name already exists'
                uploadstatus = 'F'
                return render_template('addfile.html', uploadmsg=uploadmsg, uploadstatus=uploadstatus)
            
            db = get_db()
            cur = db.execute('select filetitle from jsonfiles where filetitle = (?)', (request.form['filetitle'],))
            result = cur.fetchone()
            if result == request.form['filetitle']:
                uploadmsg = 'ERROR! Duplicate File title'
                uploadstatus = 'F'
                return render_template('addfile.html', uploadmsg=uploadmsg, uploadstatus=uploadstatus)

            #all is ok
            #uploadedfilesavepath = os.path.join(app.config['UPLOAD_FOLDER'], uploadedfilename)
            uploadedfile.save(uploadedfilesavepath)

            db = get_db()
            db.execute('insert into jsonfiles (filetitle, description, uploadedtime, filename) values (?, ?, ?, ?)', \
                    (request.form['filetitle'], request.form['filedesc'], datetime.now(), uploadedfilename))
            db.commit()

            uploadmsg = 'File Upload Successful'
            uploadstatus = 'T'

            #save it in sleepycat db
            dbname = '_'.join(request.form['filetitle'].split())
            triples = save_in_sleepycat(dbname=dbname, jsonldfilepath=uploadedfilesavepath)

            return render_template('addfile.html', uploadmsg=uploadmsg, uploadstatus=uploadstatus, triplecount=triples)


#-------------------------------------------------------------------------     
@app.route('/viewer/<filetitle>', methods=['GET'])
def viewer(filetitle):
    """Displays the viewer main page

    Parameters:
        ---filetitle = the title of the file to be opened in viewer

    Returns:
        ---the viewer template passing the entire file information and the actual
        jsonld file content
    """

    #gets sqlite db connection instance
    db = get_db()

    #executes query to fetch details of using filetitle
    cur = db.execute('select filetitle, description, \
            uploadedtime, filename from jsonfiles where filetitle = (?)', (filetitle,))
    result = cur.fetchone()


    #initialize rdf graph and send json to template

    ##extracts the file name
    filename = result[3]

    #creates file path
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)

    #opens the jsonld file and reads the contents into memory
    with open(filepath) as jsonld_data:
        d = json.load(jsonld_data)
        j = json.dumps(d, indent=4)
        
    #returns the viewer page with the necessary data
    return render_template('viewer.html', fileinfo=result, jsonld=j)

#-------------------------------------------------------------------------
@app.route('/delete/<filetitle>', methods=['GET'])
def delete(filetitle):
    """Deletes a jsonld file and also its associated SQLite DB entry
    and the Sleepycat RDF Database

    Parameters:
        ---filetitle = the title of the file to be deleted

    Returns:
        ---Redirects to Index view with a response code and a response message
    """
    #gets db connection instance
    db = get_db()

    #executes query to fetch details of using filetitle
    cur = db.execute('select filetitle, description, \
        uploadedtime, filename from jsonfiles where filetitle = (?)', (filetitle,))
    result = cur.fetchone()
    
    #extracts the file name
    filename = result[3]

    #creates file path
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)

    #removes file using file path
    os.remove(filepath)

    #deletes entry of file from sqlite database
    db.execute('delete from jsonfiles where filetitle = (?)', (filetitle,))
    db.commit()

    #creates path to sleepycat database for this file
    SLEEPYCAT_DB_PATH = os.path.join(SLEEPYCAT_DB_FOLDER, '_'.join(filetitle.split()))

    #removes entire directory containing sleepycat database
    shutil.rmtree(SLEEPYCAT_DB_PATH, ignore_errors=False)

    return redirect(url_for('index'))


#-------------------------------------------------------------------------
@app.route('/getNamespaceURIs', methods=['POST'])
def getNamespaceURIs():
    d = dict()
    d['rdf'] = rdf_uri
    d['rdfs'] = rdfs_uri
    d['owl'] = owl_uri
    d['skos'] = skos_uri
    d['bf'] = brickframe_uri
    d['tag'] = bricktag_uri
    d['brick'] = brick_uri
    d['site'] = site_uri
    return jsonify(d)

#--------------------------------------------------------------------------
def shortenURI(uri):
    if re.search(rdf_uri, uri):
        return re.sub(rdf_uri, 'rdf:', uri)
    elif re.search(rdfs_uri, uri):
        return re.sub(rdfs_uri, 'rdfs:', uri)
    elif re.search(owl_uri, uri):
        return re.sub(owl_uri, 'owl:', uri)
    elif re.search(skos_uri, uri):
        return re.sub(skos_uri, 'skos:', uri)
    elif re.search(brickframe_uri, uri):
        return re.sub(brickframe_uri, 'bf:', uri)
    elif re.search(bricktag_uri, uri):
        return re.sub(bricktag_uri, 'tag:', uri)
    elif re.search(brick_uri, uri):
        return re.sub(brick_uri, 'brick:', uri)
    elif re.search(site_uri, uri):
        return re.sub(site_uri, 'site:', uri)
    else:
        return uri
        

#-------------------------------------------------------------------------------
@app.route('/searchByClass', methods=['POST'])
def searchByClass():
    BRICKFRAME = rdflib.Namespace('http://buildsys.org/ontologies/BrickFrame#')
    BRICK = rdflib.Namespace('http://buildsys.org/ontologies/Brick#')
    BRICKTAG = rdflib.Namespace('http://buildsys.org/ontologies/BrickTag#')
    SITE = rdflib.Namespace('http://jci.buildings.org/ontology/carsongulley#')
    SKOS = rdflib.Namespace('http://www.w3.org/2004/02/skos/core#')

    filetitle = '_'.join(request.json['filetitle'].split())
    brickclass = 'brick:' + request.json['brickClass']

    dbpath = os.path.join(SLEEPYCAT_DB_FOLDER, filetitle)
    ds = rdflib.Dataset(store='Sleepycat', default_union=True)
    rt = ds.open(dbpath, create=False)
    if rt == rdflib.store.NO_STORE:
        return jsonify(response = 'No RDF DB exists')
        
    queryString = """SELECT ?s WHERE {
                ?s rdf:type ?o.
                ?o rdfs:subClassOf* %s.
                }""" % (brickclass,)    

    # elif brickclass == 'location':
    #     queryString = """SELECT ?s WHERE {
    #             ?s rdf:type ?o.
    #             ?o rdfs:subClassOf* brick:Location.
    #             }"""

    # elif brickclass == 'point':
    #     queryString = """SELECT ?s WHERE {
    #             ?s rdf:type ?o.
    #             ?o rdfs:subClassOf* brick:Point.
    #             }"""
        
    print queryString
    q = prepareQuery(queryString, initNs={'brick':BRICK, 'rdf':rdflib.RDF, 'rdfs':rdflib.RDFS, 'site':SITE})
    queryResult = ds.query(q)
        
    print len(queryResult)

                
    d = list()
            
    for row in queryResult:
        tmp = shortenURI(row['s'])            
        d.append(tmp)
                        
    ds.close()
    return jsonify(d)


#---------------------------------------------------------------------------------------------
@app.route('/customSearch', methods=['POST'])
def customSearch():
    BRICKFRAME = rdflib.Namespace('http://buildsys.org/ontologies/BrickFrame#')
    BRICK = rdflib.Namespace('http://buildsys.org/ontologies/Brick#')
    BRICKTAG = rdflib.Namespace('http://buildsys.org/ontologies/BrickTag#')
    SITE = rdflib.Namespace('http://jci.buildings.org/ontology/carsongulley#')
    SKOS = rdflib.Namespace('http://www.w3.org/2004/02/skos/core#')

    filetitle = '_'.join(request.json['filetitle'].split())
    searchTerm = request.json['searchTerm']
    selectedPosition = request.json['selectedPosition']

    dbpath = os.path.join(SLEEPYCAT_DB_FOLDER, filetitle)
    ds = rdflib.Dataset(store='Sleepycat', default_union=True)
    rt = ds.open(dbpath, create=False)
    if rt == rdflib.store.NO_STORE:
        return jsonify(response = 'No RDF DB exists')
    
    graph_uri = rdflib.URIRef(UPLOAD_GRAPH + '_'.join(filetitle.split()) + 'graph')
    #print graph_uri

    # for c in ds.graphs():
    #     print c.identifier
    #     if str(c.identifier).startswith('http'):
    #         g = ds.graph(rdflib.URIRef(c.identifier))
    #         print len(g)
    
    graph = ds.graph(graph_uri)
    #print len(graph)
    if selectedPosition == 'subject':
        queryString = """SELECT ?s ?p ?o ?c WHERE {
                ?s ?p ?o.
                FILTER(regex(str(?s), "%s", "i"))
                }""" % (searchTerm,)
        q = prepareQuery(queryString, initNs={'brick':BRICK, 'rdf':rdflib.RDF, 'rdfs':rdflib.RDFS, 'site':SITE})
        queryResult = graph.query(q)
            
        d = dict()

        d['type'] = 'subject'
        d['result'] = list()
        
        for row in queryResult:
            tmp = dict()
            if row['s']:
                tmp['s'] = shortenURI(row['s'])
            else:
                tmp['s'] = ''
            
            if row['p']:
                tmp['p'] = shortenURI(row['p'])
            else:
                tmp['p'] = ''
            
            if row['o']:
                tmp['o'] = shortenURI(row['o'])
            else:
                tmp['o'] = ''
            d['result'].append(tmp)

        return jsonify(d)

    elif selectedPosition == 'property':
        queryString = """SELECT ?s ?p ?o WHERE {
                ?s ?p ?o.
                FILTER(regex(str(?p), "%s", "i"))
                }""" % (searchTerm,)
        q = prepareQuery(queryString, initNs={'brick':BRICK, 'rdf':rdflib.RDF, 'rdfs':rdflib.RDFS, 'site':SITE})
        queryResult = graph.query(q)

        d = dict()

        d['type'] = 'property'
        d['result'] = list()
        
        tmp = dict()
        for row in queryResult:
            tmp = dict()
            if row['s']:
                tmp['s'] = shortenURI(row['s'])
            else:
                tmp['s'] = ''
            
            if row['p']:
                tmp['p'] = shortenURI(row['p'])
            else:
                tmp['p'] = ''
            
            if row['o']:
                tmp['o'] = shortenURI(row['o'])
            else:
                tmp['o'] = ''
            d['result'].append(tmp)

        return jsonify(d)

    elif selectedPosition == 'object':
        queryString = """SELECT ?s ?p ?o WHERE {
                ?s ?p ?o.
                FILTER(regex(str(?o), "%s", "i"))
                }""" % (searchTerm,)
        q = prepareQuery(queryString, initNs={'brick':BRICK, 'rdf':rdflib.RDF, 'rdfs':rdflib.RDFS, 'site':SITE})
        queryResult = graph.query(q)

        d = dict()

        d['type'] = 'object'
        d['result'] = list()
        
        tmp = dict()
        for row in queryResult:
            tmp = dict()
            if row['s']:
                tmp['s'] = shortenURI(row['s'])
            else:
                tmp['s'] = ''
            
            if row['p']:
                tmp['p'] = shortenURI(row['p'])
            else:
                tmp['p'] = ''
            
            if row['o']:
                tmp['o'] = shortenURI(row['o'])
            else:
                tmp['o'] = ''
            d['result'].append(tmp)

        return jsonify(d)

    elif selectedPosition == 'all':
        queryString1 = """SELECT ?s ?p ?o WHERE {
                ?s ?p ?o.
                FILTER(regex(str(?s), "%s", "i"))
                }""" % (searchTerm,)
        q1 = prepareQuery(queryString1, initNs={'brick':BRICK, 'rdf':rdflib.RDF, 'rdfs':rdflib.RDFS, 'site':SITE})
        queryResult1 = graph.query(q1)

        queryString2 = """SELECT ?s ?p ?o WHERE {
                ?s ?p ?o.
                FILTER(regex(str(?p), "%s", "i"))
                }""" % (searchTerm,)
        q2 = prepareQuery(queryString2, initNs={'brick':BRICK, 'rdf':rdflib.RDF, 'rdfs':rdflib.RDFS, 'site':SITE})
        queryResult2 = graph.query(q2)


        queryString3 = """SELECT ?s ?p ?o WHERE {
                ?s ?p ?o.
                FILTER(regex(str(?o), "%s", "i"))
                }""" % (searchTerm,)
        q3 = prepareQuery(queryString3, initNs={'brick':BRICK, 'rdf':rdflib.RDF, 'rdfs':rdflib.RDFS, 'site':SITE})
        queryResult3 = graph.query(q3)

        d = dict()

        d['type'] = 'all'
        d['result'] = dict()
        d['result']['subjectwise'] = list()
        d['result']['propertywise'] = list()
        d['result']['objectwise'] = list()
        
        for row in queryResult1:
            tmp = dict()
            if row['s']:
                tmp['s'] = shortenURI(row['s'])
            else:
                tmp['s'] = ''
            
            if row['p']:
                tmp['p'] = shortenURI(row['p'])
            else:
                tmp['p'] = ''
            
            if row['o']:
                tmp['o'] = shortenURI(row['o'])
            else:
                tmp['o'] = ''
            d['result']['subjectwise'].append(tmp)

        for row in queryResult2:
            tmp = dict()
            if row['s']:
                tmp['s'] = shortenURI(row['s'])
            else:
                tmp['s'] = ''
            
            if row['p']:
                tmp['p'] = shortenURI(row['p'])
            else:
                tmp['p'] = ''
            
            if row['o']:
                tmp['o'] = shortenURI(row['o'])
            else:
                tmp['o'] = ''
            d['result']['propertywise'].append(tmp)

        for row in queryResult3:
            tmp = dict()
            if row['s']:
                tmp['s'] = shortenURI(row['s'])
            else:
                tmp['s'] = ''
            
            if row['p']:
                tmp['p'] = shortenURI(row['p'])
            else:
                tmp['p'] = ''
            
            if row['o']:
                tmp['o'] = shortenURI(row['o'])
            else:
                tmp['o'] = ''
            d['result']['objectwise'].append(tmp)

        return jsonify(d)

if __name__ == '__main__':
    app.run()