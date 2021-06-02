###############################################
#          Import some packages               #
###############################################
from flask import Flask, render_template, request, redirect, url_for
from flask_bootstrap import Bootstrap
from flask_nav import Nav
from flask_nav.elements import *
from dominate.tags import img
import pandas as pd
import psycopg2
import sqlalchemy
import pymysql


###bokeh
from bokeh.embed import components, server_document
from bokeh.resources import CDN

from bokeh.io import show
from bokeh.models import ColumnDataSource, DataTable, TableColumn

##logging
import logging

##created fn
import fndef
import visualfn
import mba


###############################################
#      Define navbar with logo                #
###############################################
logo = img(src='./static/img/logo.jpg', height="50", width="80", style="margin-top:-15px")
#here we define our menu items
topbar = Navbar(View(logo, 'home'),
                #View('EDA', 'get_eda'),
                View('Visualisation', 'get_visualisation'),
                View('Dashboard', 'get_dashboard'),
                View('Analysis Results', 'get_analysis'),
                View('Documentation', 'get_documentation')
                )

# registers the "top" menubar
nav = Nav()
nav.register_element('top', topbar)

# database connection
engine = sqlalchemy.create_engine('postgresql+psycopg2://admin:admin@localhost:5432/capstone')
#engine = sqlalchemy.create_engine('sqlite:///capstone.db')
#engine = sqlalchemy.create_engine('mysql+pymysql://root:Kukku123MYSQL@localhost/vinpac')
################Logger##########
logger = logging.getLogger('werkzeug') # grabs underlying WSGI logger
handler = logging.FileHandler('vinpac.log') # creates handler for the log file
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s - %(funcName)s"))
logger.addHandler(handler) # adds handler to the werkzeug WSGI logger
###############################################
#          Define flask app                   #
###############################################
app = Flask(__name__)
Bootstrap(app)

###############################################
#          Render Home page                   #
###############################################
@app.route('/', methods=['GET'])
def home(msg=""):
    lastUpdatedMsg = fndef.GetLastUpdatedMsg(engine, logger)
    return(render_template('upload.html', updateMsg=msg, lastUpdatedMsg=lastUpdatedMsg))

###############################################
#          Render upload page                   #
###############################################
@app.route('/upload', methods=['GET','POST'])
def upload_data():
    lastUpdatedMsg = fndef.GetLastUpdatedMsg(engine, logger)
    if request.method == 'POST':
        # get the uploaded file
        uploaded_file = request.files['inputFile']

        if uploaded_file.filename != '':
            parseCSV(uploaded_file)
            preProcess()
            fndef.changedstatus(engine,logger)
            fndef.machineDetailsFillerStop(engine, logger)
            #fndef.mbaAllZeros(engine, logger)
            fndef.mbaFillerChange(engine, logger)
            mba.mba_results(engine, logger)
            fndef.lastupdated(engine, logger)
            return (render_template('upload.html', updateMsg="File Updated Successfully!! You can now view the results in other tabs.", lastUpdatedMsg=lastUpdatedMsg))
        else:
            return(render_template('upload.html', updateMsg="File Error! Upload again!", lastUpdatedMsg=lastUpdatedMsg))
    else:
        return (render_template('upload.html', updateMsg="", lastUpdatedMsg=lastUpdatedMsg))


def parseCSV(filePath):
    csvData = pd.read_csv(filePath)
    logger.info("Reading the file")
    # update to db
    # Write data into the table in PostgreSQL database
    csvData.to_sql('vinpac1',con = engine, if_exists='replace', index=False)
    logger.info("DB updated - file")

def preProcess():
    #loop = asyncio.get_event_loop()
    logger.info("Entering preprocessing")
    table_df = pd.read_sql_table('vinpac1', con=engine)
    table_df.fillna(10, inplace=True)
    #table_df = table_df.astype(dtype= {"Depal":"int64", "Filler":"int64","Screwcap":"int64", "Dynac":"int64","Labeller":"int64","Packer":"int64","Erector":"int64","TopSealer":"int64","Palletiser":"int64"})
    machines = table_df.columns
    machines = machines.drop('t_stamp')
    for machine in machines:    
        table_df[machine] = table_df[machine].astype("int64")
    
    table_df['t_stamp'] = pd.to_datetime(table_df['t_stamp'])
    #one record for one timestamp
    table_df = table_df.groupby('t_stamp').tail(1)
    s_df = table_df.shift(1) == table_df
    #idx = s_df.loc[(s_df.Depal==True) & (s_df.Filler==True) & (s_df.Screwcap==True) &( s_df.Dynac==True) & (s_df.Labeller==True) & (s_df.Packer==True) & (s_df.Divider==True) & (s_df.Erector==True) & (s_df.TopSealer==True) & (s_df.Palletiser==True)].index
    s_df = s_df.drop('t_stamp', axis=1)
    s_df =  s_df.all(axis='columns')
    idx = s_df[s_df == True].index  
    del s_df
    table_df.drop(index=idx, axis=0, inplace=True)
    del idx
    table_df.to_sql('vinpacCleaned',con = engine, if_exists='replace', index=False)
    #loop.close()
    logger.info("Preprocessed data is now updated")

###############################################
#          Render EDA page              #
###############################################
@app.route('/eda', methods=["GET"])
def get_eda():
    logger.info("Displaying EDA")
    table_df = pd.read_sql_table('vinpac1', con=engine)
    ndf = table_df.isnull().sum().reset_index()
    ndf.columns=['Columns', 'MissingRecords']
    #ndf.transpose()
    eda_table = datatable1(ndf)
    script1, div1 = components(eda_table)
    cdn_js = CDN.js_files
    cdn_css = CDN.css_files
    return(render_template('eda.html', script1=script1, div1=div1, cdn_css=cdn_css, cdn_js=cdn_js ))
    
def datatable1(data):
    logger.info("Preparing the bokeh table")
    source = ColumnDataSource(data)
    columns = [TableColumn(field="Columns", title="Columns"), TableColumn(field="MissingRecords", title="Missing Records")] 
    data_table = DataTable(source=source, columns=columns, width=250, height=250)
    #show(data_table)
    return data_table
    

###############################################
#          Render Viz1 page              #
###############################################
@app.route('/visualisation', methods=["GET"])
def get_visualisation():
    logger.info("Visualisation 1")
    scatter_obj = visualfn.plot_scatter(engine, logger)
    script1, div1 = components(scatter_obj)
    return(render_template('visualisation.html', script1=script1, div1=div1))


###############################################
#          Render Viz2 page             #
###############################################
@app.route('/dashboard', methods=["GET"])
def get_dashboard():
    logger.info("Visualisation 2")
    script = server_document(url='http://127.0.0.1:5006/dash_final')
    return(render_template('dashboard.html', script=script, template="Flask"))

###############################################
#          Render readme page                #
###############################################

""" 
def bkapp(doc):
    df = sea_surface_temperature.copy()
    source = ColumnDataSource(data=df)

    plot = figure(x_axis_type='datetime', y_range=(0, 25), y_axis_label='Temperature (Celsius)',
                  title="Sea Surface Temperature at 43.18, -70.43")
    plot.line('time', 'temperature', source=source)

    def callback(attr, old, new):
        if new == 0:
            data = df
        else:
            data = df.rolling(f"{new}D").mean()
        source.data = ColumnDataSource.from_df(data)

    slider = Slider(start=0, end=30, value=0, step=1, title="Smoothing by N Days")
    slider.on_change('value', callback)

    doc.add_root(column(slider, plot))

    doc.theme = Theme(filename="theme.yaml")
# can't use shortcuts here, since we are passing to low level BokehTornado
bkapp = Application(FunctionHandler(bkapp))

# This is so that if this app is run using something like "gunicorn -w 4" then
# each process will listen on its own port
sockets, port = bind_sockets("127.0.0.1", 0) """

@app.route('/mbaresults', methods=["GET"])
def get_analysis():
    #script = server_document(url='http://127.0.0.1:5006/dash_final')
    mba_obj = mba.mba_result_page(engine, logger)
    script1, div1 = components(mba_obj)
    return(render_template('mbaresults.html', script1=script1, div1=div1))

@app.route('/documentation', methods=["GET"])
def get_documentation():
    return(render_template('documentation.html')) 
    
""" def bk_worker():
    asyncio.set_event_loop(asyncio.new_event_loop())

    bokeh_tornado = BokehTornado({'/bkapp': bkapp}, extra_websocket_origins=["127.0.0.1:5001/"])
    bokeh_http = HTTPServer(bokeh_tornado)
    bokeh_http.add_sockets(sockets)

    server = BaseServer(IOLoop.current(), bokeh_tornado, bokeh_http)
    server.start()
    server.io_loop.start()



t = Thread(target=bk_worker)
t.daemon = True
t.start()
 """
###############################################
#             Init our app                    #
###############################################
nav.init_app(app)


###############################################
#                Run app                      #
###############################################
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)