from bokeh.plotting import figure, show, output_file, save
from bokeh.models import ColumnDataSource,LabelSet, Div, Span, HoverTool, DataTable, CategoricalColorMapper,BasicTickFormatter, TableColumn, Div, HTMLTemplateFormatter
from bokeh.models.widgets import Panel, Tabs
from bokeh.transform import factor_cmap 
from bokeh.layouts import gridplot, column
from bokeh.palettes import d3, Category10_10
import pandas as pd

def plot_graph_scatter(df_ss):
    #df= df.drop(df.columns[0], axis=1)
    title_str = df_ss['Machine'].iat[0] + " Stoppage Duration vs Counts"
    
    df_ss['Scale_size'] = df_ss['Percent'].rank(ascending=True)
    # for index, item in enumerate(df_ss['Scale_size']):
    df_ss['Scale_size'] = df_ss['Scale_size'] * 3.5
    
    stopped_states = ['Safety Stopped','Starved','Blocked','Faulted','Unallocated','User Stopped']

    df_ss = df_ss.loc[df_ss['Status'].isin(stopped_states)]
    #df_ss['Status'] = df_ss['Status'].apply(lambda x: 'SafetyStopped' if x==1 else 'Starved' if x==2 else 'Blocked' if x==3 else  'Faulted' if x==4 else 'Unallocated' if x==5 else 'UserStopped' if x==6 else x)
        
    #palette = d3['Category10'][10]
    factors_str = ["Safety Stopped", "Starved",  "Blocked", "Faulted", "Unallocated", "User Stopped","Off", "Setup","Running", "Runout" ]
    
    color_map = CategoricalColorMapper(factors=factors_str, palette=Category10_10)
    if(df_ss.empty):
        return None
    else: 
        source = ColumnDataSource(data=df_ss)
        
        p = figure(title = title_str, plot_width=450, plot_height=450)      
            
        # name of the x-axis 
        p.xaxis.axis_label = "Duration in Hours"       
        # name of the y-axis 
        p.yaxis.axis_label = "No. of Times Status Changed"
        # graph.scatter("Duration in Hours", "Count", source=source, legend_field="Status", fill_alpha=0.4, size=12, color=factor_cmap('Status', 'Category10_10', df_ss['Status']))  
        p.scatter("Duration_Hours", "Count", source=source, fill_alpha=0.6, size='Scale_size', color={'field': 'Status', 'transform': color_map})
        p.yaxis.formatter = BasicTickFormatter(use_scientific=False)    
        labels = LabelSet(x='Duration_Hours', y='Count', text='Status', level='glyph',y_offset=5, source=source, render_mode='canvas', text_font_size='6pt')
        # graph.add_layout(graph.legend[0], 'right')
        p.add_layout(labels)
        #add quadrant line
    
        yparallel = (df_ss.Duration_Hours.min()+df_ss.Duration_Hours.max())/2
        xparallel = (df_ss.Count.max()+df_ss.Count.min())/2
    
        xp = Span(location=xparallel, dimension='width', line_color='grey', line_dash='dashed', line_width=1)
        p.add_layout(xp)
        yp = Span(location=yparallel, dimension='height', line_color='grey', line_dash='dashed', line_width=1)
        p.add_layout(yp)
    
        #p.x_range.start = 0
        #p.y_range.start = 0
    
        p.ygrid.grid_line_alpha = 0.5
        p.xgrid.grid_line_alpha = 0.5
        #p.xgrid.visible = False
        #p.ygrid.visible = False
        hover = HoverTool()
        hover.tooltips = [("Status","@Status"),("Stoppage Change Count","@Count"),("Duration of Stoppage","@Duration_Hours{1.111} hours"),("Percentage",'@Percent %')]  ## define the content of the hover tooltip
        p.add_tools(hover)
        return p

def plot_scatter(engine, logger):
    logger.info("plotting scatterplot bokeh")
    plot_list = []
    machines_df = pd.read_sql_table('MachineStoppageChange', con=engine)
    txt = ""
    missing_machine = []
    machine_names = machines_df.Machine.unique()
    for machine in machine_names:
        dfm = machines_df.loc[machines_df.Machine == machine]
        fig = plot_graph_scatter(dfm)
        if(fig != None):
            plot_list.append(fig)
        else:
            missing_machine.append(machine)

    grid = gridplot(plot_list, ncols=3, sizing_mode = 'stretch_width')

    del plot_list, machines_df, machine, machine_names, dfm, fig

    textDiv = Div(text="""<b>Legends:&nbsp  </b>
    <span style="color:#1f77b4">Safety Stopped  </span> &nbsp |&nbsp
    <span style="color:#ff7f0e">Starved  </span> &nbsp |&nbsp
    <span style="color:#2ca02c">Blocked </span> &nbsp |&nbsp
    <span style="color:#d62728">Faulted </span> &nbsp |&nbsp
    <span style="color:#9467bd">Unallocated </span> &nbsp |&nbsp
    <span style="color:#8c564b">User Stopped</span> """)

    if len(missing_machine) != 0:
        txt = "Note: No stoppage details available for " + ", ".join(missing_machine)
    
    missingDiv = Div(text="""<b> """+txt + """</b>""")

    col = column(textDiv, missingDiv, grid, sizing_mode = 'stretch_width')
    return col

###############################################
################ for tabs #####################
###############################################

def const_d_table(ss, machine, logger):
    logger.info("in const_d_table fn")
    data = ss[ss.Machine==machine]
    data = data[data.Status != "Running"]
    max_value_index = data.index[data['duration_sec']==data['duration_sec'].max()]
    sts = data['Status'][max_value_index]
    fr = data['Count'][max_value_index]
    drs = data['duration_sec'][max_value_index]
    source = ColumnDataSource(data)
    template="""                
            <div style="color:<%= 
                (function colorfromint(){
                    if (Status=="""+"'"+sts.iloc[0]+"'"+""")
                        {return('red')}
                    }()) %>;"> 
                <%= value %>
            </div>
            """
    formatter =  HTMLTemplateFormatter(template=template)
    columns = [TableColumn(field="Status", title="Status",  formatter=formatter), TableColumn(field="Count", title="Freq",  formatter=formatter), TableColumn(field="duration_sec", title="Duration(s)",  formatter=formatter)] 
    data_table = DataTable(source=source, columns=columns, width=275, height=200)
    div = Div(text="""<b>"""+machine+""" Details</b>""")
    return (column(div, data_table))

def cons_tabs(m, mstatus,logger):
    logger.info('in cons_tabs function')
    machines= ['Depal', 'Screwcap', 'Dynac', 'Labeller', 'Packer', 'Divider', 'Erector', 'TopSealer', 'Palletiser']
    plot_list = []
    ss = m[m.Filler_Status==mstatus]
    for machine in machines:
        plot_list.append(const_d_table(ss,machine, logger))
    grid = gridplot(plot_list, ncols=3)
    div = Div(text="""<b>"""+mstatus+""" Details</b>""",style={'font-size': '200%', 'color': 'blue'})
    g = column(div, grid)
    return (Panel(child = g, title=mstatus))

def plot_tabs(engine, logger):
    logger.info('Plotting tabs')
    m_status = ['Blocked', 'Faulted', 'Safety Stopped', 'Starved', 'Unallocated', 'User Stopped']
    tabs_list = []
    dfn = pd.read_sql_table('MachineDetailsFillerStoppage', con=engine)
    m = dfn.groupby(['Filler_Status','Machine','Status']).sum().reset_index()
    m['duration_sec'] = round(m['duration_sec'],3)
    for mstatus in m_status:
        tabs_list.append(cons_tabs(m, mstatus, logger))
    tabs = Tabs(tabs=tabs_list)
    return tabs