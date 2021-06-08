# -*- coding: utf-8 -*-
"""
Created on Wed May 13 16:33:08 2021

@author: Kaustubh
"""
import pandas as pd
from mlxtend.frequent_patterns import apriori
from mlxtend.frequent_patterns import association_rules
from bokeh.plotting import figure
from bokeh.models import ColumnDataSource, HoverTool, DataTable, TableColumn, Div, NumberFormatter
from bokeh.layouts import column, gridplot, layout, grid


def mba_results(engine, logger):
    
    logger.info("Entering analysis")
    group_stoppage_21 = pd.read_sql_table('mba_filler_changed', con=engine)

    #grouped_stopped_states_21 = [1,2,3,4,5,6]

    #grouped_filler_times_21 = pd.DataFrame()
    #grouped_filler_times_21['Start_Time'] = grouped_filler_stoppage[(grouped_filler_stoppage['Filler'] == 0) & (grouped_filler_stoppage['Filler'].shift(-1).isin(grouped_stopped_states_21))]['t_stamp'].reset_index(drop=True)

    grouped_Rules_mapping_21 = group_stoppage_21

    grouped_Rules_mapping_21.drop('Item_No', inplace=True, axis=1)

    grouped_ix_21 =  grouped_Rules_mapping_21[(grouped_Rules_mapping_21.T!=0).any()]
    
    grouped_frequent_itemsets_21 = apriori(grouped_ix_21, min_support= 0.03, max_len = 2, use_colnames=True)
    grouped_rules_21 = association_rules(grouped_frequent_itemsets_21, metric="confidence", min_threshold=0.5)
    grouped_rules_21.head()
    grouped_rules_21['antecedents'] = grouped_rules_21['antecedents'].apply(lambda a: ','.join(list(a)))
    grouped_rules_21['consequents'] = grouped_rules_21['consequents'].apply(lambda a: ','.join(list(a)))

    grouped_rules_21.to_sql('mba_all_results',con = engine, if_exists='replace', index=False)


    grouped_rules_21_4_5 = association_rules(grouped_frequent_itemsets_21, metric="confidence", min_threshold=0.10)

    grouped_rules_21_4_5['antecedents'] = grouped_rules_21_4_5['antecedents'].apply(lambda a: ','.join(list(a)))
    grouped_rules_21_4_5['consequents'] = grouped_rules_21_4_5['consequents'].apply(lambda a: ','.join(list(a)))



    ###################################### RESPONSIBLE MACHINES #################################

    machine_list =["Filler_Safety_Stopped", "Filler_Starved", "Filler_Blocked", "Filler_Faulted", "Filler_Unallocated", "Filler_User_Stopped"]

    responsible_machine_for_filler = pd.DataFrame()

    responsible_machine_for_filler = grouped_rules_21[grouped_rules_21['consequents'].astype(str).isin(machine_list)]
    responsible_machine_for_filler = responsible_machine_for_filler.sort_values(by=['confidence'], ascending=False)

    responsible_machine_for_filler.to_sql('mba_filler_all_results',con = engine, if_exists='replace', index=False)

    ###################### BACKTRACKING ##############

    Ante_string_list = list(responsible_machine_for_filler['antecedents'][ responsible_machine_for_filler['antecedents'].str.contains("_Starved") | responsible_machine_for_filler['antecedents'].str.contains("_Blocked")])

    responsible_machine_for_filler_backtracking = grouped_rules_21[grouped_rules_21['consequents'].astype(str).isin(Ante_string_list)]
    responsible_machine_for_filler_backtracking = responsible_machine_for_filler_backtracking.sort_values(by=['confidence'], ascending=False)


    ###################### State_4 and state_5 ##############

        #grouped_rules_21_4_5["antecedents"] = grouped_rules_21_4_5['antecedents'].astype(str)
    Ante_string_list_4_5 = list(grouped_rules_21_4_5['antecedents'][ grouped_rules_21_4_5['antecedents'].str.contains("_Faulted") | grouped_rules_21_4_5['antecedents'].str.contains("_Unallocated")])
    responsible_machine_for_state_4_5 = pd.DataFrame()

    responsible_machine_for_state_4_5 = grouped_rules_21_4_5[grouped_rules_21_4_5['consequents'].astype(str).isin(Ante_string_list_4_5)]
    responsible_machine_for_state_4_5 = responsible_machine_for_state_4_5.sort_values(by=['confidence'], ascending=False)
    #responsible_machine_for_state_4_5["Conse_string"] = responsible_machine_for_state_4_5['consequents'].astype(str)

    ##################### Displaying results for State_4 and state_5 ###############
    responsible_machine_for_state_4_5.rename(columns={'confidence': 'confidence_main_responsible' ,'antecedents': 'Main_responsible', 'consequents': 'Target_machine'}, inplace=True)

    responsible_machine_for_state_4_5 = responsible_machine_for_state_4_5.drop(['antecedent support','consequent support','support','lift','leverage','conviction'],axis=1)
    responsible_machine_for_state_4_5 = responsible_machine_for_state_4_5[['Main_responsible','confidence_main_responsible','Target_machine']]

    responsible_machine_for_state_4_5.to_sql('state4_state5_results',con = engine, if_exists='replace', index=False)

    ##################### DISPLAYING FINAL RESULTS ##############
    
    mba_results = pd.DataFrame()

    mba_results_21 = pd.merge(responsible_machine_for_filler_backtracking, responsible_machine_for_filler, left_on='consequents', right_on='antecedents').reset_index()

    final_mba_results = mba_results_21.drop(['index','antecedent support_x','consequent support_x','support_x','lift_x','leverage_x', 'conviction_x','antecedent support_y','consequent support_y','support_y','lift_y', 'leverage_y','conviction_y','antecedents_y'],axis=1)

    final_mba_results = final_mba_results.sort_values(by=['confidence_y'], ascending=False)
    final_mba_results.rename(columns={'confidence_x': 'confidence_main_responsible', 'confidence_y': 'confidence_secondary_responsible' ,'antecedents_x': 'Main_responsible', 'consequents_x': 'secondary_responsible','consequents_y': 'Target_Filler'}, inplace=True)

    final_mba_results[['confidence_main_responsible','confidence_secondary_responsible']] = final_mba_results[['confidence_main_responsible','confidence_secondary_responsible']]*100
    final_mba_results = final_mba_results[['Main_responsible','confidence_main_responsible','secondary_responsible','confidence_secondary_responsible','Target_Filler']]
    final_mba_results.to_sql('mba_final_results',con = engine, if_exists='replace', index=False)

    logger.info("Analysis completed")

def mba_result_page(engine, logger):
    mba_results = pd.read_sql_table('mba_final_results',con = engine)
    state_4_5 = pd.read_sql_table('state4_state5_results',con = engine)

    columns_results = [TableColumn(field="Main_responsible", title="Primary Responsible Machine"),TableColumn(field="confidence_main_responsible", title="Confidence-Primary", formatter=NumberFormatter(format="0.000")), TableColumn(field="secondary_responsible", title="Secondary Responsible Machine"), TableColumn(field="confidence_secondary_responsible", title="Confidence-Secondary",formatter=NumberFormatter(format="0.000")), TableColumn(field="Target_Filler", title="Filler Status")] 
    data_table_res = DataTable(source=ColumnDataSource(mba_results), columns=columns_results, width=400, height=300, autosize_mode = 'fit_viewport')
        
    columns_s4_5 = [TableColumn(field="Main_responsible", title="Responsible Machine"), TableColumn(field="confidence_main_responsible", title="Confidence", formatter=NumberFormatter(format="0.000")),TableColumn(field="Target_machine", title="Target Machine")] 
    data_table_4_5 = DataTable(source=ColumnDataSource(state_4_5), columns=columns_s4_5, width=400, height=300, autosize_mode = 'fit_viewport')

    p = figure(x_axis_label = 'Confidence in %',y_axis_label = 'Responsible Machines',plot_width=600, plot_height=600,y_range=mba_results['Main_responsible'].unique())

    p.asterisk("confidence_main_responsible", "Main_responsible", source=ColumnDataSource(mba_results), size=15, color="#2ca02c",alpha=0.7)

    hover = HoverTool()
    hover.tooltips = [("Responsible Machine","@Main_responsible"),("Confidence","@confidence_main_responsible"),("Filler Status","@Target_Filler")]  ## define the content of the hover tooltip
    p.add_tools(hover)

    p_h = Div(text="""<b>Responsible Machines for Filler</b>""")
    p_col = column(p_h, p)
    data_table_res_h = Div(text="""<b>Responsible Machines for Filler</b>""")
    data_table_res_col = column(data_table_res_h, data_table_res)

    data_table_4_5_h = Div(text="""<b>Responsible Machines for States Faulted and Unallocated</b>""")
    data_table_4_5_col = column(data_table_4_5_h,data_table_4_5 )
    col = column(data_table_res_col, data_table_4_5_col)
    ly = layout([[p_col , col]])

    return ly

        





