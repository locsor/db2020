#!/usr/bin/env python
# coding: utf-8

# In[2]:


import mysql.connector
import random
import string
import pandas as pd
import PySimpleGUI as sg
import numpy as np
import mysqlx
import json
import datetime

def connection(user, password):
    conn = mysql.connector.connect(host = "localhost",
                  user = user,
                  password = password,
                  database = 'shipping',
                  auth_plugin='mysql_native_password')

    c = conn.cursor(buffered=True)
    
    connect_args = {
        'host': 'localhost',
        'port': 33060,
        'user': user,
        'password': password,
    };

    document_session = mysqlx.get_session(**connect_args)
    document_db = document_session.get_schema('documents')
    
    return c, conn, document_db, document_session

def select(table, c):
    query = "SELECT * FROM " + table
    #print("QUERY: " + query)
    c.execute(query)
    res = c.fetchall()
    
    column_names_query = "SHOW columns FROM " + table + ";"
    c.execute(column_names_query)
    column_names = c.fetchall()
    #print(column_names)

    #for x in res:
    #    print(x)
    return res, column_names
        
def insert(table, values, c):
    validity_check(values, table, "insert", c)
    values_str = "("
    for i in range(len(values)):
        if type(values[i]) == datetime.date or values[i].isdigit() == False:
            values_str += "'" + str(values[i]) + "'" + ","
        else:
            values_str += str(values[i]) + ","
    values_str = values_str[:-1]
    values_str += ")"
    query = "INSERT INTO " + table + " VALUES " + values_str + ";"
    print("QUERY: " + query)
    c.execute(query)
    
def update(table, columns, values, id_name, id_num, c):
    validity_check(values, table, "update", c)
    query = "UPDATE " + table + " SET"
    
    for i in range(len(columns)):
        if type(values[i]) == datetime.date or values[i].isdigit() == False:
            query += " " + columns[i] + "= '" + str(values[i]) + "' ,"
        else:
            query += " " + columns[i] + "=" + str(values[i]) + ","
    query = query[:-1]
    query += " WHERE " + str(id_name) + "=" + str(id_num) + ";"
    print("QUERY: " + query)
    c.execute(query)
    
def delete(table, id_name, id_num, c):
    if table == "ports" and id_num == '0':
        error = "Can not delete HQ port = " + str(id_num)
        raise Exception(error) 
    query = "DELETE FROM " + table + " WHERE " + id_name + "=" + id_num
    #print(query)
    c.execute(query)
    
def get_tables(c):
    query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA='shipping'"
    c.execute(query)
    tables = c.fetchall()
    
    flat_tables = [item for sublist in tables for item in sublist]
    table_types = ['sql' for sublist in tables for item in sublist]
    
    return flat_tables, table_types
def get_rights(c):
    query = "SHOW GRANTS FOR CURRENT_USER;"
    c.execute(query)
    res = c.fetchall()
    return res

def load_data(current_table, c):
    res, column_names = select(current_table, c)
    
    df = pd.DataFrame(res, columns=[i[0] for i in column_names])
    header_list = list(df.columns.values)
    if current_table == "ferries":
        df = df.sort_values(["ShipmentId", "StartDate"], ascending = (True, True))
    data = df.values.tolist()
    
    return data, header_list

def load_document(current_table, document_db):
    my_collection = document_db.get_collection(current_table)
    my_docs = my_collection.find().execute()
    documents = my_docs.fetch_all()

    header_list= ['Id']
    data = [[d['ShipmentId']] for d in documents]
    
    return data, documents, header_list

def error_codes(code):
    if code == 0:
        error = "Recived an empty answer"
    elif code == 1:
        error = "Wrong dates"
    elif code == 2:
        error = "Restrictions For Cargo"
    
    return error

def get_response(query, c, skip = False):
    c.execute(query)
    res = np.asarray(c.fetchall())
    if len(res) == 0 and skip == False:
        empty_response(query)
    if len(res) == 0 and skip == True:
        res = []
    return res

def empty_response(query):
    error = "Got empty response for query: " + query
    raise Exception(error) 
        
def restrictions_checker(table, value, restrictions, c):
    restrictions_int = [int(i) for i in restrictions]
    if table == "cargo":
        query = """SELECT ships.ShipId, ships.CargoRestrictions, ports.PortId, ports.CargoRestictions  
                        FROM cargo
                        JOIN shipments ON shipments.ShipmentId = cargo.ShipmentId
                        JOIN ferries ON shipments.ShipmentId = ferries.ShipmentId
                        JOIN ports ON ports.PortId = ferries.PortStart or ports.PortId = ferries.PortEnd
                        JOIN ships ON ships.ShipId = ferries.ShipId
                        WHERE shipments.ShipmentId = """ + value
        
        res = get_response(query, c, True)
        
        if len(res) > 0:
            ships_restr = res[:,0:2]
            ports_restr = res[:,2:4]

            for restr in ships_restr:
                ships_restr_int = [int(val) for val in restr[1]]
                if 1 in [restrictions_int[val]+ships_restr_int[val] for val in range(len(restrictions_int))]:
                    error = "Found an inconsistency in cargo restrictions for a ship with an id = " + str(restr[0])
                    #print(error)
                    raise Exception(error) 

            for restr in ports_restr:
                ports_restr_int = [int(val) for val in restr[1]]
                if 1 in [restrictions_int[val]+ports_restr_int[val] for val in range(len(restrictions_int))]:
                    error = "Found an inconsistency in cargo restrictions for a port with an id = " + str(restr[0])
                    #print(error)
                    raise Exception(error) 
    
    if table == "ships":
        query = """SELECT shipments.ShipmentId, cargo.CargoId, cargo.CargoRestrictions
                    FROM shipments
                    JOIN cargo ON cargo.ShipmentId = shipments.ShipmentId
                    JOIN ferries ON ferries.ShipmentId = shipments.ShipmentId
                    JOIN ships ON ships.ShipId = ferries.ShipId
                    WHERE ships.ShipId = """ + value
        
        res = get_response(query, c, True)
        
        if len(res) > 0:
            cargo_restr = res[:,1:3]

            for restr in cargo_restr:
                cargo_restr_int = [int(val) for val in restr[1]]
                if 1 in [restrictions_int[val]+cargo_restr_int[val] for val in range(len(restrictions_int))]:
                    error = "Found an inconsistency in cargo restrictions for a cargo with an id = " + str(restr[0])
                    #print(error)
                    raise Exception(error) 
    
    if table == "ports":
        query = """SELECT shipments.ShipmentId, cargo.CargoId, cargo.CargoRestrictions
                    FROM shipments
                    JOIN cargo ON cargo.ShipmentId = shipments.ShipmentId
                    JOIN ferries ON ferries.ShipmentId = shipments.ShipmentId
                    JOIN ports ON ports.PortId = ferries.PortStart or ports.PortId = ferries.PortEnd
                    WHERE ports.PortId = """ + value
        
        res = get_response(query, c, True)
        
        if len(res) > 0:
            cargo_restr = res[:,1:3]
            for restr in cargo_restr:
                cargo_restr_int = [int(val) for val in restr[1]]
                if 1 in [restrictions_int[val]+cargo_restr_int[val] for val in range(len(restrictions_int))]:
                    error = "Found an inconsistency in cargo restrictions for a cargo with an id = " + str(restr[0])
                    raise Exception(error) 
    
    if table == "ferries":
        query0 = "SELECT * FROM shipping.cargo WHERE ShipmentId = " + value[1]
        query1 = "SELECT * FROM shipping.ports WHERE PortId = " + value[5] +" or PortId = " + value[6]
        query2 = "SELECT * FROM shipping.ships WHERE ShipId = " + value[2]
        
        res0 = get_response(query0, c, True)
        res1 = get_response(query1, c, True)
        res2 = get_response(query2, c, True)
        
        if len(res0) > 0 and len(res1) > 0 and len(res2) > 0:
            cargo_restr = res0[:,[0, 3]]
            ports_restr = res1[:,[0, 3]]
            ships_restr = res2[:,[0, 4]]

            for i in range(len(cargo_restr)):
                cargo_restr_int = [int(val) for val in cargo_restr[i][1]]

                for j in range(len(ports_restr)):
                    ports_restr_int = [int(val) for val in ports_restr[j][1]]
                    if 1 in [cargo_restr_int[val]+ports_restr_int[val] for val in range(len(cargo_restr_int))]:
                        error = "Found an inconsistency in port restrictions for a port with an id = " + str(ports_restr[j][0])
                        #print(error)
                        raise Exception(error) 

                for j in range(len(ships_restr)):
                    ships_restr_int = [int(val) for val in ships_restr[j][1]]
                    if 1 in [cargo_restr_int[val]+ships_restr_int[val] for val in range(len(cargo_restr_int))]:
                        error = "Found an inconsistency in port restrictions for a port with an id = " + str(ships_restr[j][0])
                        #print(error)
                        raise Exception(error) 
        

def date_checker(table, value, dates, c):
    if table == "ferries":
        query = "SELECT ferries.ShipId, ferries.StartDate, ferries.EndDate FROM ferries WHERE ferries.ShipId = " + value
        ship_timetables = get_response(query, c, True)
        
        if len(ship_timetables) > 0:
            for time_slot in ship_timetables:

                if (max(dates[0], time_slot[1]) < min(dates[1], time_slot[2])) == True and dates[0] != time_slot[1] and dates[1] != time_slot[2]: 
                    error = "Found an inconsistency in dates for a ship with an id = " + str(time_slot[0])
                    raise Exception(error) 
    elif table == "shipments":
        query = """SELECT shipments.ShipmentId, ferries.ShipId, ferries.StartDate, ferries.EndDate FROM shipments
                    JOIN ferries ON ferries.ShipmentId = shipments.ShipmentId
                    WHERE shipments.ShipmentId = """ + value
        ship_timetables = get_response(query, c, True)
        
        if len(ship_timetables) > 0:
            for time_slot in ship_timetables:
                if (dates <= time_slot[3]) or (dates >= time_slot[2]):
                    error = "Found an inconsistency in dates for a ship with an id = " + str(time_slot[1])
                    raise Exception(error) 

def chain_deleter(table, value, c, conn):
    if table == "shipments":
        query = """SELECT cargo.CargoId, ferries.FerriesId FROM shipments 
                    JOIN cargo ON cargo.ShipmentId = shipments.ShipmentId
                    JOIN ferries ON ferries.ShipmentId = shipments.ShipmentId
                    #JOIN shipments_staff ON shipments_staff.ShipmentId = shipments.ShipmentId
                    WHERE shipments.ShipmentId = """ + value
        
        res = get_response(query, c, True)
        if len(res) == 0:
            delete(table, "ShipmentId", value, c)
        
        else:
            cargo_ids = list(set(res[:,0]))
            ferries_ids = list(set(res[:,1]))

            for i in range(len(cargo_ids)):
                delete("cargo", "CargoId", str(cargo_ids[i]), c)
            for i in range(len(ferries_ids)):
                delete("ferries", "FerriesId", str(ferries_ids[i]), c)

            delete(table, "ShipmentId", value, c)
        conn.commit()
        
    elif table == "ports":
        query0 = """SELECT ferries.FerriesId, ports.PortId from ports
                        JOIN ferries ON ferries.PortStart = ports.PortId or ferries.PortEnd = ports.PortId
                        WHERE ports.PortId = """ + value
        query1 = """SELECT StaffId FROM staff
                        WHERE staff.Station = """ + value
        query2 = """SELECT * FROM shipments
                        where DestinationPortId = """ + value
        
        res0 = get_response(query0, c, True)
        res1 = get_response(query1, c, True)
        res2 = get_response(query2, c, True)
        
        for i in range(len(res0)):
            delete("ferries", "FerriesId", str(res0[i,0]), c)
        for i in range(len(res1)):
            update("staff", ["Station"], ['0'], "StaffId", str(res1[i,0]), c)
        for i in range(len(res2)):
            delete("shipments_staff", "ShipmentId", str(res2[i,0]), c)
            delete("shipments", "ShipmentId", str(res2[i,0]), c)
            
        delete("ports", "PortId", str(value), c)
            
        conn.commit()
    
    elif table == "ships":
        query0 = """SELECT ferries.FerriesId, ships.ShipId from ships
                        JOIN ferries ON ferries.ShipId = ships.ShipId
                        WHERE ships.ShipId = """ + value
        
        res0 = get_response(query0, c, True)
        
        for i in range(len(res0)):
            delete("ferries", "FerriesId", str(res0[i,0]), c)
            
        delete("ships", "ShipId", str(value), c)
            
        conn.commit()
        
            
            
def validity_check(values, table, operation, c):
    if table in ['cargo', 'ships', 'ports']:
        if len(values[-1]) != 6:
            error = "Exception should be a string of six digits!"
            raise Exception(error) 
                
    if operation == "insert":
        if table == 'cargo':
            restrictions_checker(table, values[1], values[3], c)
        elif table == 'ferries':
            restrictions_checker(table, values, '000000', c)
            date_checker(table, values[2], values[3:5], c)
        elif table == 'shipments' and operation == "update":
            date_checker("shipments", values[0], values[3], c)
        elif table == 'shipments_staff':
            query0 = """select DestinationPortId from shipments
                            WHERE ShipmentId = """ + values[0]
            query1 = """select Station, Position from staff
                            WHERE StaffId = """ + values[1]
            res0 = get_response(query0, c, True)
            res1 = get_response(query1, c, True)
            
            if res1[0,1] != "Representative" and (res0[0,0] != res1[0,0] or (len(res0) == 0 or len(res1) == 0)):
                error = "Found an inconsistency in shipments_staff table"
                raise Exception(error) 
    elif operation == "update":
        if table == 'ports':
            restrictions_checker(table, values[0], values[3], c)
        elif table == 'ships':
            restrictions_checker(table, values[0], values[4], c)
        elif table == 'cargo':
            restrictions_checker(table, values[1], values[3], c)
        elif table == 'ferries':
            restrictions_checker(table, values, '000000', c)
            date_checker(table, values[2], values[3:5], c)
        elif table == 'shipments' and operation == "update":
            date_checker("shipments", values[0], values[3], c)
        elif table == 'shipments_staff':
            query0 = """select DestinationPortId from shipments
                            WHERE ShipmentId = """ + values[0]
            query1 = """select Station, Position from staff
                            WHERE StaffId = """ + values[1]
            res0 = get_response(query0, c, True)
            res1 = get_response(query1, c, True)
            
            if res1[0,1] != "Representative" and (res0[0,0] != res1[0,0] or (len(res0) == 0 or len(res1) == 0)):
                error = "Found an inconsistency in shipments_staff table"
                raise Exception(error) 
        

def refresh(window, current_table, document_db, current_table_type, c):
    if current_table_type == 'sql':
        data, header_list = load_data(current_table, c)
    elif current_table_type == 'documents':
        data, documents_full, header_list = load_document(current_table, document_db)

    window.FindElement('Table').Update(values=data, num_rows=min(25, len(data)))
    
def flap_display(value, c):
    query = """SELECT shipments.ShipmentId, shipments.DestinationPortId, ferries.PortStart, ferries.PortEnd, ferries.StartDate, ferries.EndDate FROM shipping.shipments
                JOIN ferries ON ferries.ShipmentId = shipments.ShipmentId
                WHERE shipments.ShipmentId = """ + value
    res = get_response(query, c, skip = True)
    today = datetime.date.today()
    #print(query)
    
    if len(res) > 1:
        last_date = max(res[:,5])
        last_date_ind = np.argmax(res[:,5])
        destination = res[0,1]
        if destination == res[last_date_ind,3]:
            code = "LightGreen"
            if today >= last_date:
                code = "green"
        elif destination != res[last_date_ind,3]:
            code = "yellow"

        for i in range(1, len(res)):
            if res[i,2] != res[i-1,3]:
                code = "red"
        return code
        
    elif len(res) == 1:
        last_date = max(res[:,5])
        destination = res[0,1]
        if res[0,3] != res[0,1]:
            code = "yellow"
        else:
            code = "green"
            if today >= last_date:
                code = "Green"
            
        return code
    else:
        code = "white"
        return code 
    
def restrictions2str(restr):
    types = ["Alchohol", "Chemical/Biological", "Fragile", "Large", "Needs refrigeration", "Perishable food"]
    dict1 = {}
    for i in range(len(restr)):
        if restr[i] == '1':
            dict1[types[i]] = "Yes"
        elif restr[i] == '2':
            dict1[types[i]] = "No"
        else:
            dict1[types[i]] = "N/A"
    return dict1

def data2json(value, c):
    color = flap_display(str(value), c)
    ship_types = {0:"Regular cargo ship", 1: "Bulker", 2: "Multipurpose ship", 3: "Reefer ship"}

    query0 = """select customers.CustomerId, customers.FirstName, customers.LastName, customers.Address from customers
                join shipments on shipments.CustomerId = customers.CustomerId
                where shipments.ShipmentId = """ + str(value)
    
    res0 = get_response(query0, c, True)

    query1 = """select cargo.CargoId, cargo.Weight, cargo.CargoRestrictions from cargo
                join shipments on shipments.ShipmentId = cargo.ShipmentId
                where shipments.ShipmentId = """ + str(value)
    res1 = get_response(query1, c, True)

    query2 = """select ferries.FerriesId, ferries.ShipId, ferries.StartDate, ferries.EndDate, ferries.PortStart, ferries.PortEnd from ferries
                join shipments on shipments.ShipmentId = ferries.ShipmentId
                where shipments.ShipmentId = """ + str(value)
    res2 = get_response(query2, c, True)

    query3 = """select ports.PortId, ports.PortName, ports.PortCountry, ports.CargoRestictions from ports
                join ferries on ferries.PortStart = ports.PortId or ferries.PortEnd = ports.PortId
                join shipments on shipments.ShipmentId = ferries.ShipmentId
                where shipments.ShipmentId = """ + str(value)
    res3 = [list(x) for x in set(tuple(x) for x in get_response(query3, c, True))]

    query4 = """select ships.ShipId, ships.ShipName, ships.ShipCountry, ships.Type, ships.CargoRestrictions from ships
                join ferries on ferries.ShipId = ships.ShipId
                join shipments on shipments.ShipmentId = ferries.ShipmentId
                where shipments.ShipmentId = """ + str(value)
    res4 = get_response(query4, c, True)

    query5 = """select shipments_staff.ShipmentId, shipments_staff.StaffId, shipments_staff.Action, staff.FirstName, staff.LastName, staff.Position, staff.Station from shipments_staff
                join shipments on shipments.ShipmentId = shipments_staff.ShipmentId
                join staff on staff.StaffId = shipments_staff.StaffId
                where shipments.ShipmentId = """ + str(value)
    res5 = get_response(query5, c, True)

    query6 = """select * from shipments
                where shipments.ShipmentId = """ + str(value)
    res6 = get_response(query6, c, True)

    query7 = """select * from ports"""
    res7 = get_response(query7, c, True)
    dict7 = {}
    for l in res7:
        dict7[l[0]] = l[1:]

    dict1 = {}
    for l in res1:
        dict1[l[0]] = {"Weight": l[1], "Cargo Restrictions":
                       restrictions2str(l[2])}

    dict3 = {}
    for l in res3:
        dict3[l[0]] = {"Port's Name": l[1], "Port's Country": l[2], "Cargo Restrictions":
                       restrictions2str(l[3])}

    dict4 = {}
    for l in res4:
        dict4[l[0]] = {"Ship's name": l[1], "Country of registration": l[2], "Type": ship_types[int(l[3])], "Cargo Restrictions":
                       restrictions2str(l[4])}

    dict5 = {}
    for l in res5:
        dict5[l[1]] = {"Name": l[3] + ' ' + l[4], "Position": l[5], "Location": dict7[str(l[6])][0] + ', ' + dict7[str(l[6])][1],
                       "Action": l[2]}

    dict2 = {}
    for l in res2:
        dict2[l[0]] = {"FerryId": l[0], "Ship": dict4[str(l[1])], "Start date": l[2].strftime("%Y-%m-%d"),
                       "End date": l[3].strftime("%Y-%m-%d"), "Port Start": dict3[str(l[4])], "Port End": dict3[str(l[5])]}
        
        
    if len(res6) == 0:
        shipment_info = ['???']
    else:
        shipment_info = res6[0][0]
    if len(res0) == 0:
        customer_info = ['???']
    else:
        customer_info = {"CustomerId": res0[0][0], "First Name": res0[0][1], "Last Name": res0[0][2], "Address": res0[0][3]}
    if len(res6) == 0:
        destination_info = ['???']
        date_of_order_info = ['???']
    else:
        destination_info =  dict7[str(res6[0][2])][0] + ", " + dict7[str(res6[0][2])][1]
        date_of_order_info = res6[0][3].strftime("%Y-%m-%d")
    if len(res5) == 0:
        staff_info = ['???']
    else:
        staff_info = dict5
    if len(res2) == 0:
        ferries_info = ['???']
    else:
        ferries_info = dict2
    if len(res1) == 0:
        cargo_info = ['???']
    else:
        cargo_info = dict1
    status_info = ['???']
    if color == "green":
        status_info = ['Completed']
    elif color == "LightGreen":
        status_info = ['In progress']
    elif color == "yellow":
        status_info = ['Incomplete route']
    elif color == "red":
        status_info = ['Gap in a route! Check ferries.']
        
        
    dict_main = {"ShipmentId": shipment_info, "Customer": customer_info,
                "Destination": destination_info, "Date of Order": date_of_order_info,
                "Staff": staff_info, "Ferries": ferries_info, "Cargo": cargo_info, "Status": status_info[0]} 
    return json.dumps(dict_main, ensure_ascii=False, indent=4)

def main():
    rights = []
    data_list = []
    no_doubt_header_list = []
    windowsDoc = []
    in_Insert = False
    in_Query = False
    in_Update = False
    in_Documents = False
    editMode = False
    connected = False
    running = True
    
    user = None
    c, conn, document_db = None, None, None
    menu_def = [['Connect', ['Establish Connection','Disconnect']], ['Help', ['How to input data','Cargo restrictions cheatsheet']]]
    sg.set_options(auto_size_buttons=True)
    headers = {}
    
    layout = [
        [sg.Menu(menu_def)],
        [sg.Combo([''], enable_events=True, key='Table_Selector')],
        [sg.Table(values=[['Awaiting connection..']],
                          headings=['ShipmentId', 'CustomerId', 'DestinationPortId', 'DateOfOrder'],
                          display_row_numbers=True,
                          auto_size_columns=False,
                          num_rows=5,
                          key='Table',
                          text_color='black',
                          bind_return_key = True)],
        [sg.Input(key='input_num', justification='left', size=(8, 1), pad=(1, 1), visible=False), sg.Button('Insert', visible=False), 
         sg.Button('SQL Query', visible=True, key='Query'), sg.T(' '*13),
         sg.Checkbox(key='checkbox', text='Change', enable_events=True)],
        [sg.Button('Update', visible=False, key='Update'), sg.Button('Delete', visible=False, key='Delete'),
         sg.Button('Summary', visible=False, key='Summary')]
    ]
    
    
    window = sg.Window('Bruh', layout, grab_anywhere=False, resizable = True)
    
    selected_rows = []
    
    while running:
        if editMode == False and in_Documents == False:
            event, values = window.read(timeout=100)
        else:
            event, values = window.read()
        
        if event is None and values is None:
            running = False
            break
        
        current_table = values['Table_Selector']
        
        if event == 'Establish Connection':
            
            layoutConnect = [
                [sg.Text('Login:', size=(8, 1), justification='left'),
                     sg.Input(key='user_input', justification='left', size=(8, 1), pad=(1, 1))],
                [sg.Text('Password:', size=(8, 1), justification='left'),
                     sg.Input(key='pass_input', justification='left', size=(8, 1), pad=(1, 1), password_char='*')],
                [sg.Button('Connect'), sg.T(' '*13)]
            ]
            
            windowConnect = sg.Window('Insert', layoutConnect, grab_anywhere=False)

            window.Disable()
            event, values = windowConnect.read()
            
            if event == 'Connect':
                user, password = values['user_input'], values['pass_input']
                
                try:
                    c, conn, document_db, document_session = connection(user, password)
                    current_table = 'shipments'
                    current_table_old = 'shipments'
                    current_table_type = 'sql'
                    #data0, header_list0 = load_data("test", c)
                    #data1, header_list1 = load_data("tasks", c)
                    
                    tables, table_types = get_tables(c)
                    
                    for i in range(len(tables)):
                        temp_data, temp_header_list = load_data(tables[i], c)
                        data_list.append(temp_data)
                        no_doubt_header_list.append(temp_header_list)

                    for collection in document_db.get_collections():
                        tables += [collection.name]
                        no_doubt_header_list += [["ShipmentId"]]

                    table_types += ['documents']
                    
                    headers = dict(zip(tables, no_doubt_header_list))
                    #headers = {'test': header_list0, 'tasks': header_list1, 'my_docs': ['Id']}

                    rights = get_rights(c)
                    if len(rights) <= 1:
                        rights_visibility = False
                    else:
                        rights_visibility = True
#                     if "UPDATE" not in rights[0][0]: 
#                         window.FindElement('input_num').update(visible=False)
#                         window.FindElement('Insert').update(visible=False)
#                         window.FindElement('checkbox').update(visible=False)
#                     else:
#                         window.FindElement('input_num').update(visible=True)
#                         window.FindElement('Insert').update(visible=True)
#                         window.FindElement('checkbox').update(visible=True)
                        
                    windowConnect.close()
                    window.Enable()
                    window.BringToFront()
                    window.FindElement('Table').Update(values=data_list[0], num_rows=min(25, len(data_list[0])))
                    window.FindElement('Table_Selector').Update(value=current_table, values=tables)
                    #window.FindElement('Table_Selector').set_size((8,1))
                    window.FindElement('Table_Selector').expand(expand_x=True,expand_y=True,expand_row=True)
                    window.FindElement('input_num').update(visible=rights_visibility)
                    window.FindElement('checkbox').update(visible=rights_visibility)
                    window.FindElement('Insert').update(visible=rights_visibility)
                    window.FindElement('Update').update(visible=rights_visibility)
                    window.FindElement('Delete').update(visible=rights_visibility)
                    window.FindElement('Summary').update(visible=rights_visibility)
                    connected = True
                except Exception as e: 
                    sg.popup_error('Error connecting to the database.')
                    windowConnect.close()
                    window.Enable()
                    window.BringToFront()
                    connected = False
            
            if event == None:
                window.Enable()
                window.BringToFront()
                event, values = 'Dud', {}
        
        if event == "How to input data":
            layoutHelp1 = [
                    [sg.Text('Format for dates: YYYY-MM-DD.', size=(45, 1), justification='left')],
                    [sg.Button('Ok'), sg.T(' '*13)]
                ]
                
            windowHelp1 = sg.Window('Clarification on cargo restrictions.', layoutHelp1, grab_anywhere=False)
            window.Disable()
            event, values = windowHelp1.read()

            if event == 'Ok':
                windowHelp1.close()
                window.Enable()
                window.BringToFront()

            if event == None:
                window.Enable()
                window.BringToFront()
                event, values = 'Dud', {}
        
        if event == "Cargo restrictions cheatsheet":
            layoutHelp2 = [
                    [sg.Text('Clarification on cargo restrictions in some tables.', size=(45, 1), justification='left')],
                    [sg.Text('Each digit represents specific type of cargo.', size=(45, 1), justification='left')],
                    [sg.Text('For ports and ships:', size=(45, 1), justification='left')],
                    [sg.Text('0 — shows incompatibility with this type of cargo,', size=(45, 1), justification='left')],
                    [sg.Text('1 — shows compatibility with this type of cargo.', size=(45, 1), justification='left')],
                    [sg.Text('For cargo:', size=(45, 1), justification='left')],
                    [sg.Text('1 — shows type of cargo,', size=(45, 1), justification='left')],
                    [sg.Text('2 — shows that this cargo does not belong to this type.', size=(45, 1), justification='left')],
                    [sg.Text('Cargo can have multiple types or dont have any.', size=(45, 1), justification='left')],
                    [sg.Text('Types of cargo (order of digits corresponds to this list):', size=(45, 1), justification='left')],
                    [sg.Text('1 — Alcohol', size=(45, 1), justification='left')],
                    [sg.Text('2 — Chemical/Biological', size=(45, 1), justification='left')],
                    [sg.Text('3 — Flammable', size=(45, 1), justification='left')],
                    [sg.Text('4 — Large', size=(45, 1), justification='left')],
                    [sg.Text('5 — Needs refrigeration', size=(45, 1), justification='left')],
                    [sg.Text('6 — Perishable goods', size=(45, 1), justification='left')],
                    [sg.Button('Ok'), sg.T(' '*13)]
                ]
                
            windowHelp2 = sg.Window('Clarification on cargo restrictions.', layoutHelp2, grab_anywhere=False)
            window.Disable()
            event, values = windowHelp2.read()

            if event == 'Ok':
                windowHelp2.close()
                window.Enable()
                window.BringToFront()

            if event == None:
                window.Enable()
                window.BringToFront()
                event, values = 'Dud', {}
        
        if connected:
            conn.commit()
            current_table_type = table_types[tables.index(current_table)]
        
            if current_table != current_table_old:
                #print(table_types[tables.index(current_table)])
                current_table_old = current_table
                if current_table_type == 'sql':
                    window.close()
                    window.close()

                    data1, header_list1 = load_data(current_table, c)
                    in_Documents = False
                    
                    rights = get_rights(c)
                    if len(rights) <= 1:
                        rights_visibility = False
                    else:
                        rights_visibility = True
                    
                    layout = [
                        [sg.Menu(menu_def)],
                        [sg.Combo(tables, default_value=current_table, enable_events=True, key='Table_Selector')],
                        [sg.Table(values=data1,
                                          headings=headers[current_table],
                                          display_row_numbers=True,
                                          auto_size_columns=False,
                                          num_rows=min(25, len(data1)),
                                          key='Table',
                                          text_color='black',
                                          bind_return_key = True)],
                        [sg.Input(key='input_num', justification='left', size=(8, 1), pad=(1, 1), visible=rights_visibility), 
                         sg.Button('Insert', visible=rights_visibility),
                         sg.Button('SQL Query', visible=True, key='Query'), sg.T(' '*13),
                         sg.Checkbox(key='checkbox', text='Change', enable_events=True, visible=rights_visibility)],
                        [sg.Button('Update', visible=rights_visibility, key='Update'),
                         sg.Button('Delete', visible=rights_visibility, key='Delete')]
                    ]
                    if current_table == "shipments":
                        layout[4].append(sg.Button('Summary', visible=False, key='Summary'))
                    window = sg.Window(current_table, layout, grab_anywhere=False, resizable = True)
                    event, values = window.read(timeout=100)
                
                elif current_table_type == 'documents':
                    window.close()
                    window.close()
                    data1, documents_full, header_list1 = load_document(current_table, document_db)
                    in_Documents = True
                    
                    rights = get_rights(c)
                    if len(rights) <= 1:
                        rights_visibility = False
                        right_click_menu=['&Right', ['&View','&Download']]
                    else:
                        rights_visibility = True
                        right_click_menu=['&Right', ['&View','&Delete','&Download']]
                        
                    
                    layout = [
                        [sg.Menu(menu_def)],
                        [sg.Combo(tables, default_value=current_table, enable_events=True, key='Table_Selector')],
                        [sg.Table(values=data1,
                                          headings=headers[current_table],
                                          display_row_numbers=True,
                                          auto_size_columns=False,
                                          num_rows=min(25, len(data1)),
                                          key='Table',
                                          right_click_menu=right_click_menu,
                                          text_color='black',
                                          bind_return_key = True)],
                        [sg.In(key='BrowseIn', visible=rights_visibility),
                         sg.FileBrowse('Browse', key='Browse', enable_events=True, 
                                       file_types=(("Json Files", "*.json"),("Text files", "*.txt")))],
                        [sg.Button('Upload', key='Upload', visible=rights_visibility)]
                    ]
                    window = sg.Window('Docs', layout, grab_anywhere=False, resizable = True)
                    event, values = window.read(timeout=100)
            
            if current_table_type == 'sql':
                data, header_list = load_data(current_table, c)
                conn.commit()
            elif current_table_type == 'documents':
                data, documents_full, header_list = load_document(current_table, document_db)
            
            window.FindElement('Table').Update(values=data, num_rows=min(25, len(data)))
            
            if current_table == "shipments":
                #print("Oh color, lovely color!")
                colors = []
                for i in range(len(data)):
                    colors.append( (i, flap_display(str(data[i][0]), c)) )
                #selected_rows.remove( (values['Table'][0], 'blue') )
                window.FindElement('Table').Update(row_colors = colors)
                


            if event == 'checkbox':
                if values['checkbox'] == True and in_Documents == False:
                    editMode = True
                    window.FindElement('Update').update(visible=True)
                    window.FindElement('Delete').update(visible=True)
                    if current_table == "shipments":
                        window.FindElement('Summary').update(visible=True)
                elif values['checkbox'] == False and in_Documents == False:
                    selected_rows = []
                    editMode = False
                    window.FindElement('Update').update(visible=False)
                    window.FindElement('Delete').update(visible=False)
                    if current_table == "shipments":
                        window.FindElement('Summary').update(visible=False)
                #selected_row = values['Table'][0]
                #print(selected_row)

            if event == 'Table' and editMode == True:
                if len(values['Table']) != 0:
                    if (values['Table'][0], 'blue') not in selected_rows:
                        selected_rows.append( (values['Table'][0], 'blue') )
                    elif (values['Table'][0], 'blue') in selected_rows:
                        selected_rows.remove( (values['Table'][0], 'blue') )
                    window.FindElement('Table').Update(row_colors = [])
                    window.FindElement('Table').Update(row_colors = selected_rows)
                #print(selected_rows)
                
            if event == 'View' and in_Documents == True:
                docs = values['Table']
                for i in range(len(docs)):
                    chosen_doc = docs[i]
                    temp_dict = dict(documents_full[chosen_doc])
                    doc_text = '\n'.join('{}: {}'.format(key, val) for key, val in temp_dict.items())

                    layoutDoc = [
                        [sg.Multiline(doc_text, size=(60, 45))]
                    ]

                    windowsDoc.append(sg.Window('Document ' + str(chosen_doc), layoutDoc, 
                                                grab_anywhere=False, resizable = True))
            
                window.Disable()
                
                while True:
                    for wind in windowsDoc:
                        event, values = wind.read(timeout=100)
                        if event == None:
                            windowsDoc.remove(wind)
                            #print(len(windowsDoc))
                    if len(windowsDoc) == 0:
                        break
                    
                window.Enable()
                event, values = window.read()
            
            if event == "Download" and in_Documents == True:
                docs = values['Table']
                for i in range(len(docs)):
                    chosen_doc = docs[i]
                    #print(documents_full[chosen_doc]['_id'])
                    with open(documents_full[chosen_doc]['_id'] + '.json', 'w', encoding='utf-8') as f:
                        json.dump(dict(documents_full[chosen_doc]), f, ensure_ascii=False, indent=4)
                
            if event == 'Delete' and in_Documents == True:
                docs = values['Table']
                print(docs)
                for i in range(len(docs)):
                    chosen_doc = docs[i]
                    #print(documents_full[chosen_doc]['_id'])
                    try:
                        db_collection = document_db.get_collection(current_table)
                        #print("_id in ('" + documents_full[chosen_doc]['_id'] + "')")
                        db_collection.remove("_id in ('" + documents_full[chosen_doc]['_id'] + "')").execute()
                        
                        data, documents_full, header_list = load_document(current_table, document_db)
                        window.FindElement('Table').Update(values=data, num_rows=min(25, len(data)))
                    
                    except Exception as e: 
                        sg.popup_error(e)
                    
                
            
            if event == 'Delete' and editMode == True:
                data_for_deletion = window.FindElement('Table').Get()
                try:
                    if current_table == "shipments":
                        for i in range(len(selected_rows)):
                            chain_deleter(current_table, str(data_for_deletion[selected_rows[i][0]][0]), c, conn)
                        refresh(window, current_table, document_db, current_table_type, c)
                    elif current_table == "ports":
                        ans = sg.popup_yes_no('Are you sure? This may delete a shipment order! Update or create a new port entity.')
                        if ans == 'Yes':
                            for i in range(len(selected_rows)):
                                chain_deleter(current_table, str(data_for_deletion[selected_rows[i][0]][0]), c, conn)
                            refresh(window, current_table, document_db, current_table_type, c)
                    else:
                        for i in range(len(selected_rows)):
                            delete(current_table, headers[current_table][0], str(data_for_deletion[selected_rows[i][0]][0]), c)
                            conn.commit()
                            selected_rows = []
                        refresh(window, current_table, document_db, current_table_type, c)
                except Exception as e: 
                    selected_rows = []
                    refresh(window, current_table, document_db, current_table_type, c)
                    sg.popup_error(e)
                

            if event == 'Update' and editMode == True:
                
                MAX_COL = len(headers[current_table])
                MAX_ROWS = len(selected_rows)
                data_for_Update = window.FindElement('Table').Get()
                
                data_ports, headers_ports = select("ports", c)
                headers_ports=[i[0] for i in headers_ports]
                data_ships, headers_ships = select("ships", c)
                headers_ships=[i[0] for i in headers_ships]
                data_cargo, headers_cargo = select("cargo", c)
                headers_cargo=[i[0] for i in headers_cargo]
                
                columm_layout = [[sg.Text(headers[current_table][j], size=(12, 1), justification=
                'left') for j in range(MAX_COL)]] + [[sg.Input(default_text=str(data_for_Update[selected_rows[i][0]][j]),size=(15, 1), pad=(
                1, 1), justification='right', key=(i, j), ) for j in range(MAX_COL)] for i in range(MAX_ROWS)]

                layoutInsert = [
                    [sg.Col(columm_layout, size=(800, 600), scrollable=True, key = 'update_matrix')],
                    [sg.Table(values=data_ports,
                          headings=headers_ports,
                          auto_size_columns=False,
                          num_rows=min(25, len(data_ports[0])),
                          key='TablePorts',
                          text_color='black',
                          bind_return_key = True),
                     sg.T(' '*13),
                     sg.Table(values=data_ships,
                          headings=headers_ships,
                          auto_size_columns=False,
                          num_rows=min(25, len(data_ships[0])),
                          key='TableShips',
                          text_color='black',
                          bind_return_key = True),
                    sg.T(' '*13),
                    sg.Table(values=data_cargo,
                          headings=headers_cargo,
                          auto_size_columns=False,
                          num_rows=min(25, len(data_cargo[0])),
                          key='TableShips',
                          text_color='black',
                          bind_return_key = True)],
                    [sg.Button('Submit'), sg.T(' '*13)]
                ]

                windowInsert = sg.Window('Insert', layoutInsert, grab_anywhere=False, resizable = True)

                window.Disable()
                event, values = windowInsert.read()
                in_Update = True

            if event == 'Insert':
                MAX_COL = len(headers[current_table])
                if window.FindElement('input_num').Get() == '':
                    MAX_ROWS = 1
                else:
                    MAX_ROWS = int(window.FindElement('input_num').Get())
                    
                data_ports, headers_ports = select("ports", c)
                headers_ports=[i[0] for i in headers_ports]
                data_ships, headers_ships = select("ships", c)
                headers_ships=[i[0] for i in headers_ships]
                data_cargo, headers_cargo = select("cargo", c)
                headers_cargo=[i[0] for i in headers_cargo]

                columm_layout = [[sg.Text(headers[current_table][j], size=(14,1), pad = (1,1), justification=
                                    'left') for j in range(MAX_COL)]] + [[sg.Input(size=(15, 1), pad=
                                    (1, 1), justification='right', key=(i, j)) for j in range(MAX_COL)] for i in range(MAX_ROWS)]

                layoutInsert = [
                    [sg.Col(columm_layout, size=(800, 200), scrollable=True, key = 'update_matrix')],
                    [sg.Table(values=data_ports,
                          headings=headers_ports,
                          auto_size_columns=False,
                          num_rows=min(25, len(data_ports[0])),
                          key='TablePorts',
                          text_color='black',
                          bind_return_key = True),
                     sg.T(' '*13),
                     sg.Table(values=data_ships,
                          headings=headers_ships,
                          auto_size_columns=False,
                          num_rows=min(25, len(data_ships[0])),
                          key='TableShips',
                          text_color='black',
                          bind_return_key = True),
                    sg.T(' '*13),
                    sg.Table(values=data_cargo,
                          headings=headers_cargo,
                          auto_size_columns=False,
                          num_rows=min(25, len(data_cargo[0])),
                          key='TableShips',
                          text_color='black',
                          bind_return_key = True)],
                    [sg.Button('Submit'), sg.T(' '*13)],
                ]

                windowInsert = sg.Window('Insert', layoutInsert, grab_anywhere=False, resizable = True)

                window.Disable()
                event, values = windowInsert.read()
                in_Insert = True
            
            if event == 'Query':
                layoutQuery = [
                    [sg.Text('Query:', size=(45, 1), justification='left')],
                    [sg.Multiline(key='query_input', size=(45, 5), pad=(1, 1))],
                    [sg.Table(values=[['Awaiting connection..']],
                              headings=[''],
                              display_row_numbers=True,
                              auto_size_columns=False,
                              num_rows=5,
                              key='TableQuery',
                              bind_return_key = True)],
                    [sg.Button('Submit')]
                    ]
                
                windowQuery = sg.Window('Query', layoutQuery, grab_anywhere=False)
                
                window.Disable()
                event, values = windowQuery.read()
                in_Query = True
            
            if event == 'Upload':
                filename = window.FindElement('BrowseIn').Get()
                try:
                    with open(filename) as json_file:
                        dataJson = json.load(json_file)
                    
                    db_collection = document_db.get_collection(current_table)
                    document_session.start_transaction()
                    stmt_add = db_collection.add()
                    stmt_add.add(dataJson)
                    result = stmt_add.execute()
                    document_session.commit()
                    print("Number of documents added: {0}".format(
                        result.get_affected_items_count()))
                    print("Document IDs: {0}".format(result.get_generated_ids()))
                    data, documents_full, header_list = load_document(current_table, document_db)
                    window.FindElement('Table').Update(values=data, num_rows=min(25, len(data)))

                except Exception as e: 
                    sg.popup_error(e)
            
#             if event == 'DeleteIn':
#                 print("iiiii")
#                 doc_name = chosen_doc = values['Table'][0]
#                 print(doc_name)
#                 try:
#                     result = my_collection.remove("_id in ('00005ec00d250000000000000001')").execute()
#                 except Exception as e: 
#                     sg.popup_error(e)
            
            if event == "Summary":
                try:
                    db_collection = document_db.get_collection("my_docs")
                    document_session.start_transaction()
                    stmt_add = db_collection.add()
                    values = []
                    for i in range(len(selected_rows)):
                        values.append(window.FindElement('Table').Get()[selected_rows[i][0]][0])
                    for value in values:
                        stmt_add.add(data2json(value,c))
                    result = stmt_add.execute()
                    document_session.commit()
                    print("Number of documents added: {0}".format(
                        result.get_affected_items_count()))
                    print("Document IDs: {0}".format(result.get_generated_ids()))
                    selected_rows = []
                    refresh(window, current_table, document_db, current_table_type, c)
                    data1, documents_full, header_list1 = load_document("my_docs", document_db)

                except Exception as e:
                    refresh(window, current_table, document_db, current_table_type, c)
                    sg.popup_error(e)
                
                
                
            if in_Insert:
                event, values = windowInsert.read()
                update_data = []
                if event == None:
                    window.Enable()
                    window.BringToFront()
                    event, values = 'Dud', {}
                    in_Insert = False
                if event == 'Submit':
                    for i in range(MAX_ROWS):
                        temp_list = []
                        for j in range(MAX_COL):
                            cell_value_temp = windowInsert[(i, j)].Get()
                            if cell_value_temp == '':
                                cell_value = None
                            else:
                                cell_value = cell_value_temp

                            temp_list.append(cell_value)
                        update_data.append(temp_list)

                    windowInsert.close()
                    window.Enable()
                    window.BringToFront()

                    in_Insert = False
                    
                try:
                    for i in range(len(update_data)):
#                             temp_str = "(" 
#                             for j in range(len(update_data[i])):
#                                 temp_str += str(update_data[i][j]) + ","
#                             temp_str = temp_str[:-1]
#                             temp_str += ")"
#                             print(temp_str)
                        for j in range(len(update_data[i])):
                            if 'Date' in headers[current_table][j]:
                                clean_date = ''.join(c for c in update_data[i][j] if c.isdigit())
                                date_big = datetime.datetime.strptime(clean_date, "%Y%m%d")
                                date = date_big.date()
                                update_data[i][j] = date
                        
                        insert(current_table, update_data[i], c)
                        conn.commit()
                    refresh(window, current_table, document_db, current_table_type, c)
                except Exception as e:
                    refresh(window, current_table, document_db, current_table_type, c)
                    sg.popup_error(e)

                if event.startswith('Escape'):
                    window.Enable()
                    windowInsert.normal()

            if in_Update:
                event, values = windowInsert.read()
                if event == None:
                    selected_rows = []
                    refresh(window, current_table, document_db, current_table_type, c)
                    window.Enable()
                    window.BringToFront()
                    event, values = 'Dud', {}
                    in_Update = False
                if event == 'Submit':
                    update_data = []

                    #update_data = np.zeros((MAX_ROWS, MAX_COL))
                    for i in range(MAX_ROWS):
                        temp_list = []
                        for j in range(MAX_COL):
                            cell_value_temp = windowInsert[(i, j)].Get()
                            if cell_value_temp == '':
                                cell_value = None
                            else:
                                cell_value = cell_value_temp

                            temp_list.append(cell_value)

                        update_data.append(temp_list)


                    windowInsert.close()
                    window.Enable()
                    window.BringToFront()

                    in_Update = False
                    
                    try:
                        for i in range(len(update_data)):
                            for j in range(len(update_data[i])):
                                if 'Date' in headers[current_table][j]:
                                    clean_date = ''.join(c for c in update_data[i][j] if c.isdigit())
                                    date_big = datetime.datetime.strptime(clean_date, "%Y%m%d")
                                    date = date_big.date()
                                    update_data[i][j] = date
                                    
                            update(current_table, header_list, update_data[i], headers[current_table][0], 
                                   str(int(data_for_Update[selected_rows[i][0]][0])), c)
                            conn.commit()
                        selected_rows = []
                        refresh(window, current_table, document_db, current_table_type, c)
                    except Exception as e:
                        selected_rows = [] 
                        refresh(window, current_table, document_db, current_table_type, c)
                        sg.popup_error(e)

                    data, header_list = load_data(current_table, c)
                    window.FindElement('Table').Update(values=data, num_rows=min(25, len(data)), row_colors = selected_rows)

                if event.startswith('Escape'):
                    window.Enable()
                    windowInsert.normal()
            
            if in_Query:
                event, values = windowQuery.read()
                if event == None:
                    window.Enable()
                    window.BringToFront()
                    event, values = 'Dud', {}
                    in_Query = False
                if event == 'Submit':
                    query = windowQuery.FindElement('query_input').Get()
                    #windowQuery.close()
                    if 'select' not in query.lower():
                        sg.popup_error('Only Select queries!')
                    else:
                        #names_unparsed = query[names_start:names_end].replace(" ", "")
                        #names_parsed = names_unparsed.split(",")

                        try:
                            c.execute(query)
                            names_parsed = [i[0] for i in c.description]
                            res_custom = c.fetchall()

                            df_custom = pd.DataFrame(res_custom, columns=[i[0] for i in names_parsed])
                            data_custom = df_custom.values.tolist()
                            windowQuery.close()
                            
                            
                            layoutQuery = [
                                    [sg.Text('Query:', size=(45, 1), justification='left')],
                                    [sg.Multiline(key='query_input', size=(45, 5), pad=(1, 1))],
                                    [sg.Table(values=data_custom,
                                              headings=names_parsed,
                                              display_row_numbers=True,
                                              auto_size_columns=False,
                                              num_rows=5,
                                              key='TableQuery',
                                              bind_return_key = True)],
                                    [sg.Button('Submit')]
                                ]

                            windowQuery = sg.Window('Query', layoutQuery, grab_anywhere=False)

                        except Exception as e: 
                            sg.popup_error(e)
                    
                if event.startswith('Escape'):
                    window.Enable()
                    windowInsert.normal()
                
                
        if event == 'Disconnect':
            ans = sg.popup_yes_no('Are you sure?')
            if ans == 'Yes':
                window.FindElement('Table').Update(values=[['Awaiting connection..']], num_rows=5)
                window.FindElement('Table_Selector').Update(value=[''], values=[''])
                if conn != None:
                    conn.commit()
                    conn.close()
                    connected = False
        
        if event.startswith('Escape'):
            running = False
            conn.commit()
            conn.close()
            document_session.close()
            window.normal()
        
    window.close()

main()




