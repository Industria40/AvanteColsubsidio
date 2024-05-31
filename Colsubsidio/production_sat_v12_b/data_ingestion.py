from sqlite3 import OperationalError
import pyodbc
import numpy as np
import snap7
from time import sleep
import csv
from datetime import datetime
import struct
import sys


class comm_sql_plc:
    def __init__(self, control, syn_offset, db_data, bytes_buffer, offset_estado_plc, db_syn=105, server='10.19.142.1,1433', database='SofiaController', username='sa_controller', password='@Avante2023', ip_plc='10.19.143.1', rack=0, slot=1, tabla_rce='opc_ruteo_control_estatico', tabla_rcd='opc_ruteo_control_dinamico', tabla_cp='opc_ruteo_control_peso', container_bytes=4, buffer_size=20):
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.ip_plc = ip_plc
        self.rack = rack
        self.slot = slot
        self.control = control
        self.db_syn = db_syn
        self.syn_offset = syn_offset
        self.container_bytes = container_bytes
        self.buffer_size = buffer_size
        self.db_data = db_data
        self.bytes_buffer = bytes_buffer
        self.tabla_rce = tabla_rce
        self.tabla_rcd = tabla_rcd
        self.tabla_cp = tabla_cp
        self.offset_estado_plc = offset_estado_plc
    
    def log(self, message):
        pass
        # with open('log.csv', 'a', newline='') as file:
        #     writer = csv.writer(file)
        #     writer.writerow([datetime.now(), f'Control {self.control}: ' + message])

    def test_connection_plc(self):
        return self.client.get_cpu_state() != 'S7CpuStatusUnknown'

    def connect_plc(self):
        self.log(message=f'Control {self.control}: Inicio de conexión con el PLC')
        self.client = snap7.client.Client()
        try:
            self.client.connect(self.ip_plc, self.rack, self.slot)
        except RuntimeError:
            self.log(message='Conexión fallida con PLC') 
        
        if self.test_connection_plc():
            self.log(message='Conexión exitosa con PLC')

    def reconnect_plc(self):
        while not self.test_connection_plc():
            self.log(message='Reconexión con PLC')
            self.connect_plc()

    def get_new_container(self):
        return self.client.db_read(self.db_syn, self.syn_offset + 4, 1) == b'\x01'

    def set_new_container(self):
        self.client.db_write(self.db_syn, self.syn_offset + 4, b'\x00')

    def get_container(self):
        return int.from_bytes(bytes(self.client.db_read(self.db_syn, self.syn_offset, self.container_bytes)), byteorder='big', signed=False)       

    def get_i_stack(self):
        i_stack_byte = self.client.db_read(self.db_syn, self.syn_offset + 5, 1)
        i_stack = int.from_bytes(i_stack_byte, byteorder='big', signed=False)

        if i_stack == self.buffer_size - 1:
            self.client.db_write(self.db_syn, self.syn_offset + 5, b'\x00')
        else:
            self.client.db_write(self.db_syn, self.syn_offset + 5, (i_stack+1).to_bytes(1, sys.byteorder))

        return i_stack

    def connect_database(self):
        self.con = pyodbc.connect('DRIVER={SQL Server};SERVER='+self.server+';DATABASE='+self.database+';UID='+self.username+';PWD='+ self.password)
        self.cursor = self.con.cursor()

    def sql_to_plc_baja_alta(self, container_in, data_offset):
        query = f'''SELECT TOP 1 control, secuencia, ruta, confirmacion, estado
            FROM {self.tabla_rce}
            WHERE serial={container_in} AND control={self.control} ORDER BY secuencia'''
           
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        container_bytes = container_in.to_bytes(4, sys.byteorder)[3::-1]
        if rows:  
            data = container_bytes + bytes(np.array(rows))[0::4] + b'\xAA' + b'\x00'*4
            self.client.db_write(self.db_data, data_offset, data)
        else: 
            data = b'\xBB'
            self.client.db_write(self.db_data, data_offset + self.offset_estado_plc, data)

    def sql_to_plc_institucional_comercial(self, container_in, data_offset):
        query = f'''SELECT TOP 1 control, secuencia, ruta, confirmacion, estado
            FROM {self.tabla_rce}
            WHERE serial={container_in} AND control={self.control}''' 
           
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        container_bytes = container_in.to_bytes(4, sys.byteorder)[3::-1]
        if rows:
            data = container_bytes + bytes(np.array(rows))[0::4] + b'\xAA'
            self.client.db_write(self.db_data, data_offset, data)
        else: 
            data = b'\xBB'
            self.client.db_write(self.db_data, data_offset + self.offset_estado_plc, data)

    def sql_to_plc_inspection(self, container_in, data_offset):

        query_1 = f'''SELECT control, ruta, ruta_inspeccion, condicion_preestablecida, condicion_proceso, confirmacion, estado
                    FROM {self.tabla_rcd}
                    WHERE serial={container_in}'''
        query_2 = f'''SELECT control, ruta, ruta_inspeccion, peso_real, peso_teorico_tara, peso_tara, peso_teorico, estado
                    FROM {self.tabla_cp}
                    WHERE serial={container_in}'''
        
        self.cursor.execute(query_1)
        data_1 = self.cursor.fetchall()

        self.cursor.execute(query_2)
        data_2 = self.cursor.fetchall()

        if data_1 and data_2:
            container_bytes = container_in.to_bytes(4, sys.byteorder)[3::-1]
            data = container_bytes + bytes(np.array(data_1))[0::4] + bytes(np.array(data_2))[0:12][0::4] + bytes(np.array(data_2))[27:11:-1] + bytes(np.array(data_2))[28::][0::4] + b'\xAA'
            self.client.db_write(self.db_data, data_offset, data)  
        else:
            data = b'\xBB'
            self.client.db_write(self.db_data, data_offset + self.offset_estado_plc, data)           

    def control_syn_baja_alta(self):
        if self.get_new_container():
            self.set_new_container()
            container_in = self.get_container()
            data_offset = self.get_i_stack()*self.bytes_buffer
            self.sql_to_plc_baja_alta(container_in=container_in, data_offset=data_offset)

    def control_syn_institucional_comercial(self):
        if self.get_new_container():
            self.set_new_container()
            container_in = self.get_container()
            data_offset = self.get_i_stack()*self.bytes_buffer
            self.sql_to_plc_institucional_comercial(container_in=container_in, data_offset=data_offset)

    def control_syn_inspection(self):
        if self.get_new_container():
            self.set_new_container()
            container_in = self.get_container()
            data_offset = self.get_i_stack()*self.bytes_buffer
            self.sql_to_plc_inspection(container_in=container_in, data_offset=data_offset)

    def sesion_baja_alta(self):
        self.connect_database()
        self.connect_plc()
        while True:
            self.reconnect_plc()
            try: self.control_syn_baja_alta()
            except RuntimeError: self.log(message='Error de conexión con el PLC')     

    def sesion_institucional_comercial(self):
        self.connect_database()
        self.connect_plc()
        while True:
            self.reconnect_plc()
            try: self.control_syn_institucional_comercial()
            except RuntimeError: self.log(message='Error de conexión con el PLC')          

    def sesion_inspection(self):
        self.connect_database()
        self.connect_plc()
        while True:
            self.reconnect_plc()
            try: self.control_syn_inspection()
            except RuntimeError: self.log(message='Error de conexión con el PLC')            

class comm_plc_sql:
    def __init__(self, server='10.19.142.1,1433', database='SofiaController', username='sa_controller', password='@Avante2023', ip_plc='10.19.143.1', rack=0, slot=1, db_control=[101, 102, 103, 104], periodo=1, tabla_rce='opc_ruteo_control_estatico', tabla_rcd='opc_ruteo_control_dinamico', tabla_cp='opc_ruteo_control_peso'):

        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.ip_plc = ip_plc
        self.rack = rack
        self.slot = slot
        self.db_control = db_control
        self.periodo = periodo
        self.tabla_rce = tabla_rce
        self.tabla_rcd = tabla_rcd
        self.tabla_cp = tabla_cp

    def log(self, message):
        pass
        # with open('log.csv', 'a', newline='') as file:
        #     writer = csv.writer(file)
        #     writer.writerow([datetime.now(), 'Update: ' + message])

    def test_connection_plc(self):
        return self.client.get_cpu_state() != 'S7CpuStatusUnknown'

    def connect_plc(self):
        self.log(message='Update: Inicio de conexión con el PLC')
        self.client = snap7.client.Client()
        try:
            self.client.connect(self.ip_plc, self.rack, self.slot)
        except RuntimeError:
            self.log(message='Update: Conexión fallida con PLC') 
        
        if self.test_connection_plc():
            self.log(message='Update: Conexión exitosa con PLC')

    def reconnect_plc(self):
        while not self.test_connection_plc():
            self.log(message='Update: Reconexión con PLC')
            self.connect_plc()

    def connect_database(self):
        self.con = pyodbc.connect('DRIVER={SQL Server};SERVER='+self.server+';DATABASE='+self.database+';UID='+self.username+';PWD='+ self.password)
        self.cursor = self.con.cursor()

    def get_states_control_12(self, db, i):
        estado_plc = self.client.db_read(db, i*14 + 9, 1)
        if estado_plc == b'\x01':
            container_in = int.from_bytes(bytes(self.client.db_read(db, i*14, 4)), byteorder='big', signed=False)
            secuencia = int.from_bytes(bytes(self.client.db_read(db, i*14 + 5, 1)), byteorder='big', signed=False)
            confirmacion = int.from_bytes(bytes(self.client.db_read(db, i*14 + 7, 1)), byteorder='big', signed=False)
            estado = int.from_bytes(bytes(self.client.db_read(db, i*14 + 8, 1)), byteorder='big', signed=False)
            tara = int.from_bytes(bytes(self.client.db_read(db, i*14 + 10, 4)), byteorder='big', signed=False)
            return container_in, secuencia, confirmacion, estado, tara, tara > 0
        else: pass

    def get_states_control_3(self, db, i):
        estado_plc = self.client.db_read(db, i*10 + 9, 1)
        if estado_plc == b'\x01':
            container_in = int.from_bytes(bytes(self.client.db_read(db, i*10, 4)), byteorder='big', signed=False)
            confirmacion = int.from_bytes(bytes(self.client.db_read(db, i*10 + 7, 1)), byteorder='big', signed=False)
            estado = int.from_bytes(bytes(self.client.db_read(db, i*10 + 8, 1)), byteorder='big', signed=False)
            return container_in, confirmacion, estado
        else: pass

    def get_states_control_4(self, db, i):
        estado_plc = self.client.db_read(db, i*32 + 31, 1)
        if estado_plc == b'\x01':
            container_in = int.from_bytes(bytes(self.client.db_read(db, i*32, 4)), byteorder='big', signed=False)
            rcd_confirmacion = int.from_bytes(bytes(self.client.db_read(db, i*32 + 9, 1)), byteorder='big', signed=False)
            rcd_estado = int.from_bytes(bytes(self.client.db_read(db, i*32 + 10, 1)), byteorder='big', signed=False)
            cp_peso_real = int.from_bytes(bytes(self.client.db_read(db, i*32 + 26, 4)), byteorder='big', signed=False)
            cp_estado = int.from_bytes(bytes(self.client.db_read(db, i*32 + 30, 1)), byteorder='big', signed=False)
            return container_in, rcd_confirmacion, rcd_estado, cp_peso_real, cp_estado
        else: pass


    def set_updated(self, db, offset):
        self.client.db_write(db, offset, b'\xff')

    def plc_to_sql(self, query):
            self.cursor.execute(query)
            self.cursor.commit()     

    def scann(self):
        for control, db in enumerate(self.db_control, start=1):
            if control == 1 or control == 2:
                for i in range(20):
                    data = self.get_states_control_12(db, i)
                    if data:
                        self.plc_to_sql(query=f'UPDATE {self.tabla_rce} SET confirmacion = {data[2]}, estado = {data[3]} WHERE serial = {data[0]} AND control = {control} AND secuencia = {data[1]}')
                        if data[5]:
                            self.plc_to_sql(query=f'UPDATE {self.tabla_cp} SET peso_tara = {data[4]}, estado = 2 WHERE serial = {data[0]}')
                            self.plc_to_sql(query=f'UPDATE {self.tabla_cp} SET peso_teorico_tara = peso_teorico + peso_tara WHERE serial = {data[0]}')
                        self.set_updated(db, i*14 + 9)
            elif control == 3:
                for i in range(20):
                    data = self.get_states_control_3(db, i)
                    if data:
                        self.plc_to_sql(query=f'UPDATE {self.tabla_rce} SET confirmacion = {data[1]}, estado = {data[2]} WHERE serial = {data[0]} AND control = {control} AND secuencia = 1') #No es necesario secuencia=1
                        self.set_updated(db, i*10 + 9)
            elif control == 4:
                for i in range(20):
                    data = self.get_states_control_4(db, i)
                    if data:
                        self.plc_to_sql(query=f'UPDATE {self.tabla_rcd} SET confirmacion = {data[1]}, estado = {data[2]} WHERE serial = {data[0]} AND control = {control}') #No es necesario control = 4
                        self.plc_to_sql(query=f'UPDATE {self.tabla_cp} SET confirmacion = {data[1]}, peso_real = {data[3]}, estado = {data[4]} WHERE serial = {data[0]} AND control = {control}') #No es necesario control = 4
                        self.set_updated(db, i*32 + 31)
              

    def sesion(self):
        self.connect_plc()
        self.connect_database()
        while True:
            self.reconnect_plc()
            try: self.scann()
            except RuntimeError: self.log(message='Error de conexión con el PLC')