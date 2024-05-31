from data_ingestion import comm_sql_plc

control_3 = comm_sql_plc(control=3,
                         syn_offset=16,
                         db_data=103,
                         bytes_buffer=10,
                         offset_estado_plc=9)

control_3.sesion_institucional_comercial()