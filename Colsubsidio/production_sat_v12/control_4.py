from data_ingestion import comm_sql_plc

control_4 = comm_sql_plc(control=4,
                         syn_offset=24,
                         db_data=104,
                         bytes_buffer=32,
                         offset_estado_plc=31)


control_4.sesion_inspection()