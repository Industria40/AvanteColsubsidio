from data_ingestion import comm_sql_plc

control_2 = comm_sql_plc(control=2,
                         syn_offset=8,
                         db_data=102,
                         bytes_buffer=14,
                         offset_estado_plc=9)

control_2.sesion_baja_alta()