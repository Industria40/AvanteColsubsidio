from data_ingestion import comm_sql_plc

control_2 = comm_sql_plc(control=1,
                         syn_offset=0,
                         db_data=101,
                         bytes_buffer=14,
                         offset_estado_plc=9)

control_2.sesion_baja_alta()
